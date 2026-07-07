/*
 * File:        vdrapkin_timer_manager_server.c
 * Module:      Timer Manager Console Server
 * Author:      Vitaly Drapkin
 * Copyright:   Copyright (c) 2026 Vitaly Drapkin
 * Purpose:     Process client timer commands and deliver expiry indications.
 *
 * Design Overview:
 *   - Accepts multiple clients through overlapped named-pipe instances.
 *   - Uses one WaitForMultipleObjects loop for pipe, timer, and shutdown events.
 *   - Dispatches commands to the timer core and returns ACK or expiry messages.
 *
 * Concurrency:
 *   Server request processing runs on one thread. Named-pipe operations are
 *   overlapped, and the console control handler only signals the shutdown event.
 *
 * Assumptions:
 *   - Each connected client has at most one outstanding asynchronous read.
 *   - Client writes may block while the server sends an ACK or indication.
 *
 * Revision History:
 *   - 2026-07-01: Grouped mutable server state behind one accessor.
 *   - 2026-06-30: Enforced the 61-client admission limit.
 *   - Initial implementation.
 */
#include <windows.h>
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "vdr_timer_core.h"

/*
 * Console Timer Manager server.
 *
 * This follows the agreed sample architecture:
 *
 *   - One listener pipe instance is always armed when capacity allows.
 *   - Each connected client pipe has one outstanding overlapped ReadFile.
 *   - The waitable timer handle is part of the same WaitForMultipleObjects set.
 *   - Ctrl+C signals a shutdown event that also participates in the wait set.
 */

typedef enum vdrPipeRole_tag {
    /* Listener instances are waiting in ConnectNamedPipe. */
    vdrPIPE_ROLE_LISTENER = 1,

    /* Client instances are connected and normally waiting in ReadFile. */
    vdrPIPE_ROLE_CLIENT
} vdrPipeRole_type;

typedef struct PipeInstance_tag {
    /* Server-side named-pipe handle for this listener/client instance. */
    HANDLE pipe;

    /* One overlapped operation is outstanding per pipe instance. */
    OVERLAPPED overlapped;

    /* Describes the operation associated with overlapped.hEvent. */
    vdrPipeOperation_type operation;

    /* Distinguishes listener pipe from connected client pipe. */
    vdrPipeRole_type role;

    /* Client ownership token used for diagnostics; timer core uses pipe too. */
    TimerId_type client_id;

    /* Buffer for the next client-to-server command. */
    vdrClientToServerMessage_type inbound_message;

    /* Reusable buffer for server-to-client ACK/event messages. */
    vdrServerToClientMessage_type outbound_message;

    /* Number of bytes completed by the last overlapped operation. */
    DWORD bytes_transferred;
} PipeInstance_type;

typedef struct vdrServerState_tag {
    /* Compact array whose events occupy the first server wait-set slots. */
    PipeInstance_type* pipes[TIMER_SERVER_MAX_CLIENTS + 1];

    /* Number of live listener/client entries currently stored in pipes. */
    DWORD pipe_count;

    /* Logical timer lists and running timer-ID counter owned by the server. */
    vdrTimerCore_type timer_core;

    /* Windows timer representing the next logical timer expiry. */
    HANDLE waitable_timer;

    /* Event signaled by the console control handler to request shutdown. */
    HANDLE shutdown_event;
} vdrServerState_type;

/* The server has one process-lifetime state object. Direct access is confined
 * to vdrServer_GetState() so every other function crosses one visible state
 * access boundary. Static storage supplies the required zero initialization. */
static vdrServerState_type g_server_state;

/*
 * Return the process-lifetime server state object.
 *
 * Callers obtain this pointer once near their local declarations and then use
 * it for all server-state access performed by that function.
 */
static vdrServerState_type* vdrServer_GetState(void)
{
    return &g_server_state;
} /* end of vdrServer_GetState() */

/*
 * Print a server trace line and flush immediately.
 *
 * Each trace is prefixed with its source file and call-site line number.
 * The server is normally observed live in a console while a client is running
 * in another console. fflush keeps the trace chronological and visible without
 * waiting for the C runtime buffer to fill.
 */
#define PRINTOUT_TRACE(...) do { \
    printf("[%s:%d] ", __FILE__, __LINE__); \
    printf(__VA_ARGS__); \
    fflush(stdout); \
} while (0)

/*
 * Convert pipe role to readable text for trace prints.
 */
static const char* vdrServer_PipeRoleName(vdrPipeRole_type role)
{
    switch (role) {
    case vdrPIPE_ROLE_LISTENER:
        return "LISTENER";
    case vdrPIPE_ROLE_CLIENT:
        return "CLIENT";
    default:
        return "UNKNOWN";
    }
} /* end of vdrServer_PipeRoleName() */

/*
 * Convert pipe operation to readable text for trace prints.
 */
static const char* vdrServer_PipeOperationName(vdrPipeOperation_type operation)
{
    switch (operation) {
    case vdrPIPE_OPERATION_ACCEPT:
        return "ACCEPT";
    case vdrPIPE_OPERATION_READ:
        return "READ";
    case vdrPIPE_OPERATION_WRITE:
        return "WRITE";
    default:
        return "UNKNOWN";
    }
} /* end of vdrServer_PipeOperationName() */

/*
 * Convert protocol message type to readable text for trace prints.
 */
static const char* vdrServer_MessageTypeName(uint16_t message_type)
{
    switch ((vdrMessageType_type)message_type) {
    case vdrMESSAGE_TYPE_CREATE_TIMER_REQ:
        return "CREATE_TIMER_REQ";
    case vdrMESSAGE_TYPE_START_TIMER_REQ:
        return "START_TIMER_REQ";
    case vdrMESSAGE_TYPE_STOP_TIMER_REQ:
        return "STOP_TIMER_REQ";
    case vdrMESSAGE_TYPE_DELETE_TIMER_REQ:
        return "DELETE_TIMER_REQ";
    case vdrMESSAGE_TYPE_ACK_RSP:
        return "ACK_RSP";
    case vdrMESSAGE_TYPE_TIMER_EXPIRED_EVT:
        return "TIMER_EXPIRED_EVT";
    default:
        return "UNKNOWN";
    }
} /* end of vdrServer_MessageTypeName() */

/*
 * Convert ACK status to readable text for server trace prints.
 */
static const char* vdrServer_AckStatusName(vdrAckStatus_type status)
{
    switch (status) {
    case vdrACK_STATUS_OK:
        return "OK";
    case vdrACK_STATUS_INVALID_TIMER_ID:
        return "INVALID_TIMER_ID";
    case vdrACK_STATUS_INVALID_DURATION:
        return "INVALID_DURATION";
    case vdrACK_STATUS_TIMER_NOT_OWNED:
        return "TIMER_NOT_OWNED";
    case vdrACK_STATUS_SERVER_BUSY:
        return "SERVER_BUSY";
    case vdrACK_STATUS_PROTOCOL_ERROR:
        return "PROTOCOL_ERROR";
    case vdrACK_STATUS_INTERNAL_ERROR:
        return "INTERNAL_ERROR";
    case vdrACK_STATUS_SERVER_NOT_AVAILABLE:
        return "SERVER_NOT_AVAILABLE";
    case vdrACK_STATUS_NO_EXPIRED_TIMERS:
        return "NO_EXPIRED_TIMERS";
    default:
        return "UNKNOWN";
    }
} /* end of vdrServer_AckStatusName() */

/*
 * Return monotonic time in milliseconds.
 *
 * QueryPerformanceCounter is used rather than wall-clock time because timer
 * ordering should not be affected by system clock adjustments.
 */
static uint64_t vdrGetTimeMs(void)
{
    static LARGE_INTEGER frequency = { 0 };
    LARGE_INTEGER counter;

    if (frequency.QuadPart == 0) {
        /* Cache the process-wide performance-counter frequency. Once this
         * static state is set, all future timer calculations use the same
         * conversion factor and skip this initialization branch. */
        QueryPerformanceFrequency(&frequency);
        PRINTOUT_TRACE("[server][time] QueryPerformanceFrequency=%lld\n", frequency.QuadPart);
    }

    QueryPerformanceCounter(&counter);
    return (uint64_t)((counter.QuadPart * 1000LL) / frequency.QuadPart);
} /* end of vdrGetTimeMs() */

/*
 * Fill a protocol header for a server-to-client message.
 */
static void vdrFillHeader(
    vdrMessageHeader_type* header,
    vdrMessageType_type message_type,
    uint32_t request_id,
    uint32_t message_size)
{
    assert(header != NULL);

    if (header == NULL) {
        return;
    }

    header->magic = TIMER_PIPE_MAGIC;
    header->version = TIMER_PROTOCOL_VERSION;
    header->message_type = (uint16_t)message_type;
    header->request_id = request_id;
    header->payload_size = vdrPayloadSize(message_size);
} /* end of vdrFillHeader() */

/*
 * Validate a client command before dispatching it.
 *
 * This is the server's protocol boundary. No command payload is interpreted
 * until magic, version, message type, total byte count, and payload size are
 * proven consistent.
 */
static BOOL vdrValidateClientMessage(
    const vdrClientToServerMessage_type* message,
    DWORD bytes_read)
{
    uint32_t expected_size;

    PRINTOUT_TRACE("[server][validate] begin bytes_read=%lu\n", (unsigned long)bytes_read);

    assert(message != NULL);

    if (message == NULL) {
        PRINTOUT_TRACE("[server][validate] failed: message pointer is NULL\n");
        return FALSE;
    }

    if (bytes_read < sizeof(vdrMessageHeader_type)) {
        PRINTOUT_TRACE("[server][validate] failed: bytes_read=%lu is smaller than header=%zu\n",
            (unsigned long)bytes_read,
            sizeof(vdrMessageHeader_type));
        return FALSE;
    }

    if ((message->header.magic != TIMER_PIPE_MAGIC) ||
        (message->header.version != TIMER_PROTOCOL_VERSION)) {
        PRINTOUT_TRACE("[server][validate] failed: magic=0x%08X version=%u\n",
            message->header.magic,
            (unsigned)message->header.version);
        return FALSE;
    }

    /* Determine the exact fixed structure size expected for this message type. */
    switch ((vdrMessageType_type)message->header.message_type) {
    case vdrMESSAGE_TYPE_CREATE_TIMER_REQ:
        expected_size = sizeof(vdrCreateTimerReq_type);
        break;
    case vdrMESSAGE_TYPE_START_TIMER_REQ:
        expected_size = sizeof(vdrStartTimerReq_type);
        break;
    case vdrMESSAGE_TYPE_STOP_TIMER_REQ:
        expected_size = sizeof(vdrStopTimerReq_type);
        break;
    case vdrMESSAGE_TYPE_DELETE_TIMER_REQ:
        expected_size = sizeof(vdrDeleteTimerReq_type);
        break;
    default:
        PRINTOUT_TRACE("[server][validate] failed: unsupported message_type=%u\n",
            (unsigned)message->header.message_type);
        return FALSE;
    }

    if (bytes_read != expected_size) {
        PRINTOUT_TRACE("[server][validate] failed: type=%s bytes_read=%lu expected=%lu\n",
            vdrServer_MessageTypeName(message->header.message_type),
            (unsigned long)bytes_read,
            (unsigned long)expected_size);
        return FALSE;
    }

    if (message->header.payload_size != vdrPayloadSize(expected_size)) {
        PRINTOUT_TRACE("[server][validate] failed: type=%s payload=%u expected_payload=%u\n",
            vdrServer_MessageTypeName(message->header.message_type),
            message->header.payload_size,
            vdrPayloadSize(expected_size));
        return FALSE;
    }

    PRINTOUT_TRACE("[server][validate] pass: request=%u type=%s payload=%u\n",
        message->header.request_id,
        vdrServer_MessageTypeName(message->header.message_type),
        message->header.payload_size);

    return TRUE;
} /* end of vdrValidateClientMessage() */

/*
 * Allocate and initialize one named-pipe instance.
 *
 * The same structure is used for listener and connected client instances. A
 * manual-reset event is stored in OVERLAPPED so WaitForMultipleObjects can wait
 * for completion of ConnectNamedPipe or ReadFile.
 */
static PipeInstance_type* vdrCreatePipeInstance(vdrPipeRole_type role)
{
    PipeInstance_type* instance;

    assert((role == vdrPIPE_ROLE_LISTENER) || (role == vdrPIPE_ROLE_CLIENT));

    PRINTOUT_TRACE("[server][pipe-create] begin role=%s\n", vdrServer_PipeRoleName(role));

    instance = (PipeInstance_type*)malloc(sizeof(PipeInstance_type));

    if (instance == NULL) {
        PRINTOUT_TRACE("[server][pipe-create] failed: malloc returned NULL\n");
        return NULL;
    }

    /* Clear the whole instance before assigning handle fields so failure paths
     * can safely close only the resources that were actually created. */
    ZeroMemory(instance, sizeof(PipeInstance_type));

    /* Create a message-mode overlapped pipe endpoint. Listener instances use
     * this handle for ConnectNamedPipe; connected instances reuse it for reads
     * and writes after the listener accepts one client. */
    instance->pipe = CreateNamedPipeW(
        PIPE_NAME,
        PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED,
        PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE | PIPE_WAIT,
        PIPE_UNLIMITED_INSTANCES,
        sizeof(vdrServerToClientMessage_type),
        sizeof(vdrClientToServerMessage_type),
        0,
        NULL);

    if (instance->pipe == INVALID_HANDLE_VALUE) {
        PRINTOUT_TRACE("[server][pipe-create] failed: CreateNamedPipeW error=%lu\n",
            (unsigned long)GetLastError());
        free(instance);
        return NULL;
    }

    /* The event is reused each time this pipe is armed for a new overlapped
     * operation. The arm functions reset it before starting the operation. */
    instance->overlapped.hEvent = CreateEventW(NULL, TRUE, FALSE, NULL);

    if (instance->overlapped.hEvent == NULL) {
        PRINTOUT_TRACE("[server][pipe-create] failed: CreateEventW error=%lu\n",
            (unsigned long)GetLastError());
        CloseHandle(instance->pipe);
        free(instance);
        return NULL;
    }

    instance->operation = vdrPIPE_OPERATION_ACCEPT;
    instance->role = role;
    instance->client_id = (TimerId_type)(uintptr_t)instance->pipe;

    PRINTOUT_TRACE("[server][pipe-create] success pipe=%p event=%p role=%s client_id=%llu\n",
        instance->pipe,
        instance->overlapped.hEvent,
        vdrServer_PipeRoleName(instance->role),
        (unsigned long long)instance->client_id);

    return instance;
} /* end of vdrCreatePipeInstance() */

/*
 * Add a pipe instance to the compact global wait array.
 */
static BOOL vdrAddPipe(PipeInstance_type* instance)
{
    vdrServerState_type* server_state = vdrServer_GetState();

    assert(instance != NULL);

    if (instance == NULL) {
        PRINTOUT_TRACE("[server][pipe-array] add failed: instance is NULL\n");
        return FALSE;
    }

    if (server_state->pipe_count >= (TIMER_SERVER_MAX_CLIENTS + 1)) {
        PRINTOUT_TRACE("[server][pipe-array] add failed: wait set is full count=%lu\n",
            (unsigned long)server_state->pipe_count);
        return FALSE;
    }

    /* Add the pipe to the global wait set. The next main-loop iteration will
     * include this pipe's overlapped event in WaitForMultipleObjects. */
    server_state->pipes[server_state->pipe_count] = instance;

    /* Increasing the global pipe count expands the active wait set and changes
     * the calculated indexes for timer and shutdown handles in the next loop. */
    server_state->pipe_count++;

    PRINTOUT_TRACE("[server][pipe-array] added pipe=%p role=%s new_count=%lu\n",
        instance->pipe,
        vdrServer_PipeRoleName(instance->role),
        (unsigned long)server_state->pipe_count);
    return TRUE;
} /* end of vdrAddPipe() */

/*
 * Arm a listener pipe for an asynchronous client connection.
 *
 * ERROR_IO_PENDING means the connection will complete later and signal the
 * event. ERROR_PIPE_CONNECTED means a client connected between CreateNamedPipe
 * and ConnectNamedPipe; in that case the event is signaled manually so the main
 * loop handles it like a normal completion.
 */
static BOOL vdrArmListener(PipeInstance_type* instance)
{
    BOOL ok;
    DWORD error;
    HANDLE event_handle;

    assert(instance != NULL);

    if (instance == NULL) {
        PRINTOUT_TRACE("[server][listener] arm failed: instance is NULL\n");
        return FALSE;
    }

    PRINTOUT_TRACE("[server][listener] arm begin pipe=%p event=%p\n",
        instance->pipe,
        instance->overlapped.hEvent);

    event_handle = instance->overlapped.hEvent;

    /* Preserve the event handle while clearing old OVERLAPPED bookkeeping from
     * the previous operation on this pipe instance. */
    ZeroMemory(&instance->overlapped, sizeof(instance->overlapped));
    instance->overlapped.hEvent = event_handle;

    /* Resetting the event makes the next WaitForMultipleObjects wake belong to
     * this new connect operation, not a prior completion. */
    ResetEvent(event_handle);

    /* Mark this pipe as a listener before it enters the global wait set so the
     * main loop dispatches completion through the accept path. */
    instance->operation = vdrPIPE_OPERATION_ACCEPT;
    instance->role = vdrPIPE_ROLE_LISTENER;

    /* Start the asynchronous accept. Completion is reported through the event
     * stored in instance->overlapped. */
    ok = ConnectNamedPipe(instance->pipe, &instance->overlapped);
    error = GetLastError();

    if (ok) {
        PRINTOUT_TRACE("[server][listener] ConnectNamedPipe completed immediately pipe=%p\n",
            instance->pipe);
        return TRUE;
    }

    if (error == ERROR_IO_PENDING) {
        PRINTOUT_TRACE("[server][listener] ConnectNamedPipe pending pipe=%p\n",
            instance->pipe);
        return TRUE;
    }

    if (error == ERROR_PIPE_CONNECTED) {
        /* The client connected between CreateNamedPipe and ConnectNamedPipe.
         * Signal this listener's event manually so the main server loop wakes
         * through WaitForMultipleObjects and runs the normal accept path. */
        SetEvent(event_handle);
        PRINTOUT_TRACE("[server][listener] client already connected pipe=%p; event signaled\n",
            instance->pipe);
        return TRUE;
    }

    PRINTOUT_TRACE("[server][listener] arm failed pipe=%p error=%lu\n",
        instance->pipe,
        (unsigned long)error);
    return FALSE;
} /* end of vdrArmListener() */

/*
 * Arm a connected client pipe for one asynchronous command read.
 *
 * Version 1 keeps exactly one pending read per client. After each command is
 * processed and ACKed, the main loop rearms another read.
 */
static BOOL vdrArmRead(PipeInstance_type* instance)
{
    BOOL ok;
    DWORD error;
    HANDLE event_handle;

    assert(instance != NULL);

    if (instance == NULL) {
        PRINTOUT_TRACE("[server][read] arm failed: instance is NULL\n");
        return FALSE;
    }

    PRINTOUT_TRACE("[server][read] arm begin pipe=%p client_id=%llu\n",
        instance->pipe,
        (unsigned long long)instance->client_id);

    event_handle = instance->overlapped.hEvent;

    /* Preserve the event handle and clear stale OVERLAPPED/read-buffer state
     * before posting the next command read for this client. */
    ZeroMemory(&instance->overlapped, sizeof(instance->overlapped));
    ZeroMemory(&instance->inbound_message, sizeof(instance->inbound_message));
    instance->overlapped.hEvent = event_handle;

    /* Resetting the event makes the next wait wake only when this read has
     * actually completed or has been manually signaled for immediate success. */
    ResetEvent(event_handle);

    /* Mark the pipe as a connected client read so the main loop processes the
     * completion as an inbound command. */
    instance->operation = vdrPIPE_OPERATION_READ;
    instance->role = vdrPIPE_ROLE_CLIENT;

    /* Post one overlapped read. The server keeps exactly one read outstanding
     * per client pipe. */
    ok = ReadFile(
        instance->pipe,
        &instance->inbound_message,
        sizeof(instance->inbound_message),
        NULL,
        &instance->overlapped);

    if (ok) {
        /* The command bytes were available immediately. Signal this pipe event
         * manually so the main loop processes the completed read exactly like
         * an asynchronous OVERLAPPED completion. */
        SetEvent(event_handle);
        PRINTOUT_TRACE("[server][read] completed immediately pipe=%p\n", instance->pipe);
        return TRUE;
    }

    error = GetLastError();

    PRINTOUT_TRACE("[server][read] ReadFile returned error=%lu pipe=%p%s\n",
        (unsigned long)error,
        instance->pipe,
        (error == ERROR_IO_PENDING) ? " (pending)" : "");
    return (error == ERROR_IO_PENDING);
} /* end of vdrArmRead() */

/*
 * Send one server-to-client message and wait for write completion.
 *
 * This uses overlapped WriteFile for consistency with the pipe handle mode, but
 * waits immediately. That is intentionally simple and acceptable for this RTOS
 * simulation sample.
 */
static BOOL vdrSendMessage(PipeInstance_type* instance, const vdrServerToClientMessage_type* message, DWORD message_size)
{
    OVERLAPPED overlapped;
    BOOL ok;
    DWORD error;
    DWORD transferred;

    assert(instance != NULL);
    assert(message != NULL);
    assert(message_size >= sizeof(vdrMessageHeader_type));

    if ((instance == NULL) || (message == NULL) || (message_size < sizeof(vdrMessageHeader_type))) {
        PRINTOUT_TRACE("[server][send] failed validation instance=%p message=%p size=%lu\n",
            instance,
            message,
            (unsigned long)message_size);
        return FALSE;
    }

    PRINTOUT_TRACE("[server][send] begin pipe=%p type=%s request=%u bytes=%lu\n",
        instance->pipe,
        vdrServer_MessageTypeName(message->header.message_type),
        message->header.request_id,
        (unsigned long)message_size);

    /* Use a local OVERLAPPED structure for this one send operation. The pipe
     * instance's read OVERLAPPED remains reserved for inbound commands. */
    ZeroMemory(&overlapped, sizeof(overlapped));

    /* This event is waited by GetOverlappedResult below when the write is still
     * pending, keeping the send path synchronous for the sample. */
    overlapped.hEvent = CreateEventW(NULL, TRUE, FALSE, NULL);

    if (overlapped.hEvent == NULL) {
        PRINTOUT_TRACE("[server][send] failed: CreateEventW error=%lu\n",
            (unsigned long)GetLastError());
        return FALSE;
    }

    /* Write the complete fixed-size protocol message to the client. */
    ok = WriteFile(instance->pipe, message, message_size, NULL, &overlapped);

    if (ok == FALSE) {
        error = GetLastError();

        if (error != ERROR_IO_PENDING) {
            PRINTOUT_TRACE("[server][send] WriteFile failed pipe=%p error=%lu\n",
                instance->pipe,
                (unsigned long)error);
            CloseHandle(overlapped.hEvent);
            return FALSE;
        }

        PRINTOUT_TRACE("[server][send] WriteFile pending pipe=%p\n", instance->pipe);
    }

    /* Wait for write completion when needed and collect the transferred byte
     * count before the stack OVERLAPPED structure goes out of scope. */
    ok = GetOverlappedResult(instance->pipe, &overlapped, &transferred, TRUE);

    if (ok) {
        PRINTOUT_TRACE("[server][send] completed pipe=%p transferred=%lu\n",
            instance->pipe,
            (unsigned long)transferred);
    } else {
        PRINTOUT_TRACE("[server][send] completion failed pipe=%p error=%lu\n",
            instance->pipe,
            (unsigned long)GetLastError());
    }

    CloseHandle(overlapped.hEvent);

    return ok;
} /* end of vdrSendMessage() */

/*
 * Build and send the generic ACK response used by every command.
 */
static BOOL vdrSendAck(PipeInstance_type* instance, uint32_t request_id, vdrAckStatus_type status, TimerId_type timer_id)
{
    vdrServerToClientMessage_type response;
    BOOL sent;

    assert(instance != NULL);

    if (instance == NULL) {
        return FALSE;
    }

    /* Construct the wire ACK in the server-to-client union. The generic ACK is
     * used for positive and negative command completion. */
    ZeroMemory(&response, sizeof(response));

    vdrFillHeader(&response.ack_rsp.header, vdrMESSAGE_TYPE_ACK_RSP, request_id, sizeof(vdrAckRsp_type));
    response.ack_rsp.ack_status = (uint32_t)status;
    response.ack_rsp.reserved = 0;
    response.ack_rsp.timer_id = timer_id;

    /* Sending the ACK releases the client API call waiting for this request ID. */
    sent = vdrSendMessage(instance, &response, sizeof(vdrAckRsp_type));

    if (sent == TRUE) {
        PRINTOUT_TRACE("[server][ack] sent ACK request=%u status=%s(%u) timer=%llu\n",
            request_id,
            vdrServer_AckStatusName(status),
            (unsigned)status,
            (unsigned long long)timer_id);
    }

    return sent;
} /* end of vdrSendAck() */

/*
 * Build and send an asynchronous timer-expiry event.
 */
static BOOL vdrSendExpired(PipeInstance_type* instance, TimerId_type timer_id)
{
    vdrServerToClientMessage_type event_message;
    BOOL sent;

    assert(instance != NULL);
    assert(timer_id != TIMER_ID_INVALID);

    if ((instance == NULL) || (timer_id == TIMER_ID_INVALID)) {
        return FALSE;
    }

    /* Construct an asynchronous event message. Request ID zero identifies it as
     * an indication rather than a command response. */
    ZeroMemory(&event_message, sizeof(event_message));

    vdrFillHeader(
        &event_message.timer_expired_evt.header,
        vdrMESSAGE_TYPE_TIMER_EXPIRED_EVT,
        0,
        sizeof(vdrTimerExpiredEvt_type));
    event_message.timer_expired_evt.timer_id = timer_id;

    /* Sending the expiry indication wakes the client receiver thread, which
     * queues the timer ID and signals the application notification event. */
    sent = vdrSendMessage(instance, &event_message, sizeof(vdrTimerExpiredEvt_type));

    if (sent == TRUE) {
        PRINTOUT_TRACE("[server][expiry-send] sent timer expiry indication timer=%llu\n",
            (unsigned long long)timer_id);
    }

    return sent;
} /* end of vdrSendExpired() */

/*
 * Locate the pipe instance for an owner handle stored in a timer entry.
 */
static PipeInstance_type* vdrFindPipeByHandle(HANDLE pipe)
{
    vdrServerState_type* server_state = vdrServer_GetState();
    DWORD index;

    if ((pipe == NULL) || (pipe == INVALID_HANDLE_VALUE)) {
        return NULL;
    }

    for (index = 0; index < server_state->pipe_count; ++index) {
        if ((server_state->pipes[index] != NULL) &&
            (server_state->pipes[index]->pipe == pipe)) {
            PRINTOUT_TRACE("[server][pipe-find] found pipe=%p index=%lu role=%s\n",
                pipe,
                (unsigned long)index,
                vdrServer_PipeRoleName(server_state->pipes[index]->role));
            return server_state->pipes[index];
        }
    }

    return NULL;
} /* end of vdrFindPipeByHandle() */

/*
 * Close all OS handles owned by one pipe instance and free the structure.
 */
static void vdrDestroyPipe(PipeInstance_type* instance)
{
    if (instance == NULL) {
        return;
    }

    PRINTOUT_TRACE("[server][pipe-destroy] begin pipe=%p role=%s client_id=%llu\n",
        instance->pipe,
        vdrServer_PipeRoleName(instance->role),
        (unsigned long long)instance->client_id);

    if (instance->pipe != INVALID_HANDLE_VALUE) {
        /* Cancel outstanding pipe I/O before closing the handle so the wait set
         * cannot later observe completion on a destroyed instance. */
        CancelIoEx(instance->pipe, NULL);

        /* Disconnect the named pipe to notify the client API that this server
         * endpoint is no longer available. */
        DisconnectNamedPipe(instance->pipe);

        /* Closing the pipe releases the operating-system handle owned by this
         * instance. */
        CloseHandle(instance->pipe);
    }

    if (instance->overlapped.hEvent != NULL) {
        /* The pipe event is part of the server wait set while the instance is
         * alive, so it must be closed with the instance. */
        CloseHandle(instance->overlapped.hEvent);
    }

    /* The pipe instance memory is no longer referenced after it has been
     * removed from the server state's pipe array. */
    free(instance);
    PRINTOUT_TRACE("[server][pipe-destroy] complete\n");
} /* end of vdrDestroyPipe() */

/*
 * Remove a pipe from the compact wait array and return the new pipe count.
 *
 * Connected clients own timers, so removing a client also deletes all active
 * and inactive timers associated with that pipe. The last pipe entry is moved
 * into the removed slot to keep the array dense for WaitForMultipleObjects.
 */
static DWORD vdrRemovePipeAt(DWORD index)
{
    vdrServerState_type* server_state = vdrServer_GetState();
    PipeInstance_type* instance;

    assert(index < server_state->pipe_count);

    if (index >= server_state->pipe_count) {
        return server_state->pipe_count;
    }

    instance = server_state->pipes[index];

    if ((instance != NULL) && (instance->role == vdrPIPE_ROLE_CLIENT)) {
        PRINTOUT_TRACE("[server][pipe-remove] removing client pipe=%p index=%lu; deleting owned timers\n",
            instance->pipe,
            (unsigned long)index);

        /* Removing a client changes global timer-core ownership state. Any
         * active timers for this pipe are deleted, so future timer waits and
         * expiry indications must no longer include that client. */
        vdrTimerCore_RemoveTimersForPipe(&server_state->timer_core, instance->pipe);
    } else if (instance != NULL) {
        PRINTOUT_TRACE("[server][pipe-remove] removing listener pipe=%p index=%lu\n",
            instance->pipe,
            (unsigned long)index);
    }

    /* After timer ownership has been cleaned up, destroy the pipe instance and
     * remove its wait event from future server waits. */
    vdrDestroyPipe(instance);

    /* Keep the global pipe array dense for WaitForMultipleObjects by moving the
     * last live pipe into the removed slot. This changes which pipe corresponds
     * to this wait index on the next loop iteration. */
    server_state->pipes[index] = server_state->pipes[server_state->pipe_count - 1];

    /* Clear the old last slot so shutdown/cleanup cannot revisit a stale pipe
     * pointer after the array has been compacted. */
    server_state->pipes[server_state->pipe_count - 1] = NULL;

    /* Shrinking the global count removes one pipe event from the next wait set
     * and also shifts the timer/shutdown indexes down by one. */
    server_state->pipe_count--;

    PRINTOUT_TRACE("[server][pipe-remove] complete new_count=%lu\n",
        (unsigned long)server_state->pipe_count);
    return server_state->pipe_count;
} /* end of vdrRemovePipeAt() */

/*
 * Arm, rearm, or cancel the single Windows waitable timer.
 *
 * The timer core reports the duration for the active-list head. If no active
 * timers exist, the waitable timer is canceled so it no longer signals.
 */
static void vdrRearmWaitableTimer(void)
{
    vdrServerState_type* server_state = vdrServer_GetState();
    uint64_t duration_ms;
    const uint64_t now_ms = vdrGetTimeMs();

    assert(server_state->waitable_timer != NULL);

    if (server_state->waitable_timer == NULL) {
        PRINTOUT_TRACE("[server][timer-arm] skipped: waitable timer handle is NULL\n");
        return;
    }

    if (vdrTimerCore_GetNextWaitDuration(
        &server_state->timer_core,
        now_ms,
        &duration_ms) == FALSE) {
        /* No active timers remain in global timer-core state. Canceling the
         * waitable timer prevents the main loop from waking for timer work that
         * no longer exists. */
        CancelWaitableTimer(server_state->waitable_timer);
        PRINTOUT_TRACE("[server][timer-arm] no active timers; waitable timer canceled\n");
        return;
    }

    if (duration_ms == 0) {
        duration_ms = 1;
    }

    {
        LARGE_INTEGER due_time;
        /* Windows relative waitable-timer due times are negative 100-ns units.
         * The API contract uses milliseconds, so convert here at the OS edge. */
        due_time.QuadPart = -((LONGLONG)duration_ms * 10000LL);

        /* Arm the global waitable timer for the current active-list head. This
         * changes the next main-loop wakeup from pipe-only to timer-or-pipe. */
        SetWaitableTimer(
            server_state->waitable_timer,
            &due_time,
            0,
            NULL,
            NULL,
            FALSE);
        PRINTOUT_TRACE("[server][timer-arm] waitable timer armed duration=%llu ms now=%llu\n",
            (unsigned long long)duration_ms,
            (unsigned long long)now_ms);
    }
} /* end of vdrRearmWaitableTimer() */

/*
 * Dispatch one validated client command to the timer core and send its ACK.
 */
static void vdrHandleCommand(PipeInstance_type* instance)
{
    vdrServerState_type* server_state = vdrServer_GetState();
    vdrClientToServerMessage_type* message;
    vdrAckStatus_type status;
    TimerId_type ack_timer_id;
    uint32_t request_id;

    assert(instance != NULL);

    if (instance == NULL) {
        return;
    }

    message = &instance->inbound_message;
    request_id = message->header.request_id;
    status = vdrACK_STATUS_PROTOCOL_ERROR;
    ack_timer_id = TIMER_ID_INVALID;

    PRINTOUT_TRACE("[server][command] begin pipe=%p bytes=%lu raw_type=%s request=%u\n",
        instance->pipe,
        (unsigned long)instance->bytes_transferred,
        vdrServer_MessageTypeName(message->header.message_type),
        request_id);

    if (vdrValidateClientMessage(message, instance->bytes_transferred) == FALSE) {
        /* Protocol failures still receive a negative ACK when possible so the
         * client API can unblock and report the error. */
        (void)vdrSendAck(instance, request_id, vdrACK_STATUS_PROTOCOL_ERROR, TIMER_ID_INVALID);
        PRINTOUT_TRACE("[server][command] rejected malformed request=%u\n", request_id);
        return;
    }

    /* Each command updates ack_timer_id according to the ACK contract: use the
     * requested timer ID when present, or the newly allocated ID for create. */
    switch ((vdrMessageType_type)message->header.message_type) {
    case vdrMESSAGE_TYPE_CREATE_TIMER_REQ:
        PRINTOUT_TRACE("[server][command] dispatch CreateTimer request=%u\n", request_id);

        /* CreateTimer mutates the global timer core by adding an inactive timer
         * owned by this pipe. It does not arm the waitable timer until a later
         * StartTimer moves the timer active. */
        status = vdrTimerCore_CreateTimer(
            &server_state->timer_core,
            instance->pipe,
            &ack_timer_id);

        PRINTOUT_TRACE("[server][command] core CreateTimer done status=%s(%u) timer=%llu\n",
            vdrServer_AckStatusName(status),
            (unsigned)status,
            (unsigned long long)ack_timer_id);
        break;

    case vdrMESSAGE_TYPE_START_TIMER_REQ:
        ack_timer_id = message->start_timer_req.timer_id;

        PRINTOUT_TRACE("[server][command] dispatch StartTimer request=%u timer=%llu duration=%llu\n",
            request_id,
            (unsigned long long)ack_timer_id,
            (unsigned long long)message->start_timer_req.duration_ms);

        status = vdrTimerCore_StartTimer(
            &server_state->timer_core,
            instance->pipe,
            message->start_timer_req.timer_id,
            message->start_timer_req.duration_ms,
            vdrGetTimeMs());

        /* StartTimer may change the global active-list head. After the ACK, the
         * server rearms the waitable timer so the main loop wakes at the new
         * scheduled expiry. */
        PRINTOUT_TRACE("[server][command] core StartTimer done timer=%llu duration=%llu status=%s(%u)\n",
            (unsigned long long)ack_timer_id,
            (unsigned long long)message->start_timer_req.duration_ms,
            vdrServer_AckStatusName(status),
            (unsigned)status);
        break;

    case vdrMESSAGE_TYPE_STOP_TIMER_REQ:
        ack_timer_id = message->stop_timer_req.timer_id;

        PRINTOUT_TRACE("[server][command] dispatch StopTimer request=%u timer=%llu\n",
            request_id,
            (unsigned long long)ack_timer_id);

        /* StopTimer may move an active timer back to inactive global state. The
         * waitable timer is rearmed below because the active-list head may have
         * disappeared or shifted to a later timer. */
        status = vdrTimerCore_StopTimer(
            &server_state->timer_core,
            instance->pipe,
            message->stop_timer_req.timer_id,
            vdrGetTimeMs());

        PRINTOUT_TRACE("[server][command] core StopTimer done timer=%llu status=%s(%u)\n",
            (unsigned long long)ack_timer_id,
            vdrServer_AckStatusName(status),
            (unsigned)status);
        break;

    case vdrMESSAGE_TYPE_DELETE_TIMER_REQ:
        ack_timer_id = message->delete_timer_req.timer_id;

        PRINTOUT_TRACE("[server][command] dispatch DeleteTimer request=%u timer=%llu\n",
            request_id,
            (unsigned long long)ack_timer_id);

        /* DeleteTimer removes the timer from global core state entirely. If it
         * was active, the server waitable timer may need a new head duration. */
        status = vdrTimerCore_DeleteTimer(
            &server_state->timer_core,
            instance->pipe,
            message->delete_timer_req.timer_id,
            vdrGetTimeMs());

        PRINTOUT_TRACE("[server][command] core DeleteTimer done timer=%llu status=%s(%u)\n",
            (unsigned long long)ack_timer_id,
            vdrServer_AckStatusName(status),
            (unsigned)status);
        break;

    default:
        status = vdrACK_STATUS_PROTOCOL_ERROR;
        ack_timer_id = TIMER_ID_INVALID;
        break;
    }

    if (vdrSendAck(instance, request_id, status, ack_timer_id) == FALSE) {
        PRINTOUT_TRACE("[server][command] ACK send failed; client disconnected during send\n");
    }

    /* Any command may have changed the active list head, so the waitable timer
     * is rearmed after command processing. */
    PRINTOUT_TRACE("[server][command] rearming waitable timer after request=%u\n", request_id);
    vdrRearmWaitableTimer();
    PRINTOUT_TRACE("[server][command] complete request=%u status=%s(%u) timer=%llu\n",
        request_id,
        vdrServer_AckStatusName(status),
        (unsigned)status,
        (unsigned long long)ack_timer_id);
} /* end of vdrHandleCommand() */

/*
 * Send one expiry indication requested by the timer core.
 *
 * The timer core calls this after moving the expired timer back to inactive.
 * The callback stays in the server layer because only the server understands
 * pipe instances and protocol messages.
 */
static void vdrExpiredTimerCallback(TimerId_type timer_id, HANDLE server_pipe)
{
    PipeInstance_type* owner;

    assert(timer_id != TIMER_ID_INVALID);
    assert(server_pipe != NULL);
    assert(server_pipe != INVALID_HANDLE_VALUE);

    if ((timer_id == TIMER_ID_INVALID) ||
        (server_pipe == NULL) ||
        (server_pipe == INVALID_HANDLE_VALUE)) {
        PRINTOUT_TRACE("[server][timer-expiry] invalid callback data timer=%llu pipe=%p\n",
            (unsigned long long)timer_id,
            server_pipe);
        return;
    }

    owner = vdrFindPipeByHandle(server_pipe);

    if (owner != NULL) {
        PRINTOUT_TRACE("[server][timer-expiry] sending indication timer=%llu pipe=%p\n",
            (unsigned long long)timer_id,
            owner->pipe);

        /* The callback sends the asynchronous expiry event requested by the
         * timer core. If the send fails, the server logs the disconnect symptom;
         * the surrounding server loop will clean up broken pipe state. */
        if (vdrSendExpired(owner, timer_id) == FALSE) {
            PRINTOUT_TRACE("[server][timer-expiry] send failed; client disconnected during indication\n");
        } else {
            PRINTOUT_TRACE("[server][timer-expiry] indication complete timer=%llu\n",
                (unsigned long long)timer_id);
        }
    } else {
        PRINTOUT_TRACE("[server][timer-expiry] owner pipe missing for timer=%llu\n",
            (unsigned long long)timer_id);
    }
} /* end of vdrExpiredTimerCallback() */

/*
 * Process all timers due at the current time and notify their owners.
 */
static void vdrProcessTimerExpiry(void)
{
    vdrServerState_type* server_state = vdrServer_GetState();

    PRINTOUT_TRACE("[server][timer-expiry] begin\n");

    /* Processing expiry mutates the global timer core: all due active timers
     * move back to inactive state. The callback sends each client indication
     * after the corresponding timer is inactive again. */
    vdrTimerCore_ProcessExpired(
        &server_state->timer_core,
        vdrGetTimeMs(),
        vdrExpiredTimerCallback);

    PRINTOUT_TRACE("[server][timer-expiry] rearming waitable timer after expiry processing\n");
    vdrRearmWaitableTimer();
    PRINTOUT_TRACE("[server][timer-expiry] complete\n");
} /* end of vdrProcessTimerExpiry() */

/*
 * Create a replacement listener if there is room in the wait set.
 */
static BOOL vdrCreateAndArmListener(void)
{
    vdrServerState_type* server_state = vdrServer_GetState();
    PipeInstance_type* listener;

    /* Leave room for the waitable timer and shutdown event. If the pipe wait
     * set is already full, existing clients continue to run but no additional
     * listener is created. */
    if (server_state->pipe_count >= (TIMER_SERVER_MAX_CLIENTS + 1)) {
        PRINTOUT_TRACE("[server][listener-create] skipped: wait set full count=%lu\n",
            (unsigned long)server_state->pipe_count);
        return TRUE;
    }

    PRINTOUT_TRACE("[server][listener-create] begin current_count=%lu\n",
        (unsigned long)server_state->pipe_count);

    /* Allocate a new pipe instance that will wait for the next client
     * connection. */
    listener = vdrCreatePipeInstance(vdrPIPE_ROLE_LISTENER);

    if (listener == NULL) {
        PRINTOUT_TRACE("[server][listener-create] failed: create pipe instance\n");
        return FALSE;
    }

    /* Post the overlapped ConnectNamedPipe operation before adding the listener
     * event to the main wait set. */
    if (vdrArmListener(listener) == FALSE) {
        PRINTOUT_TRACE("[server][listener-create] failed: arm listener pipe=%p\n", listener->pipe);

        /* The listener was allocated but never entered the wait set, so destroy
         * it locally before reporting failure. */
        vdrDestroyPipe(listener);
        return FALSE;
    }

    /* Add the armed listener event to the compact global pipe array so the main
     * loop can wake when a client connects. */
    if (vdrAddPipe(listener) == FALSE) {
        PRINTOUT_TRACE("[server][listener-create] failed: add listener pipe=%p\n", listener->pipe);

        /* The listener is armed but not owned by the server state's pipe array,
         * so this function must close it before returning. */
        vdrDestroyPipe(listener);
        return FALSE;
    }

    PRINTOUT_TRACE("[server][listener-create] complete pipe=%p\n", listener->pipe);
    return TRUE;
} /* end of vdrCreateAndArmListener() */

/*
 * Check whether the global pipe array already contains an armed listener.
 */
static BOOL vdrHasListenerPipe(void)
{
    vdrServerState_type* server_state = vdrServer_GetState();
    DWORD pipe_index;

    for (pipe_index = 0; pipe_index < server_state->pipe_count; ++pipe_index) {
        if ((server_state->pipes[pipe_index] != NULL) &&
            (server_state->pipes[pipe_index]->role == vdrPIPE_ROLE_LISTENER)) {
            return TRUE;
        }
    }

    return FALSE;
} /* end of vdrHasListenerPipe() */

/*
 * Restore the accept path after capacity or disconnect activity.
 *
 * At maximum client capacity the server may temporarily have no listener,
 * because every pipe slot is occupied by connected clients. Once a client is
 * removed, this helper recreates the listener so new clients can connect again.
 */
static void vdrEnsureListenerAvailable(void)
{
    if (vdrHasListenerPipe() == FALSE) {
        PRINTOUT_TRACE("[server][listener-create] no listener is armed; attempting to restore accept path\n");

        if (vdrCreateAndArmListener() == FALSE) {
            PRINTOUT_TRACE("[server][listener-create] failed to restore accept path\n");
        }
    }
} /* end of vdrEnsureListenerAvailable() */

/*
 * Console control handler for Ctrl+C and console close.
 *
 * The handler does minimal work: it only signals the shutdown event. Cleanup is
 * performed by the main thread after WaitForMultipleObjects wakes up.
 */
static BOOL WINAPI vdrConsoleCtrlHandler(DWORD control_type)
{
    vdrServerState_type* server_state = vdrServer_GetState();

    if ((control_type == CTRL_C_EVENT) ||
        (control_type == CTRL_CLOSE_EVENT) ||
        (control_type == CTRL_BREAK_EVENT)) {
        if (server_state->shutdown_event != NULL) {
            PRINTOUT_TRACE("[server][shutdown] console control received type=%lu; signaling shutdown event\n",
                (unsigned long)control_type);

            /* Signaling the global shutdown event redirects main-loop control
             * flow out of pipe/timer processing and into cleanup. */
            SetEvent(server_state->shutdown_event);
        }

        return TRUE;
    }

    return FALSE;
} /* end of vdrConsoleCtrlHandler() */

/*
 * Release all server resources in shutdown order.
 */
static void vdrCleanupServer(void)
{
    vdrServerState_type* server_state = vdrServer_GetState();

    PRINTOUT_TRACE("[server][cleanup] begin pipe_count=%lu\n",
        (unsigned long)server_state->pipe_count);

    while (server_state->pipe_count > 0) {
        server_state->pipe_count = vdrRemovePipeAt(server_state->pipe_count - 1);
    }

    PRINTOUT_TRACE("[server][cleanup] deinitializing timer core\n");

    /* Reset the global timer core after all pipes are gone. This guarantees no
     * later cleanup path can produce expiry notifications for deleted clients. */
    vdrTimerCore_Deinit(&server_state->timer_core);

    if (server_state->waitable_timer != NULL) {
        PRINTOUT_TRACE("[server][cleanup] closing waitable timer\n");
        CancelWaitableTimer(server_state->waitable_timer);
        CloseHandle(server_state->waitable_timer);

        /* NULL marks the timer wait path unavailable. Any later rearm attempt
         * will skip instead of waiting on a closed handle. */
        server_state->waitable_timer = NULL;
    }

    if (server_state->shutdown_event != NULL) {
        PRINTOUT_TRACE("[server][cleanup] closing shutdown event\n");
        CloseHandle(server_state->shutdown_event);

        /* NULL prevents the console-control path from signaling a stale handle
         * after the main loop has already left shutdown processing. */
        server_state->shutdown_event = NULL;
    }

    PRINTOUT_TRACE("[server][cleanup] complete\n");
} /* end of vdrCleanupServer() */

/*
 * Server entry point.
 *
 * The main loop builds a dense wait-handle array each iteration:
 *
 *   [0 .. pipe_count-1]  pipe operation events
 *   [pipe_count]         waitable timer
 *   [pipe_count + 1]     shutdown event
 */
int main(void)
{
    vdrServerState_type* server_state = vdrServer_GetState();
    BOOL running;

    PRINTOUT_TRACE("[server][main] initializing timer core\n");

    /* Initialize the global timer lists before any pipe can dispatch commands
     * that create, start, stop, delete, or expire timers. */
    vdrTimerCore_Init(&server_state->timer_core);

    /* These global handles are inserted into the main WaitForMultipleObjects
     * set. Their values decide how timer expiry and Ctrl+C wake the server loop. */
    server_state->waitable_timer = CreateWaitableTimerW(NULL, TRUE, NULL);
    server_state->shutdown_event = CreateEventW(NULL, TRUE, FALSE, NULL);

    if ((server_state->waitable_timer == NULL) ||
        (server_state->shutdown_event == NULL)) {
        PRINTOUT_TRACE("[server][main] failed to create server events waitable=%p shutdown=%p error=%lu\n",
            server_state->waitable_timer,
            server_state->shutdown_event,
            (unsigned long)GetLastError());
        vdrCleanupServer();
        return 1;
    }

    PRINTOUT_TRACE("[server][main] created waitable_timer=%p shutdown_event=%p\n",
        server_state->waitable_timer,
        server_state->shutdown_event);

    /* Register Ctrl+C handling before accepting clients so console shutdown can
     * wake the same main wait loop used for pipe and timer activity. */
    SetConsoleCtrlHandler(vdrConsoleCtrlHandler, TRUE);
    PRINTOUT_TRACE("[server][main] console control handler installed\n");

    /* Create the first listener before entering the wait loop so clients have
     * a named pipe endpoint to connect to immediately. */
    if (vdrCreateAndArmListener() == FALSE) {
        PRINTOUT_TRACE("[server][main] failed to create initial listener pipe\n");
        vdrCleanupServer();
        return 1;
    }

    PRINTOUT_TRACE("[server][main] vdrapkinTimerManager started on %ls\n", PIPE_NAME);
    running = TRUE;

    while (running == TRUE) {
        HANDLE wait_handles[TIMER_SERVER_MAX_WAIT_HANDLES];
        DWORD pipe_index;
        DWORD wait_count;
        DWORD wait_result;
        DWORD waitable_timer_index;
        DWORD shutdown_index;
        uint64_t next_wait_ms;
        BOOL timer_in_wait_set;

        for (pipe_index = 0; pipe_index < server_state->pipe_count; ++pipe_index) {
            wait_handles[pipe_index] =
                server_state->pipes[pipe_index]->overlapped.hEvent;
        }

        /* Pipe events are already placed at the beginning of wait_handles, so
         * the next available wait slot starts immediately after the pipe list. */
        wait_count = server_state->pipe_count;

        /* The timer handle is not part of the wait set until the timer core
         * confirms that at least one active timer exists. */
        waitable_timer_index = TIMER_SERVER_MAX_WAIT_HANDLES;

        /* The timer core fills this with the delay to the next scheduled
         * expiry; the value is printed for traceability of timer scheduling. */
        next_wait_ms = 0;

        /* Querying the active timer list determines whether the server loop
         * should wait on the waitable timer during this iteration. */
        timer_in_wait_set = vdrTimerCore_GetNextWaitDuration(
            &server_state->timer_core,
            vdrGetTimeMs(),
            &next_wait_ms);

        if (timer_in_wait_set == TRUE) {
            /* The waitable timer is meaningful only while an active timer
             * exists. After the active list becomes empty, excluding the handle
             * prevents an old signaled state from spinning the wait loop. */
            waitable_timer_index = wait_count;
            wait_handles[wait_count] = server_state->waitable_timer;
            wait_count++;
        }

        /* Add the shutdown event as the final wait handle so Ctrl+C or console-close
         * can wake the main server loop regardless of pipe or timer activity. */
        shutdown_index = wait_count;
        wait_handles[shutdown_index] = server_state->shutdown_event;
        wait_count++;

        PRINTOUT_TRACE("[server][wait] waiting handles=%lu pipes=%lu timer_active=%u waitable_timer_index=%lu next_wait=%llu shutdown_index=%lu\n",
            (unsigned long)wait_count,
            (unsigned long)server_state->pipe_count,
            (unsigned)(timer_in_wait_set == TRUE),
            (unsigned long)waitable_timer_index,
            (unsigned long long)next_wait_ms,
            (unsigned long)shutdown_index);

        /* Wait until a pipe operation completes, a timer expires, or server
         * shutdown is requested. */
        wait_result = WaitForMultipleObjects(wait_count, wait_handles, FALSE, INFINITE);

        if ((wait_result < WAIT_OBJECT_0) || (wait_result >= (WAIT_OBJECT_0 + wait_count))) {
            PRINTOUT_TRACE("[server][wait] unexpected result=%lu error=%lu\n",
                (unsigned long)wait_result,
                (unsigned long)GetLastError());
            printf("\n\n");
            fflush(stdout);
            continue;
        }

        pipe_index = wait_result - WAIT_OBJECT_0;
        PRINTOUT_TRACE("[server][wait] signaled index=%lu\n", (unsigned long)pipe_index);

        if (pipe_index == shutdown_index) {
            /* Ctrl+C or console-close requested graceful shutdown. */
            PRINTOUT_TRACE("[server][wait] shutdown event signaled\n");
            running = FALSE;
            printf("\n\n");
            fflush(stdout);
            continue;
        }

        if (pipe_index == waitable_timer_index) {
            /* The single waitable timer represents the active-list head. */
            PRINTOUT_TRACE("[server][wait] waitable timer signaled\n");
            vdrProcessTimerExpiry();
            printf("\n\n");
            fflush(stdout);
            continue;
        }

        {
            PipeInstance_type* instance = server_state->pipes[pipe_index];

            PRINTOUT_TRACE("[server][wait] pipe event signaled index=%lu pipe=%p role=%s operation=%s\n",
                (unsigned long)pipe_index,
                instance->pipe,
                vdrServer_PipeRoleName(instance->role),
                vdrServer_PipeOperationName(instance->operation));

            if (instance->role == vdrPIPE_ROLE_LISTENER) {
                DWORD ignored;

                /* Complete the listener's overlapped ConnectNamedPipe before
                 * reusing the same pipe instance as a connected client. */
                (void)GetOverlappedResult(instance->pipe, &instance->overlapped, &ignored, FALSE);
                PRINTOUT_TRACE("[server][accept] client connected on listener pipe=%p\n", instance->pipe);

                if (server_state->pipe_count >= (TIMER_SERVER_MAX_CLIENTS + 1)) {
                    PRINTOUT_TRACE("[server][accept] rejecting excess client; maximum=%u\n",
                        TIMER_SERVER_MAX_CLIENTS);

                    /* Disconnect the excess connection without promoting this
                     * reserved listener. Rearming the same instance preserves
                     * capacity for a future connection after a client leaves. */
                    DisconnectNamedPipe(instance->pipe);

                    if (vdrArmListener(instance) == FALSE) {
                        PRINTOUT_TRACE("[server][accept] failed to rearm listener after capacity rejection\n");
                        (void)vdrRemovePipeAt(pipe_index);
                        vdrEnsureListenerAvailable();
                    }

                    printf("\n\n");
                    fflush(stdout);
                    continue;
                }

                instance->role = vdrPIPE_ROLE_CLIENT;

                /* The listener pipe is now a connected client pipe, so arm it
                 * for commands and create a new listener for future clients. */
                if (vdrArmRead(instance) == FALSE) {
                    PRINTOUT_TRACE("[server][accept] failed to arm read for connected pipe=%p\n", instance->pipe);
                    (void)vdrRemovePipeAt(pipe_index);
                    vdrEnsureListenerAvailable();
                    printf("\n\n");
                    fflush(stdout);
                    continue;
                }

                /* Replace the listener that just became a client so the server
                 * keeps accepting new connections while this client is active. */
                if (vdrCreateAndArmListener() == FALSE) {
                    PRINTOUT_TRACE("[server][accept] failed to arm replacement listener\n");
                }

                printf("\n\n");
                fflush(stdout);
                continue;
            }

            if (GetOverlappedResult(instance->pipe, &instance->overlapped, &instance->bytes_transferred, FALSE) == FALSE) {
                PRINTOUT_TRACE("[server][read-complete] client disconnected pipe=%p error=%lu\n",
                    instance->pipe,
                    (unsigned long)GetLastError());
                (void)vdrRemovePipeAt(pipe_index);
                vdrEnsureListenerAvailable();
                vdrRearmWaitableTimer();
                printf("\n\n");
                fflush(stdout);
                continue;
            }

            PRINTOUT_TRACE("[server][read-complete] pipe=%p bytes=%lu\n",
                instance->pipe,
                (unsigned long)instance->bytes_transferred);

            /* A full client command has been read into inbound_message. */
            vdrHandleCommand(instance);

            if (vdrFindPipeByHandle(instance->pipe) != NULL) {
                /* Keep the connection alive by immediately posting the next
                 * overlapped read for this client. */
                if (vdrArmRead(instance) == FALSE) {
                    PRINTOUT_TRACE("[server][read-rearm] failed; client disconnected while rearming pipe=%p\n",
                        instance->pipe);
                    (void)vdrRemovePipeAt(pipe_index);
                    vdrEnsureListenerAvailable();
                    vdrRearmWaitableTimer();
                } else {
                    PRINTOUT_TRACE("[server][read-rearm] success pipe=%p\n", instance->pipe);
                }
            }
        }

        printf("\n\n");
        fflush(stdout);
    }

    PRINTOUT_TRACE("[server][main] vdrapkinTimerManager shutting down\n");

    /* Normal shutdown uses the same cleanup path as startup failure, closing
     * all pipes, deleting timers, and releasing global wait handles. */
    vdrCleanupServer();
    return 0;
} /* end of main() */
