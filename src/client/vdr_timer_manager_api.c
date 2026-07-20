/*
 * File:        vdr_timer_manager_api.c
 * Module:      Timer Manager Client API
 * Author:      Vitaly Drapkin
 * Copyright:   Copyright (c) 2026 Vitaly Drapkin
 * Purpose:     Provide the application-facing timer-management interface.
 *
 * Public APIs:
 *   - vdrTimerManager_CreateTimer() - Create a new inactive timer.
 *   - vdrTimerManager_StartTimer() - Start or restart a one-shot timer.
 *   - vdrTimerManager_StopTimer() - Stop an active timer.
 *   - vdrTimerManager_DeleteTimer() - Delete an existing timer.
 *   - vdrTimerManager_GetNotificationEvent() - Return the expiry event handle.
 *   - vdrTimerManager_GetExpiredTimer() - Retrieve one expired timer ID.
 *
 * Design Overview:
 *   - Sends synchronous timer commands to the server through a named pipe.
 *   - Receives ACK and expiry messages on a dedicated receiver thread.
 *   - Queues expired timer IDs and exposes a waitable notification event.
 *
 * Concurrency:
 *   Public commands are serialized by command_mutex. Shared connection and
 *   ACK state are protected by state_mutex, while notification_mutex protects
 *   the expired-timer queue and notification-event state.
 *
 * Assumptions:
 *   - The first public API operation is vdrTimerManager_CreateTimer().
 *   - Only one command request is outstanding per client process.
 *
 * Revision History:
 *   - 2026-07-01: Grouped mutable client API state behind one accessor.
 *   - 2026-06-30: Serialized concurrent lazy initialization.
 *   - Initial implementation.
 */
#include "vdr_timer_manager_api.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

/*
 * Client API implementation.
 *
 * The application sees synchronous command functions and an asynchronous
 * expiry notification event. Internally, the API keeps one receiver thread that
 * reads all server-to-client messages:
 *
 *   ACK response       -> wakes the currently waiting command API.
 *   TimerExpired event -> queues timer ID and signals notification event.
 *
 * Command functions are serialized with g_command_mutex, so version 1 has one
 * outstanding request at a time per process.
 */

typedef struct vdrExpiredQueueNode_tag {
    /* Timer ID received from a server-side TimerExpired event. */
    TimerId_type timer_id;

    /* Singly-linked queue pointer. The API keeps FIFO order for expiries. */
    struct vdrExpiredQueueNode_tag* next;
} vdrExpiredQueueNode_type;

typedef struct vdrClientApiState_tag {
    /* Executes process-wide synchronization initialization exactly once. */
    INIT_ONCE initialize_once;

    /* Serializes public command APIs so only one request waits for one ACK. */
    CRITICAL_SECTION command_mutex;

    /* Protects pipe handle, receiver-thread state, request ID, and last ACK. */
    CRITICAL_SECTION state_mutex;

    /* Protects the expiry queue and notification-event signal state. */
    CRITICAL_SECTION expiry_mutex;

    /* Connected client-side named-pipe handle. INVALID_HANDLE_VALUE means none. */
    HANDLE pipe;

    /* Background thread that receives ACKs and TimerExpired events. */
    HANDLE receiver_thread;

    /* Signaled by the receiver thread when the expected ACK arrives. */
    HANDLE ack_event;

    /* Manual-reset event exposed to the application for WaitForMultipleObjects. */
    HANDLE notification_event;

    /* TRUE after CreateTimer performs successful lazy initialization. */
    BOOL initialized;

    /* Receiver-thread loop flag. Cleared during cleanup. */
    BOOL receiver_running;

    /* Ensures atexit cleanup is registered only once. */
    BOOL cleanup_registered;

    /* Monotonic command request ID. Zero is avoided. */
    uint32_t next_request_id;

    /* Request ID currently waiting for ACK. Protected by state_mutex. */
    uint32_t waiting_request_id;

    /* Most recent matching ACK copied by the receiver thread. */
    vdrAckRsp_type last_ack;

    /* FIFO queue of expired timer IDs waiting for application retrieval. */
    vdrExpiredQueueNode_type* expired_head;
    vdrExpiredQueueNode_type* expired_tail;
} vdrClientApiState_type;

/* The client API has one process-lifetime state object. Direct access is
 * confined to vdrClientApi_GetState(); all operational functions retrieve the
 * pointer through that accessor. */
static vdrClientApiState_type g_client_api_state = { INIT_ONCE_STATIC_INIT };

/*
 * Return the process-lifetime client API state object.
 *
 * Synchronization requirements still apply to individual members; this
 * accessor establishes an ownership boundary but does not acquire any lock.
 */
static vdrClientApiState_type* vdrClientApi_GetState(void)
{
    return &g_client_api_state;
} /* end of vdrClientApi_GetState() */

/*
 * Return a process-local monotonic timestamp in milliseconds.
 *
 * The server uses the same QueryPerformanceCounter-derived time base, so the
 * StartTimer command can carry an API-side timestamp that lets the server
 * subtract Windows scheduling and pipe-delivery delay from the requested timer
 * duration.
 */
static uint64_t vdrClientApi_GetTimeMs(void)
{
    static LARGE_INTEGER frequency = { 0 };
    LARGE_INTEGER counter;

    if (frequency.QuadPart == 0) {
        QueryPerformanceFrequency(&frequency);
    }

    QueryPerformanceCounter(&counter);
    return (uint64_t)((counter.QuadPart * 1000LL) / frequency.QuadPart);
} /* end of vdrClientApi_GetTimeMs() */

/*
 * Print a client API trace line and flush immediately.
 *
 * Each trace is prefixed with its source file and call-site line number.
 * Immediate flushing lets redirected logs or a live console show the request
 * sequence while it is happening, not only after process exit.
 */
#define PRINTOUT_TRACE(...) do { \
    printf("[%s:%d] ", __FILE__, __LINE__); \
    printf(__VA_ARGS__); \
    fflush(stdout); \
} while (0)

/*
 * Convert message type to readable text for trace prints.
 *
 * The trace output is intentionally simple printf-based logging so this sample
 * can be followed from the console without adding a logging dependency.
 */
static const char* vdrClientApi_MessageTypeName(uint16_t message_type)
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
} /* end of vdrClientApi_MessageTypeName() */

/*
 * Convert ACK status to readable text for trace prints.
 */
static const char* vdrClientApi_AckStatusName(uint32_t ack_status)
{
    switch ((vdrAckStatus_type)ack_status) {
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
} /* end of vdrClientApi_AckStatusName() */

/*
 * Initialize process-wide client API state exactly once.
 *
 * The public API has no Init function by design. Windows INIT_ONCE gives us a
 * thread-safe lazy initializer that runs before the first real client operation
 * touches critical sections or handles.
 */
static BOOL CALLBACK vdrClientApi_InitializeProcessState(
    PINIT_ONCE init_once,
    PVOID parameter,
    PVOID* context)
{
    vdrClientApiState_type* api_state = vdrClientApi_GetState();

    (void)init_once;
    (void)parameter;
    (void)context;

    /* Initialize synchronization objects stored in global API state. These
     * locks define which thread is allowed to advance command, ACK, and expiry
     * queue control flow at any moment. */
    InitializeCriticalSection(&api_state->command_mutex);
    InitializeCriticalSection(&api_state->state_mutex);
    InitializeCriticalSection(&api_state->expiry_mutex);

    /* Global state starts disconnected. This forces the first legal public API
     * call, CreateTimer, to take the lazy initialization path before any command
     * can be written to the server pipe. */
    api_state->pipe = INVALID_HANDLE_VALUE;
    api_state->receiver_thread = NULL;
    api_state->ack_event = NULL;
    api_state->notification_event = NULL;
    api_state->initialized = FALSE;
    api_state->receiver_running = FALSE;
    api_state->cleanup_registered = FALSE;

    /* Request ID zero is reserved for asynchronous server events, so command
     * control flow starts at one and wraps back to one later. */
    api_state->next_request_id = 1;
    api_state->waiting_request_id = 0;
    ZeroMemory(&api_state->last_ack, sizeof(api_state->last_ack));

    /* Empty queue state means the application notification event has no expiry
     * work to report until the receiver thread appends the first node. */
    api_state->expired_head = NULL;
    api_state->expired_tail = NULL;

    return TRUE;
} /* end of vdrClientApi_InitializeProcessState() */

/*
 * Clear and free the queued expired timer IDs.
 *
 * The queue is protected while it is detached from global state. Nodes are
 * freed after releasing the lock so cleanup does not hold the expiry mutex
 * during heap operations.
 */
static void vdrClientApi_FreeExpiredQueue(void)
{
    vdrClientApiState_type* api_state = vdrClientApi_GetState();
    vdrExpiredQueueNode_type* current = NULL;

    EnterCriticalSection(&api_state->expiry_mutex);
    current = api_state->expired_head;

    /* Detach all queued expiries from global state. From this point on,
     * GetExpiredTimer sees an empty queue and cannot return nodes that cleanup
     * is about to free. */
    api_state->expired_head = NULL;
    api_state->expired_tail = NULL;

    if (api_state->notification_event != NULL) {
        /* No queued IDs remain, so the application-visible wait handle must stop
         * waking WaitForMultipleObjects until a future expiry is queued. */
        ResetEvent(api_state->notification_event);
    }

    LeaveCriticalSection(&api_state->expiry_mutex);

    while (current != NULL) {
        vdrExpiredQueueNode_type* next = current->next;
        free(current);
        current = next;
    }
} /* end of vdrClientApi_FreeExpiredQueue() */

/*
 * Release client API resources at process shutdown.
 *
 * The sample registers this with atexit after first initialization. Cleanup
 * closes the pipe, asks the receiver thread to exit, releases queued expiries,
 * and closes local events. The server also cleans up timers when it observes
 * the pipe disconnect.
 */
static void vdrClientApi_Cleanup(void)
{
    vdrClientApiState_type* api_state = vdrClientApi_GetState();
    HANDLE receiver_thread = NULL;
    DWORD wait_result = WAIT_FAILED;

    InitOnceExecuteOnce(&api_state->initialize_once, vdrClientApi_InitializeProcessState, NULL, NULL);

    PRINTOUT_TRACE("[client-api] cleanup begin\n");

    EnterCriticalSection(&api_state->state_mutex);
    receiver_thread = api_state->receiver_thread;

    /* Stop the receiver loop so shutdown control flow owns the pipe and shared
     * API state instead of the background read path. */
    api_state->receiver_running = FALSE;

    if (receiver_thread != NULL) {
        /* The receiver thread blocks in a synchronous ReadFile. Cancel it
         * directly so process exit is not held open by the background reader. */
        PRINTOUT_TRACE("[client-api] cleanup canceling receiver thread synchronous read\n");
        (void)CancelSynchronousIo(receiver_thread);
    }

    if (api_state->pipe != INVALID_HANDLE_VALUE) {
        /* Also request cancellation on the pipe handle before closing it. */
        PRINTOUT_TRACE("[client-api] cleanup closing pipe handle\n");
        CancelIoEx(api_state->pipe, NULL);
        CloseHandle(api_state->pipe);

        /* Mark the pipe unusable before releasing state_mutex. Any racing public
         * API call must fail instead of writing to a closed handle. */
        api_state->pipe = INVALID_HANDLE_VALUE;
    }

    /* Remove the live receiver/initialized state. Future public calls must go
     * back through initialization rather than waiting for this old receiver. */
    api_state->receiver_thread = NULL;
    api_state->initialized = FALSE;
    LeaveCriticalSection(&api_state->state_mutex);

    if (receiver_thread != NULL) {
        wait_result = WaitForSingleObject(receiver_thread, 2000);

        PRINTOUT_TRACE("[client-api] cleanup receiver thread wait result=%lu\n",
            (unsigned long)wait_result);

        CloseHandle(receiver_thread);
    }

    vdrClientApi_FreeExpiredQueue();

    if (api_state->ack_event != NULL) {
        CloseHandle(api_state->ack_event);

        /* A NULL ACK event prevents later command paths from waiting on a stale
         * event after cleanup has closed the handle. */
        api_state->ack_event = NULL;
    }

    if (api_state->notification_event != NULL) {
        CloseHandle(api_state->notification_event);

        /* A NULL notification event prevents the application from being given a
         * stale expiry wait handle after cleanup. */
        api_state->notification_event = NULL;
    }

    PRINTOUT_TRACE("[client-api] cleanup complete\n");
} /* end of vdrClientApi_Cleanup() */

/*
 * Populate the common protocol header for an outbound client command.
 *
 * request_id may be zero at initial construction time. The send helper assigns
 * the real request ID immediately before writing the command to the pipe.
 */
static void vdrClientApi_FillHeader(
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
} /* end of vdrClientApi_FillHeader() */

/*
 * Validate the protocol header received from the server before interpreting
 * the union payload.
 *
 * This prevents stale bytes, wrong protocol versions, or accidental traffic on
 * the pipe from being treated as a valid ACK or expiry event.
 */
static BOOL vdrClientApi_IsValidServerHeader(
    const vdrMessageHeader_type* header,
    DWORD bytes_read)
{
    uint32_t expected_size = 0;

    assert(header != NULL);

    if (header == NULL) {
        return FALSE;
    }

    if (bytes_read < sizeof(vdrMessageHeader_type)) {
        return FALSE;
    }

    if ((header->magic != TIMER_PIPE_MAGIC) ||
        (header->version != TIMER_PROTOCOL_VERSION)) {
        return FALSE;
    }

    if (header->message_type == vdrMESSAGE_TYPE_ACK_RSP) {
        expected_size = sizeof(vdrAckRsp_type);
    } else if (header->message_type == vdrMESSAGE_TYPE_TIMER_EXPIRED_EVT) {
        expected_size = sizeof(vdrTimerExpiredEvt_type);
    } else {
        return FALSE;
    }

    if (bytes_read != expected_size) {
        return FALSE;
    }

    if (header->payload_size != vdrPayloadSize(expected_size)) {
        return FALSE;
    }

    return TRUE;
} /* end of vdrClientApi_IsValidServerHeader() */

/*
 * Add an expired timer ID to the application-visible expiry queue.
 *
 * The notification event is manual-reset and remains signaled until the queue
 * is drained by vdrTimerManager_GetExpiredTimer().
 */
static void vdrClientApi_QueueExpiredTimer(TimerId_type timer_id)
{
    vdrClientApiState_type* api_state = vdrClientApi_GetState();
    vdrExpiredQueueNode_type* node = NULL;

    if (timer_id == TIMER_ID_INVALID) {
        PRINTOUT_TRACE("[client-api][error] invalid timer expiry indication received: timer=%llu\n",
            (unsigned long long)timer_id);
        return;
    }

    node = (vdrExpiredQueueNode_type*)malloc(sizeof(vdrExpiredQueueNode_type));

    if (node == NULL) {
        PRINTOUT_TRACE("[client-api][error] failed to allocate expiry queue node for timer=%llu\n",
            (unsigned long long)timer_id);
        return;
    }

    ZeroMemory(node, sizeof(vdrExpiredQueueNode_type));
    node->timer_id = timer_id;
    node->next = NULL;

    EnterCriticalSection(&api_state->expiry_mutex);

    /* FIFO insertion keeps the application-visible expiry order identical to
     * the server event order. */
    if (api_state->expired_tail == NULL) {
        /* First queued expiry changes the API from "no expiry work pending" to
         * "application should wake and drain one or more timer IDs." */
        api_state->expired_head = node;
        api_state->expired_tail = node;
    } else {
        /* Appending preserves FIFO delivery while keeping the notification
         * event's meaning as "the queue is still non-empty." */
        api_state->expired_tail->next = node;
        api_state->expired_tail = node;
    }

    /* Signal the application-visible expiry event after publishing the queue
     * node. This wakes any WaitForMultipleObjects caller and guarantees that
     * vdrTimerManager_GetExpiredTimer can return at least one timer ID. */
    SetEvent(api_state->notification_event);

    LeaveCriticalSection(&api_state->expiry_mutex);

    PRINTOUT_TRACE("[client-api] queued timer expiry indication: timer=%llu\n",
        (unsigned long long)timer_id);
} /* end of vdrClientApi_QueueExpiredTimer() */

/*
 * Receiver thread for all server-to-client pipe messages.
 *
 * The client writes commands synchronously, but reads are centralized here so
 * asynchronous TimerExpired events can arrive at any time. ACKs are matched by
 * request_id to the currently waiting command.
 */
static DWORD WINAPI vdrClientApi_ReceiverThread(LPVOID parameter)
{
    vdrClientApiState_type* api_state = vdrClientApi_GetState();
    HANDLE read_event = NULL;

    (void)parameter;

    read_event = CreateEventW(NULL, TRUE, FALSE, NULL);

    if (read_event == NULL) {
        PRINTOUT_TRACE("[client-api] receiver failed to create read event error=%lu\n",
            (unsigned long)GetLastError());

        EnterCriticalSection(&api_state->state_mutex);

        /* Receiver startup failed. Mark the global receiver as stopped so the
         * public command path does not wait for ACK delivery from a dead thread. */
        api_state->receiver_running = FALSE;

        if (api_state->ack_event != NULL) {
            /* If initialization already has a command waiting, publish an error
             * ACK state and signal the ACK event so the blocked API call wakes
             * from WaitForSingleObject and returns the failure to its caller. */
            api_state->last_ack.ack_status = vdrACK_STATUS_INTERNAL_ERROR;

            SetEvent(api_state->ack_event);
        }

        LeaveCriticalSection(&api_state->state_mutex);
        return 0;
    }

    while (TRUE) {
        vdrServerToClientMessage_type message;
        OVERLAPPED overlapped;
        DWORD bytes_read = 0;
        DWORD error = ERROR_SUCCESS;
        DWORD wait_result = WAIT_FAILED;
        BOOL ok = FALSE;

        EnterCriticalSection(&api_state->state_mutex);

        if ((api_state->receiver_running == FALSE) || (api_state->pipe == INVALID_HANDLE_VALUE)) {
            LeaveCriticalSection(&api_state->state_mutex);
            break;
        }

        LeaveCriticalSection(&api_state->state_mutex);

        ZeroMemory(&message, sizeof(message));
        ZeroMemory(&overlapped, sizeof(overlapped));
        ResetEvent(read_event);
        overlapped.hEvent = read_event;

        /* The client pipe handle is opened overlapped so this receiver read
         * does not prevent the public API thread from writing the next command
         * through the same duplex pipe handle. */
        ok = ReadFile(api_state->pipe, &message, sizeof(message), NULL, &overlapped);

        if (ok == FALSE) {
            error = GetLastError();

            if (error != ERROR_IO_PENDING) {
                PRINTOUT_TRACE("[client-api] receiver ReadFile ended error=%lu\n",
                    (unsigned long)error);
                break;
            }

            wait_result = WaitForSingleObject(read_event, INFINITE);

            if (wait_result != WAIT_OBJECT_0) {
                PRINTOUT_TRACE("[client-api] receiver read wait ended result=%lu\n",
                    (unsigned long)wait_result);
                break;
            }
        }

        ok = GetOverlappedResult(api_state->pipe, &overlapped, &bytes_read, FALSE);

        if (ok == FALSE) {
            PRINTOUT_TRACE("[client-api] receiver overlapped read ended error=%lu\n",
                (unsigned long)GetLastError());
            break;
        }

        PRINTOUT_TRACE("[client-api] receiver read message type=%s request=%u bytes=%lu\n",
            vdrClientApi_MessageTypeName(message.header.message_type),
            message.header.request_id,
            (unsigned long)bytes_read);

        /* Header validation happens before checking message_type so malformed
         * input cannot select the wrong union member. */
        if (vdrClientApi_IsValidServerHeader(&message.header, bytes_read) == FALSE) {
            PRINTOUT_TRACE("[client-api] receiver ignored invalid server message bytes=%lu\n",
                (unsigned long)bytes_read);
            continue;
        }

        if (message.header.message_type == vdrMESSAGE_TYPE_ACK_RSP) {
            EnterCriticalSection(&api_state->state_mutex);

            if (message.header.request_id == api_state->waiting_request_id) {
                /* Copy the ACK while holding state_mutex, then wake the
                 * command API blocked in SendCommandAndWaitForAck. This is
                 * the receiver-to-API handoff for synchronous commands. */
                api_state->last_ack = message.ack_rsp;

                PRINTOUT_TRACE("[client-api] received ACK: request=%u status=%s(%u) timer=%llu\n",
                    message.header.request_id,
                    vdrClientApi_AckStatusName(message.ack_rsp.ack_status),
                    (unsigned)message.ack_rsp.ack_status,
                    (unsigned long long)message.ack_rsp.timer_id);

                /* Signal the ACK event only after last_ack is copied. This
                 * wakes the synchronous command path that is waiting for
                 * this request_id and lets it consume the completed ACK. */
                SetEvent(api_state->ack_event);
            }

            LeaveCriticalSection(&api_state->state_mutex);
        } else if (message.header.message_type == vdrMESSAGE_TYPE_TIMER_EXPIRED_EVT) {
            vdrClientApi_QueueExpiredTimer(message.timer_expired_evt.timer_id);
        }
    }

    CloseHandle(read_event);

    EnterCriticalSection(&api_state->state_mutex);

    /* The receiver loop is leaving, so no future ACK or expiry messages will be
     * delivered by this thread. Global state must reflect that before any
     * blocked command is released. */
    api_state->receiver_running = FALSE;

    if (api_state->ack_event != NULL) {
        /* Publish server loss and signal the ACK event so any command blocked
         * in SendCommandAndWaitForAck wakes and reports the disconnected pipe. */
        api_state->last_ack.ack_status = vdrACK_STATUS_SERVER_NOT_AVAILABLE;

        SetEvent(api_state->ack_event);
    }

    LeaveCriticalSection(&api_state->state_mutex);

    return 0;
} /* end of vdrClientApi_ReceiverThread() */

/*
 * Perform lazy client API initialization.
 *
 * The first legal public API is CreateTimer. It calls this helper to create
 * local events, connect to the already-running server, set message read mode,
 * and start the receiver thread. No server process is launched here.
 */
static vdrAckStatus_type vdrClientApi_EnsureInitialized(void)
{
    vdrClientApiState_type* api_state = vdrClientApi_GetState();
    BOOL ok = FALSE;

    InitOnceExecuteOnce(&api_state->initialize_once, vdrClientApi_InitializeProcessState, NULL, NULL);

    /* Serialize the complete lazy-initialization sequence. Concurrent first
     * CreateTimer calls must share one pipe, event pair, and receiver thread. */
    EnterCriticalSection(&api_state->command_mutex);

    EnterCriticalSection(&api_state->state_mutex);

    if (api_state->initialized == TRUE) {
        LeaveCriticalSection(&api_state->state_mutex);
        LeaveCriticalSection(&api_state->command_mutex);
        return vdrACK_STATUS_OK;
    }

    LeaveCriticalSection(&api_state->state_mutex);

    /* These global events define the two client-side wait paths: ACK event for
     * synchronous command completion and notification event for application
     * expiry delivery through WaitForMultipleObjects. */
    api_state->ack_event = CreateEventW(NULL, TRUE, FALSE, NULL);
    api_state->notification_event = CreateEventW(NULL, TRUE, FALSE, NULL);

    if ((api_state->ack_event == NULL) || (api_state->notification_event == NULL)) {
        PRINTOUT_TRACE("[client-api] initialization failed creating events error=%lu\n",
            (unsigned long)GetLastError());
        LeaveCriticalSection(&api_state->command_mutex);
        return vdrACK_STATUS_INTERNAL_ERROR;
    }

    if (api_state->cleanup_registered == FALSE) {
        atexit(vdrClientApi_Cleanup);

        /* After this flag changes, process exit will always route through API
         * cleanup exactly once instead of leaking pipe/event/thread handles. */
        api_state->cleanup_registered = TRUE;
    }

    /* Store the pipe globally so the public command path and receiver thread
     * operate on the same duplex named-pipe connection. */
    api_state->pipe = CreateFileW(
        PIPE_NAME,
        GENERIC_READ | GENERIC_WRITE,
        0,
        NULL,
        OPEN_EXISTING,
        FILE_FLAG_OVERLAPPED,
        NULL);

    if (api_state->pipe == INVALID_HANDLE_VALUE) {
        PRINTOUT_TRACE("[client-api] failed to connect to pipe %ls error=%lu\n",
            PIPE_NAME,
            (unsigned long)GetLastError());
        LeaveCriticalSection(&api_state->command_mutex);
        return vdrACK_STATUS_SERVER_NOT_AVAILABLE;
    }

    PRINTOUT_TRACE("[client-api] connected to pipe %ls handle=%p\n", PIPE_NAME, api_state->pipe);

    {
        /* The server uses message-mode pipes, so the client also requests
         * message read mode to preserve protocol message boundaries. */
        DWORD mode = PIPE_READMODE_MESSAGE;
        ok = SetNamedPipeHandleState(api_state->pipe, &mode, NULL, NULL);

        if (ok == FALSE) {
            PRINTOUT_TRACE("[client-api] failed to set message read mode error=%lu\n",
                (unsigned long)GetLastError());
            CloseHandle(api_state->pipe);

            /* The pipe cannot be used unless message boundaries are preserved,
             * so return the state machine to disconnected before failing init. */
            api_state->pipe = INVALID_HANDLE_VALUE;
            LeaveCriticalSection(&api_state->command_mutex);
            return vdrACK_STATUS_INTERNAL_ERROR;
        }
    }

    PRINTOUT_TRACE("[client-api] pipe read mode set to message mode\n");

    EnterCriticalSection(&api_state->state_mutex);

    /* Let the newly created receiver thread enter its read loop. This must be
     * true before CreateThread can race into vdrClientApi_ReceiverThread. */
    api_state->receiver_running = TRUE;

    /* Store the thread handle globally so cleanup can cancel/join the receiver
     * and so initialized state means ACK/expiry delivery is live. */
    api_state->receiver_thread = CreateThread(NULL, 0, vdrClientApi_ReceiverThread, NULL, 0, NULL);

    if (api_state->receiver_thread == NULL) {
        PRINTOUT_TRACE("[client-api] failed to create receiver thread error=%lu\n",
            (unsigned long)GetLastError());

        /* Roll back the global live state. Without this, command APIs could
         * write requests but no receiver would exist to wake their ACK wait. */
        api_state->receiver_running = FALSE;
        CloseHandle(api_state->pipe);
        api_state->pipe = INVALID_HANDLE_VALUE;
        LeaveCriticalSection(&api_state->state_mutex);
        LeaveCriticalSection(&api_state->command_mutex);
        return vdrACK_STATUS_INTERNAL_ERROR;
    }

    /* Publish successful initialization only after all control-flow machinery
     * is ready: pipe connected, events created, receiver thread running. */
    api_state->initialized = TRUE;
    LeaveCriticalSection(&api_state->state_mutex);

    PRINTOUT_TRACE("[client-api] initialization complete receiver_thread=%p\n", api_state->receiver_thread);

    LeaveCriticalSection(&api_state->command_mutex);
    return vdrACK_STATUS_OK;
} /* end of vdrClientApi_EnsureInitialized() */

/*
 * Send one command and wait for its matching ACK.
 *
 * command_mutex guarantees that only one command is outstanding, but request_id
 * matching is still used so the receiver thread can distinguish ACKs from
 * asynchronous timer-expiry events.
 */
static vdrAckStatus_type vdrClientApi_SendCommandAndWaitForAck(
    vdrClientToServerMessage_type* command,
    uint32_t command_size,
    TimerId_type* out_timer_id)
{
    vdrClientApiState_type* api_state = vdrClientApi_GetState();
    DWORD bytes_written = 0;
    DWORD wait_result = WAIT_FAILED;
    DWORD error = ERROR_SUCCESS;
    HANDLE write_event = NULL;
    OVERLAPPED overlapped;
    BOOL ok = FALSE;
    uint32_t request_id = 0;

    assert(command != NULL);
    assert(command_size >= sizeof(vdrMessageHeader_type));

    if ((command == NULL) || (command_size < sizeof(vdrMessageHeader_type))) {
        return vdrACK_STATUS_INTERNAL_ERROR;
    }

    EnterCriticalSection(&api_state->command_mutex);

    /* Reserve this command's request ID from global state. The receiver thread
     * uses the same value to decide which ACK releases this API call. */
    request_id = api_state->next_request_id++;

    if (api_state->next_request_id == 0) {
        /* Request ID zero is reserved for asynchronous events. */
        api_state->next_request_id = 1;
    }

    command->header.request_id = request_id;

    EnterCriticalSection(&api_state->state_mutex);

    /* Publish the command currently waiting for ACK before writing to the pipe,
     * so an immediate server response can be matched by the receiver thread. */
    api_state->waiting_request_id = request_id;

    /* Clear old ACK state and reset the ACK event. This forces control flow to
     * wait for the ACK belonging to this request rather than a previous one. */
    ZeroMemory(&api_state->last_ack, sizeof(api_state->last_ack));
    ResetEvent(api_state->ack_event);
    LeaveCriticalSection(&api_state->state_mutex);

    ZeroMemory(&overlapped, sizeof(overlapped));
    write_event = CreateEventW(NULL, TRUE, FALSE, NULL);

    if (write_event == NULL) {
        LeaveCriticalSection(&api_state->command_mutex);
        return vdrACK_STATUS_INTERNAL_ERROR;
    }

    overlapped.hEvent = write_event;

    PRINTOUT_TRACE("[client-api] sending request: request=%u type=%s payload=%u\n",
        request_id,
        vdrClientApi_MessageTypeName(command->header.message_type),
        command->header.payload_size);

    /* The handle is overlapped because the receiver thread keeps a read armed
     * while public API calls write commands. Wait here to preserve the public
     * API's synchronous command semantics. */
    ok = WriteFile(api_state->pipe, command, command_size, NULL, &overlapped);

    if (ok == FALSE) {
        error = GetLastError();

        if (error != ERROR_IO_PENDING) {
            CloseHandle(write_event);
            PRINTOUT_TRACE("[client-api] WriteFile failed request=%u error=%lu\n",
                request_id,
                (unsigned long)error);
            LeaveCriticalSection(&api_state->command_mutex);
            return vdrACK_STATUS_SERVER_NOT_AVAILABLE;
        }

        wait_result = WaitForSingleObject(write_event, INFINITE);

        if (wait_result != WAIT_OBJECT_0) {
            CloseHandle(write_event);
            PRINTOUT_TRACE("[client-api] write wait failed request=%u wait_result=%lu\n",
                request_id,
                (unsigned long)wait_result);
            LeaveCriticalSection(&api_state->command_mutex);
            return vdrACK_STATUS_INTERNAL_ERROR;
        }
    }

    if (GetOverlappedResult(api_state->pipe, &overlapped, &bytes_written, FALSE) == FALSE) {
        error = GetLastError();
        CloseHandle(write_event);
        PRINTOUT_TRACE("[client-api] WriteFile failed request=%u error=%lu\n",
            request_id,
            (unsigned long)error);
        LeaveCriticalSection(&api_state->command_mutex);
        return vdrACK_STATUS_SERVER_NOT_AVAILABLE;
    }

    CloseHandle(write_event);

    PRINTOUT_TRACE("[client-api] request written: request=%u bytes=%lu; waiting for ACK\n",
        request_id,
        (unsigned long)bytes_written);

    wait_result = WaitForSingleObject(api_state->ack_event, INFINITE);

    if (wait_result != WAIT_OBJECT_0) {
        PRINTOUT_TRACE("[client-api] ACK wait failed request=%u wait_result=%lu\n",
            request_id,
            (unsigned long)wait_result);
        LeaveCriticalSection(&api_state->command_mutex);
        return vdrACK_STATUS_INTERNAL_ERROR;
    }

    EnterCriticalSection(&api_state->state_mutex);

    if (out_timer_id != NULL) {
        /* CreateTimer needs the new ID. Other commands may ignore this echo. */
        *out_timer_id = api_state->last_ack.timer_id;
    }

    {
        const vdrAckStatus_type status = (vdrAckStatus_type)api_state->last_ack.ack_status;
        PRINTOUT_TRACE("[client-api] completed request: request=%u type=%s status=%s(%u) timer=%llu\n",
            request_id,
            vdrClientApi_MessageTypeName(command->header.message_type),
            vdrClientApi_AckStatusName((uint32_t)status),
            (unsigned)status,
            (unsigned long long)api_state->last_ack.timer_id);

        /* Clear the global waiter marker before releasing command_mutex so the
         * next command starts with a clean ACK matching state. */
        api_state->waiting_request_id = 0;
        LeaveCriticalSection(&api_state->state_mutex);
        LeaveCriticalSection(&api_state->command_mutex);
        return status;
    }
} /* end of vdrClientApi_SendCommandAndWaitForAck() */

/*
 * Public API: create an inactive timer.
 *
 * This is the first legal API call. It performs lazy initialization and then
 * sends CreateTimer to the server. On success, out_timer_id receives a nonzero
 * server-assigned timer ID.
 */
vdrAckStatus_type vdrTimerManager_CreateTimer(TimerId_type* out_timer_id)
{
    vdrClientToServerMessage_type command;
    vdrAckStatus_type status;

    assert(out_timer_id != NULL);

    if (out_timer_id == NULL) {
        return vdrACK_STATUS_INTERNAL_ERROR;
    }

    *out_timer_id = TIMER_ID_INVALID;

    /* CreateTimer is the lazy-initialization entry point. No command is built
     * or sent until the API has a pipe, events, and receiver thread. */
    status = vdrClientApi_EnsureInitialized();

    if (status != vdrACK_STATUS_OK) {
        return status;
    }

    /* Build the fixed-size CreateTimer request. The send helper will assign
     * the unique request ID immediately before writing it to the pipe. */
    ZeroMemory(&command, sizeof(command));

    vdrClientApi_FillHeader(
        &command.create_timer_req.header,
        vdrMESSAGE_TYPE_CREATE_TIMER_REQ,
        0,
        sizeof(vdrCreateTimerReq_type));

    /* Send the command synchronously and return the timer ID carried by the
     * matching ACK. */
    return vdrClientApi_SendCommandAndWaitForAck(
        &command,
        sizeof(vdrCreateTimerReq_type),
        out_timer_id);
} /* end of vdrTimerManager_CreateTimer() */

/*
 * Public API: start or restart a timer with a one-shot duration.
 *
 * Parameter validation happens before writing to the pipe. A zero duration is a
 * user error and is returned locally as INVALID_DURATION.
 */
vdrAckStatus_type vdrTimerManager_StartTimer(TimerId_type timer_id, uint64_t duration_ms)
{
    vdrClientApiState_type* api_state = vdrClientApi_GetState();
    vdrClientToServerMessage_type command;

    if (timer_id == TIMER_ID_INVALID) {
        return vdrACK_STATUS_INVALID_TIMER_ID;
    }

    if (duration_ms == 0) {
        return vdrACK_STATUS_INVALID_DURATION;
    }

    if (api_state->initialized == FALSE) {
        return vdrACK_STATUS_SERVER_NOT_AVAILABLE;
    }

    /* Build the fixed-size StartTimer request after all public parameters are
     * validated. Internal execution starts only after these checks pass. */
    ZeroMemory(&command, sizeof(command));

    vdrClientApi_FillHeader(
        &command.start_timer_req.header,
        vdrMESSAGE_TYPE_START_TIMER_REQ,
        0,
        sizeof(vdrStartTimerReq_type));
    command.start_timer_req.timer_id = timer_id;
    command.start_timer_req.duration_ms = duration_ms;

    /* The accepted API call owns the logical start time. Store this timestamp
     * directly in the request after all parameters are validated but before the
     * request is sent, so the server can account for Windows-side scheduling
     * and pipe delivery delay. */
    command.start_timer_req.api_start_timestamp_ms = vdrClientApi_GetTimeMs();

    PRINTOUT_TRACE("[client-api] StartTimer accepted timer=%llu duration_ms=%llu api_start_timestamp_ms=%llu\n",
        (unsigned long long)timer_id,
        (unsigned long long)duration_ms,
        (unsigned long long)command.start_timer_req.api_start_timestamp_ms);

    /* The server echoes command completion through the generic ACK. */
    return vdrClientApi_SendCommandAndWaitForAck(&command, sizeof(vdrStartTimerReq_type), NULL);
} /* end of vdrTimerManager_StartTimer() */

/*
 * Public API: stop a timer.
 *
 * The server treats stopping an inactive timer as success, but invalid IDs are
 * rejected here before the request is sent.
 */
vdrAckStatus_type vdrTimerManager_StopTimer(TimerId_type timer_id)
{
    vdrClientApiState_type* api_state = vdrClientApi_GetState();
    vdrClientToServerMessage_type command;

    if (timer_id == TIMER_ID_INVALID) {
        return vdrACK_STATUS_INVALID_TIMER_ID;
    }

    if (api_state->initialized == FALSE) {
        return vdrACK_STATUS_SERVER_NOT_AVAILABLE;
    }

    /* Build the fixed-size StopTimer request only after the timer ID is known
     * to be syntactically valid. */
    ZeroMemory(&command, sizeof(command));

    vdrClientApi_FillHeader(
        &command.stop_timer_req.header,
        vdrMESSAGE_TYPE_STOP_TIMER_REQ,
        0,
        sizeof(vdrStopTimerReq_type));
    command.stop_timer_req.timer_id = timer_id;

    /* Wait for the server ACK so the caller knows whether the stop succeeded. */
    return vdrClientApi_SendCommandAndWaitForAck(&command, sizeof(vdrStopTimerReq_type), NULL);
} /* end of vdrTimerManager_StopTimer() */

/*
 * Public API: delete a timer.
 *
 * A deleted timer ID becomes invalid for subsequent Start/Stop/Delete calls.
 */
vdrAckStatus_type vdrTimerManager_DeleteTimer(TimerId_type timer_id)
{
    vdrClientApiState_type* api_state = vdrClientApi_GetState();
    vdrClientToServerMessage_type command;

    if (timer_id == TIMER_ID_INVALID) {
        return vdrACK_STATUS_INVALID_TIMER_ID;
    }

    if (api_state->initialized == FALSE) {
        return vdrACK_STATUS_SERVER_NOT_AVAILABLE;
    }

    /* Build the fixed-size DeleteTimer request. Delete removes server-side
     * timer ownership if the ACK is positive. */
    ZeroMemory(&command, sizeof(command));

    vdrClientApi_FillHeader(
        &command.delete_timer_req.header,
        vdrMESSAGE_TYPE_DELETE_TIMER_REQ,
        0,
        sizeof(vdrDeleteTimerReq_type));
    command.delete_timer_req.timer_id = timer_id;

    /* Wait for the ACK before returning so the application knows whether the
     * timer ID is still valid. */
    return vdrClientApi_SendCommandAndWaitForAck(&command, sizeof(vdrDeleteTimerReq_type), NULL);
} /* end of vdrTimerManager_DeleteTimer() */

/*
 * Public API: return the application wait handle for timer expiry events.
 *
 * The returned HANDLE is owned by the API. The application waits on it but must
 * not close it.
 */
vdrAckStatus_type vdrTimerManager_GetNotificationEvent(HANDLE* out_event)
{
    vdrClientApiState_type* api_state = vdrClientApi_GetState();

    assert(out_event != NULL);

    if (out_event == NULL) {
        return vdrACK_STATUS_INTERNAL_ERROR;
    }

    if (api_state->initialized == FALSE) {
        *out_event = NULL;
        return vdrACK_STATUS_SERVER_NOT_AVAILABLE;
    }

    /* Hand out the API-owned event handle. It is shared by all timers for this
     * client and is signaled when the expiry queue becomes non-empty. */
    *out_event = api_state->notification_event;
    return vdrACK_STATUS_OK;
} /* end of vdrTimerManager_GetNotificationEvent() */

/*
 * Public API: pop one expired timer ID from the API queue.
 *
 * The notification event stays signaled while the queue is non-empty. When the
 * last item is removed, the event is reset so WaitForMultipleObjects will block
 * again until a future expiry arrives.
 */
vdrAckStatus_type vdrTimerManager_GetExpiredTimer(TimerId_type* out_timer_id)
{
    vdrClientApiState_type* api_state = vdrClientApi_GetState();
    vdrExpiredQueueNode_type* node = NULL;

    assert(out_timer_id != NULL);

    if (out_timer_id == NULL) {
        return vdrACK_STATUS_INTERNAL_ERROR;
    }

    *out_timer_id = TIMER_ID_INVALID;

    if (api_state->initialized == FALSE) {
        return vdrACK_STATUS_SERVER_NOT_AVAILABLE;
    }

    EnterCriticalSection(&api_state->expiry_mutex);
    node = api_state->expired_head;

    if (node == NULL) {
        /* No queued expiry exists, so the application wait handle must be reset
         * to make future WaitForMultipleObjects calls block again. */
        ResetEvent(api_state->notification_event);
        LeaveCriticalSection(&api_state->expiry_mutex);
        return vdrACK_STATUS_NO_EXPIRED_TIMERS;
    }

    /* Remove one expiry from the global queue. This advances application-level
     * control flow by transferring one timer ID from API-owned storage to the
     * caller's out_timer_id. */
    api_state->expired_head = node->next;

    if (api_state->expired_head == NULL) {
        /* The drained node was the last item. Clear the tail and reset the
         * event so the application stops waking until another expiry arrives. */
        api_state->expired_tail = NULL;
        ResetEvent(api_state->notification_event);
    }

    LeaveCriticalSection(&api_state->expiry_mutex);

    /* Copy the timer ID out before freeing the queue node. Ownership of the
     * heap node remains inside the API; only the ID value is returned. */
    *out_timer_id = node->timer_id;

    PRINTOUT_TRACE("[client-api] application retrieved expired timer: timer=%llu\n",
        (unsigned long long)*out_timer_id);

    /* The popped queue node is no longer reachable from global state. */
    free(node);

    return vdrACK_STATUS_OK;
} /* end of vdrTimerManager_GetExpiredTimer() */
