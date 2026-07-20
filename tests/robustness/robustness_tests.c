/*
 * File:        robustness_tests.c
 * Module:      Timer Manager Robustness Tests
 * Author:      Vitaly Drapkin
 * Copyright:   Copyright (c) 2026 Vitaly Drapkin
 * Purpose:     Verify protocol, capacity, and concurrent-client boundaries.
 *
 * Design Overview:
 *   - Sends deliberately malformed raw messages through real named pipes.
 *   - Fills the server client and WaitForMultipleObjects capacity boundary.
 *   - Starts concurrent public API callers before lazy initialization completes.
 *
 * Concurrency:
 *   Raw protocol and capacity scenarios are single-threaded. The concurrency
 *   scenario releases eight worker threads through one manual-reset event.
 *
 * Assumptions:
 *   - vdrapkinTimerManager is already running and accepting pipe connections.
 *   - Each CTest scenario runs in a new process with fresh client API state.
 *
 * Revision History:
 *   - Initial implementation.
 */
#include <windows.h>
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "vdr_timer_manager_api.h"

#define ROBUSTNESS_PIPE_TIMEOUT_MS 5000
#define ROBUSTNESS_CAPACITY_REJECT_TIMEOUT_MS 3000
#define ROBUSTNESS_CONCURRENT_THREAD_COUNT 8
#define ROBUSTNESS_CONCURRENT_WAIT_MS 30000

#define CHECK(expr) do { \
    if (!(expr)) { \
        printf("CHECK failed at %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        return 1; \
    } \
} while (0)

#define PRINTOUT_TRACE(...) do { \
    printf("[%s:%d] ", __FILE__, __LINE__); \
    printf(__VA_ARGS__); \
    fflush(stdout); \
} while (0)

typedef int (*RobustnessTestFunction_type)(void);

typedef struct RobustnessTestCase_tag {
    const char* name;
    RobustnessTestFunction_type function;
} RobustnessTestCase_type;

typedef enum vdrMalformedProtocolCase_tag {
    vdrMALFORMED_PROTOCOL_MAGIC = 0,
    vdrMALFORMED_PROTOCOL_VERSION,
    vdrMALFORMED_PROTOCOL_TYPE,
    vdrMALFORMED_PROTOCOL_TOTAL_SIZE,
    vdrMALFORMED_PROTOCOL_PAYLOAD_SIZE,
    vdrMALFORMED_PROTOCOL_TRUNCATED_HEADER,
    vdrMALFORMED_PROTOCOL_OVERSIZED_MESSAGE,
    vdrMALFORMED_PROTOCOL_PAYLOAD_TOO_SMALL,
    vdrMALFORMED_PROTOCOL_PAYLOAD_TOO_LARGE,
    vdrMALFORMED_PROTOCOL_OLD_START_TIMER_SIZE,
    vdrMALFORMED_PROTOCOL_ACK_SENT_TO_SERVER,
    vdrMALFORMED_PROTOCOL_EXPIRY_SENT_TO_SERVER
} vdrMalformedProtocolCase_type;

typedef struct vdrLegacyStartTimerReq_tag {
    vdrMessageHeader_type header;
    TimerId_type timer_id;
    uint64_t duration_ms;
} vdrLegacyStartTimerReq_type;

typedef struct vdrConcurrentWorkerContext_tag {
    HANDLE start_event;
    TimerId_type timer_id;
    vdrAckStatus_type status;
} vdrConcurrentWorkerContext_type;

/*
 * Fill a raw command header without using the public client API.
 */
static void RobustnessFillHeader(
    vdrMessageHeader_type* header,
    vdrMessageType_type message_type,
    uint32_t request_id,
    uint32_t message_size)
{
    assert(header != NULL);
    assert(message_size >= sizeof(vdrMessageHeader_type));

    if ((header == NULL) || (message_size < sizeof(vdrMessageHeader_type))) {
        return;
    }

    header->magic = TIMER_PIPE_MAGIC;
    header->version = TIMER_PROTOCOL_VERSION;
    header->message_type = (uint16_t)message_type;
    header->request_id = request_id;
    header->payload_size = vdrPayloadSize(message_size);
} /* end of RobustnessFillHeader() */

/*
 * Return the same monotonic millisecond time base used by the client API and
 * server. Raw protocol tests need this because they bypass the public API but
 * still need to populate api_start_timestamp_ms correctly.
 */
static uint64_t RobustnessGetTimeMs(void)
{
    static LARGE_INTEGER frequency = { 0 };
    LARGE_INTEGER counter;

    if (frequency.QuadPart == 0) {
        QueryPerformanceFrequency(&frequency);
    }

    QueryPerformanceCounter(&counter);
    return (uint64_t)((counter.QuadPart * 1000LL) / frequency.QuadPart);
} /* end of RobustnessGetTimeMs() */

/*
 * Open one synchronous message-mode client pipe within a bounded interval.
 */
static HANDLE RobustnessOpenRawPipe(DWORD timeout_ms)
{
    HANDLE pipe = INVALID_HANDLE_VALUE;
    uint64_t deadline_ms = 0;
    BOOL keep_trying = TRUE;

    assert(timeout_ms > 0);

    if (timeout_ms == 0) {
        return INVALID_HANDLE_VALUE;
    }

    deadline_ms = (uint64_t)GetTickCount64() + timeout_ms;

    while (keep_trying == TRUE) {
        pipe = CreateFileW(
            PIPE_NAME,
            GENERIC_READ | GENERIC_WRITE,
            0,
            NULL,
            OPEN_EXISTING,
            0,
            NULL);

        if (pipe != INVALID_HANDLE_VALUE) {
            DWORD mode = PIPE_READMODE_MESSAGE;

            if (SetNamedPipeHandleState(pipe, &mode, NULL, NULL) == TRUE) {
                keep_trying = FALSE;
            } else {
                CloseHandle(pipe);
                pipe = INVALID_HANDLE_VALUE;
                keep_trying = FALSE;
            }
        } else {
            DWORD error = GetLastError();
            uint64_t now_ms = (uint64_t)GetTickCount64();

            if ((now_ms >= deadline_ms) ||
                ((error != ERROR_PIPE_BUSY) && (error != ERROR_FILE_NOT_FOUND))) {
                keep_trying = FALSE;
            } else {
                Sleep(10);
            }
        }
    }

    return pipe;
} /* end of RobustnessOpenRawPipe() */

/*
 * Wait until an ACK-sized response can be read without an unbounded ReadFile.
 */
static BOOL RobustnessWaitForPipeData(HANDLE pipe, DWORD timeout_ms)
{
    BOOL data_available = FALSE;
    BOOL keep_waiting = TRUE;
    uint64_t deadline_ms = 0;

    assert(pipe != NULL);
    assert(pipe != INVALID_HANDLE_VALUE);
    assert(timeout_ms > 0);

    if ((pipe == NULL) || (pipe == INVALID_HANDLE_VALUE) || (timeout_ms == 0)) {
        return FALSE;
    }

    deadline_ms = (uint64_t)GetTickCount64() + timeout_ms;

    while (keep_waiting == TRUE) {
        DWORD available_bytes = 0;

        if (PeekNamedPipe(pipe, NULL, 0, NULL, &available_bytes, NULL) == FALSE) {
            keep_waiting = FALSE;
        } else if (available_bytes >= sizeof(vdrAckRsp_type)) {
            data_available = TRUE;
            keep_waiting = FALSE;
        } else if ((uint64_t)GetTickCount64() >= deadline_ms) {
            keep_waiting = FALSE;
        } else {
            Sleep(5);
        }
    }

    return data_available;
} /* end of RobustnessWaitForPipeData() */

/*
 * Wait until a caller-selected number of bytes is available on a raw pipe.
 *
 * ACK waits use the ACK size, while immediate-expiry tests wait for a smaller
 * TimerExpired event after the positive StartTimer ACK has already been read.
 */
static BOOL RobustnessWaitForPipeBytes(HANDLE pipe, DWORD timeout_ms, DWORD expected_bytes)
{
    BOOL data_available = FALSE;
    BOOL keep_waiting = TRUE;
    uint64_t deadline_ms = 0;

    assert(pipe != NULL);
    assert(pipe != INVALID_HANDLE_VALUE);
    assert(timeout_ms > 0);
    assert(expected_bytes > 0);

    if ((pipe == NULL) ||
        (pipe == INVALID_HANDLE_VALUE) ||
        (timeout_ms == 0) ||
        (expected_bytes == 0)) {
        return FALSE;
    }

    deadline_ms = (uint64_t)GetTickCount64() + timeout_ms;

    while (keep_waiting == TRUE) {
        DWORD available_bytes = 0;

        if (PeekNamedPipe(pipe, NULL, 0, NULL, &available_bytes, NULL) == FALSE) {
            keep_waiting = FALSE;
        } else if (available_bytes >= expected_bytes) {
            data_available = TRUE;
            keep_waiting = FALSE;
        } else if ((uint64_t)GetTickCount64() >= deadline_ms) {
            keep_waiting = FALSE;
        } else {
            Sleep(5);
        }
    }

    return data_available;
} /* end of RobustnessWaitForPipeBytes() */

/*
 * Send an arbitrary command byte count and read the server's generic ACK.
 */
static BOOL RobustnessSendRawCommand(
    HANDLE pipe,
    const void* command,
    DWORD command_size,
    vdrAckRsp_type* out_ack)
{
    DWORD bytes_written = 0;
    DWORD bytes_read = 0;
    BOOL completed = FALSE;

    assert(pipe != NULL);
    assert(pipe != INVALID_HANDLE_VALUE);
    assert(command != NULL);
    assert(command_size > 0);
    assert(out_ack != NULL);

    if ((pipe == NULL) ||
        (pipe == INVALID_HANDLE_VALUE) ||
        (command == NULL) ||
        (command_size == 0) ||
        (out_ack == NULL)) {
        return FALSE;
    }

    memset(out_ack, 0, sizeof(*out_ack));

    if (WriteFile(pipe, command, command_size, &bytes_written, NULL) == TRUE) {
        if ((bytes_written == command_size) &&
            (RobustnessWaitForPipeData(pipe, ROBUSTNESS_PIPE_TIMEOUT_MS) == TRUE)) {
            if (ReadFile(pipe, out_ack, sizeof(*out_ack), &bytes_read, NULL) == TRUE) {
                if ((bytes_read == sizeof(*out_ack)) &&
                    (out_ack->header.magic == TIMER_PIPE_MAGIC) &&
                    (out_ack->header.version == TIMER_PROTOCOL_VERSION) &&
                    (out_ack->header.message_type == vdrMESSAGE_TYPE_ACK_RSP) &&
                    (out_ack->header.payload_size == vdrPayloadSize(sizeof(*out_ack)))) {
                    completed = TRUE;
                }
            }
        }
    }

    return completed;
} /* end of RobustnessSendRawCommand() */

/*
 * Create one timer through a raw pipe and return the ACK timer ID.
 */
static BOOL RobustnessRawCreateTimer(
    HANDLE pipe,
    uint32_t request_id,
    TimerId_type* out_timer_id)
{
    vdrCreateTimerReq_type request;
    vdrAckRsp_type ack;
    BOOL created = FALSE;

    assert(out_timer_id != NULL);

    if (out_timer_id == NULL) {
        return FALSE;
    }

    *out_timer_id = TIMER_ID_INVALID;
    memset(&request, 0, sizeof(request));
    RobustnessFillHeader(
        &request.header,
        vdrMESSAGE_TYPE_CREATE_TIMER_REQ,
        request_id,
        sizeof(request));

    if (RobustnessSendRawCommand(pipe, &request, sizeof(request), &ack) == TRUE) {
        if ((ack.header.request_id == request_id) &&
            (ack.ack_status == vdrACK_STATUS_OK) &&
            (ack.timer_id != TIMER_ID_INVALID)) {
            *out_timer_id = ack.timer_id;
            created = TRUE;
        }
    }

    return created;
} /* end of RobustnessRawCreateTimer() */

/*
 * Start one timer through a raw pipe.
 */
static BOOL RobustnessRawStartTimer(
    HANDLE pipe,
    uint32_t request_id,
    TimerId_type timer_id,
    uint64_t duration_ms)
{
    vdrStartTimerReq_type request;
    vdrAckRsp_type ack;
    BOOL started = FALSE;

    assert(timer_id != TIMER_ID_INVALID);
    assert(duration_ms > 0);

    if ((timer_id == TIMER_ID_INVALID) || (duration_ms == 0)) {
        return FALSE;
    }

    memset(&request, 0, sizeof(request));
    RobustnessFillHeader(
        &request.header,
        vdrMESSAGE_TYPE_START_TIMER_REQ,
        request_id,
        sizeof(request));
    request.timer_id = timer_id;
    request.duration_ms = duration_ms;
    request.api_start_timestamp_ms = RobustnessGetTimeMs();

    if (RobustnessSendRawCommand(pipe, &request, sizeof(request), &ack) == TRUE) {
        if ((ack.header.request_id == request_id) &&
            (ack.ack_status == vdrACK_STATUS_OK) &&
            (ack.timer_id == timer_id)) {
            started = TRUE;
        }
    }

    return started;
} /* end of RobustnessRawStartTimer() */

/*
 * Start one timer through a raw pipe with a caller-provided API timestamp.
 *
 * This is used to simulate Windows scheduling delay without actually blocking
 * the server thread for a long period of time.
 */
static BOOL RobustnessRawStartTimerWithTimestamp(
    HANDLE pipe,
    uint32_t request_id,
    TimerId_type timer_id,
    uint64_t duration_ms,
    uint64_t api_start_timestamp_ms)
{
    vdrStartTimerReq_type request;
    vdrAckRsp_type ack;
    BOOL started = FALSE;

    assert(timer_id != TIMER_ID_INVALID);
    assert(duration_ms > 0);

    if ((timer_id == TIMER_ID_INVALID) || (duration_ms == 0)) {
        return FALSE;
    }

    memset(&request, 0, sizeof(request));
    RobustnessFillHeader(
        &request.header,
        vdrMESSAGE_TYPE_START_TIMER_REQ,
        request_id,
        sizeof(request));
    request.timer_id = timer_id;
    request.duration_ms = duration_ms;
    request.api_start_timestamp_ms = api_start_timestamp_ms;

    if (RobustnessSendRawCommand(pipe, &request, sizeof(request), &ack) == TRUE) {
        if ((ack.header.request_id == request_id) &&
            (ack.ack_status == vdrACK_STATUS_OK) &&
            (ack.timer_id == timer_id)) {
            started = TRUE;
        }
    }

    return started;
} /* end of RobustnessRawStartTimerWithTimestamp() */

/*
 * Read one asynchronous TimerExpired event from a raw pipe.
 */
static BOOL RobustnessReadExpiredEvent(
    HANDLE pipe,
    TimerId_type expected_timer_id)
{
    vdrTimerExpiredEvt_type event_message;
    DWORD bytes_read = 0;
    BOOL read_event = FALSE;

    assert(pipe != NULL);
    assert(pipe != INVALID_HANDLE_VALUE);
    assert(expected_timer_id != TIMER_ID_INVALID);

    if ((pipe == NULL) ||
        (pipe == INVALID_HANDLE_VALUE) ||
        (expected_timer_id == TIMER_ID_INVALID)) {
        return FALSE;
    }

    memset(&event_message, 0, sizeof(event_message));

    if (RobustnessWaitForPipeBytes(
        pipe,
        ROBUSTNESS_PIPE_TIMEOUT_MS,
        sizeof(vdrTimerExpiredEvt_type)) == TRUE) {
        if (ReadFile(pipe, &event_message, sizeof(event_message), &bytes_read, NULL) == TRUE) {
            if ((bytes_read == sizeof(event_message)) &&
                (event_message.header.magic == TIMER_PIPE_MAGIC) &&
                (event_message.header.version == TIMER_PROTOCOL_VERSION) &&
                (event_message.header.message_type == vdrMESSAGE_TYPE_TIMER_EXPIRED_EVT) &&
                (event_message.header.request_id == 0) &&
                (event_message.header.payload_size == vdrPayloadSize(sizeof(event_message))) &&
                (event_message.timer_id == expected_timer_id)) {
                read_event = TRUE;
            }
        }
    }

    return read_event;
} /* end of RobustnessReadExpiredEvent() */

/*
 * Stop one timer through a raw pipe.
 */
static BOOL RobustnessRawStopTimer(
    HANDLE pipe,
    uint32_t request_id,
    TimerId_type timer_id)
{
    vdrStopTimerReq_type request;
    vdrAckRsp_type ack;
    BOOL stopped = FALSE;

    assert(timer_id != TIMER_ID_INVALID);

    if (timer_id == TIMER_ID_INVALID) {
        return FALSE;
    }

    memset(&request, 0, sizeof(request));
    RobustnessFillHeader(
        &request.header,
        vdrMESSAGE_TYPE_STOP_TIMER_REQ,
        request_id,
        sizeof(request));
    request.timer_id = timer_id;

    if (RobustnessSendRawCommand(pipe, &request, sizeof(request), &ack) == TRUE) {
        if ((ack.header.request_id == request_id) &&
            (ack.ack_status == vdrACK_STATUS_OK) &&
            (ack.timer_id == timer_id)) {
            stopped = TRUE;
        }
    }

    return stopped;
} /* end of RobustnessRawStopTimer() */

/*
 * Delete one timer through a raw pipe.
 */
static BOOL RobustnessRawDeleteTimer(
    HANDLE pipe,
    uint32_t request_id,
    TimerId_type timer_id)
{
    vdrDeleteTimerReq_type request;
    vdrAckRsp_type ack;
    BOOL deleted = FALSE;

    assert(timer_id != TIMER_ID_INVALID);

    if (timer_id == TIMER_ID_INVALID) {
        return FALSE;
    }

    memset(&request, 0, sizeof(request));
    RobustnessFillHeader(
        &request.header,
        vdrMESSAGE_TYPE_DELETE_TIMER_REQ,
        request_id,
        sizeof(request));
    request.timer_id = timer_id;

    if (RobustnessSendRawCommand(pipe, &request, sizeof(request), &ack) == TRUE) {
        if ((ack.header.request_id == request_id) &&
            (ack.ack_status == vdrACK_STATUS_OK) &&
            (ack.timer_id == timer_id)) {
            deleted = TRUE;
        }
    }

    return deleted;
} /* end of RobustnessRawDeleteTimer() */

/*
 * Confirm that one malformed request is rejected and the pipe stays usable.
 */
static int RobustnessRunMalformedProtocolCase(vdrMalformedProtocolCase_type malformed_case)
{
    HANDLE pipe = RobustnessOpenRawPipe(ROBUSTNESS_PIPE_TIMEOUT_MS);
    vdrClientToServerMessage_type request;
    vdrAckRsp_type ack;
    TimerId_type timer_id = TIMER_ID_INVALID;
    DWORD command_size = sizeof(vdrCreateTimerReq_type);
    uint32_t request_id = 100 + (uint32_t)malformed_case;

    CHECK(pipe != INVALID_HANDLE_VALUE);

    memset(&request, 0, sizeof(request));
    RobustnessFillHeader(
        &request.header,
        vdrMESSAGE_TYPE_CREATE_TIMER_REQ,
        request_id,
        sizeof(vdrCreateTimerReq_type));

    switch (malformed_case) {
        case vdrMALFORMED_PROTOCOL_MAGIC:
            request.header.magic ^= 1;
            break;

        case vdrMALFORMED_PROTOCOL_VERSION:
            request.header.version++;
            break;

        case vdrMALFORMED_PROTOCOL_TYPE:
            request.header.message_type = UINT16_MAX;
            break;

        case vdrMALFORMED_PROTOCOL_TOTAL_SIZE:
            request.header.message_type = vdrMESSAGE_TYPE_START_TIMER_REQ;
            request.header.payload_size = vdrPayloadSize(sizeof(vdrStartTimerReq_type));
            command_size = sizeof(vdrMessageHeader_type);
            break;

        case vdrMALFORMED_PROTOCOL_PAYLOAD_SIZE:
            request.header.message_type = vdrMESSAGE_TYPE_START_TIMER_REQ;
            request.header.payload_size = vdrPayloadSize(sizeof(vdrStartTimerReq_type)) + 1;
            command_size = sizeof(vdrStartTimerReq_type);
            break;

        case vdrMALFORMED_PROTOCOL_TRUNCATED_HEADER:
            request.header.request_id = request_id;
            command_size = sizeof(vdrMessageHeader_type) - 1;
            break;

        case vdrMALFORMED_PROTOCOL_OVERSIZED_MESSAGE:
            command_size = sizeof(request);
            break;

        case vdrMALFORMED_PROTOCOL_PAYLOAD_TOO_SMALL:
            request.header.message_type = vdrMESSAGE_TYPE_START_TIMER_REQ;
            request.header.payload_size = vdrPayloadSize(sizeof(vdrStartTimerReq_type)) - 1;
            command_size = sizeof(vdrStartTimerReq_type);
            break;

        case vdrMALFORMED_PROTOCOL_PAYLOAD_TOO_LARGE:
            request.header.message_type = vdrMESSAGE_TYPE_START_TIMER_REQ;
            request.header.payload_size = vdrPayloadSize(sizeof(vdrStartTimerReq_type)) + 8;
            command_size = sizeof(vdrStartTimerReq_type);
            break;

        case vdrMALFORMED_PROTOCOL_OLD_START_TIMER_SIZE:
            request.header.message_type = vdrMESSAGE_TYPE_START_TIMER_REQ;
            request.header.payload_size = vdrPayloadSize(sizeof(vdrLegacyStartTimerReq_type));
            command_size = sizeof(vdrLegacyStartTimerReq_type);
            break;

        case vdrMALFORMED_PROTOCOL_ACK_SENT_TO_SERVER:
            request.header.message_type = vdrMESSAGE_TYPE_ACK_RSP;
            request.header.payload_size = vdrPayloadSize(sizeof(vdrAckRsp_type));
            command_size = sizeof(vdrAckRsp_type);
            break;

        case vdrMALFORMED_PROTOCOL_EXPIRY_SENT_TO_SERVER:
            request.header.message_type = vdrMESSAGE_TYPE_TIMER_EXPIRED_EVT;
            request.header.payload_size = vdrPayloadSize(sizeof(vdrTimerExpiredEvt_type));
            command_size = sizeof(vdrTimerExpiredEvt_type);
            break;

        default:
            CloseHandle(pipe);
            return 1;
    }

    CHECK(RobustnessSendRawCommand(pipe, &request, command_size, &ack) == TRUE);
    CHECK(ack.header.request_id == request_id);
    CHECK(ack.ack_status == vdrACK_STATUS_PROTOCOL_ERROR);
    CHECK(ack.timer_id == TIMER_ID_INVALID);

    /* A malformed message must not poison the connection or its next read. */
    CHECK(RobustnessRawCreateTimer(pipe, request_id + 10, &timer_id) == TRUE);
    CHECK(RobustnessRawDeleteTimer(pipe, request_id + 11, timer_id) == TRUE);

    CloseHandle(pipe);
    return 0;
} /* end of RobustnessRunMalformedProtocolCase() */

static int TestMalformedMagicRejected(void)
{
    return RobustnessRunMalformedProtocolCase(vdrMALFORMED_PROTOCOL_MAGIC);
} /* end of TestMalformedMagicRejected() */

static int TestMalformedVersionRejected(void)
{
    return RobustnessRunMalformedProtocolCase(vdrMALFORMED_PROTOCOL_VERSION);
} /* end of TestMalformedVersionRejected() */

static int TestMalformedTypeRejected(void)
{
    return RobustnessRunMalformedProtocolCase(vdrMALFORMED_PROTOCOL_TYPE);
} /* end of TestMalformedTypeRejected() */

static int TestMalformedTotalSizeRejected(void)
{
    return RobustnessRunMalformedProtocolCase(vdrMALFORMED_PROTOCOL_TOTAL_SIZE);
} /* end of TestMalformedTotalSizeRejected() */

static int TestMalformedPayloadSizeRejected(void)
{
    return RobustnessRunMalformedProtocolCase(vdrMALFORMED_PROTOCOL_PAYLOAD_SIZE);
} /* end of TestMalformedPayloadSizeRejected() */

static int TestMalformedTruncatedHeaderRejected(void)
{
    return RobustnessRunMalformedProtocolCase(vdrMALFORMED_PROTOCOL_TRUNCATED_HEADER);
} /* end of TestMalformedTruncatedHeaderRejected() */

static int TestMalformedOversizedMessageRejected(void)
{
    return RobustnessRunMalformedProtocolCase(vdrMALFORMED_PROTOCOL_OVERSIZED_MESSAGE);
} /* end of TestMalformedOversizedMessageRejected() */

static int TestMalformedPayloadTooSmallRejected(void)
{
    return RobustnessRunMalformedProtocolCase(vdrMALFORMED_PROTOCOL_PAYLOAD_TOO_SMALL);
} /* end of TestMalformedPayloadTooSmallRejected() */

static int TestMalformedPayloadTooLargeRejected(void)
{
    return RobustnessRunMalformedProtocolCase(vdrMALFORMED_PROTOCOL_PAYLOAD_TOO_LARGE);
} /* end of TestMalformedPayloadTooLargeRejected() */

static int TestMalformedOldStartTimerSizeRejected(void)
{
    return RobustnessRunMalformedProtocolCase(vdrMALFORMED_PROTOCOL_OLD_START_TIMER_SIZE);
} /* end of TestMalformedOldStartTimerSizeRejected() */

static int TestMalformedAckSentToServerRejected(void)
{
    return RobustnessRunMalformedProtocolCase(vdrMALFORMED_PROTOCOL_ACK_SENT_TO_SERVER);
} /* end of TestMalformedAckSentToServerRejected() */

static int TestMalformedExpirySentToServerRejected(void)
{
    return RobustnessRunMalformedProtocolCase(vdrMALFORMED_PROTOCOL_EXPIRY_SENT_TO_SERVER);
} /* end of TestMalformedExpirySentToServerRejected() */

/*
 * Wait for the server to disconnect a client admitted beyond the fixed limit.
 */
static BOOL RobustnessWaitForDisconnect(HANDLE pipe, DWORD timeout_ms)
{
    BOOL disconnected = FALSE;
    BOOL keep_waiting = TRUE;
    uint64_t deadline_ms = 0;

    assert(pipe != NULL);
    assert(pipe != INVALID_HANDLE_VALUE);
    assert(timeout_ms > 0);

    if ((pipe == NULL) || (pipe == INVALID_HANDLE_VALUE) || (timeout_ms == 0)) {
        return FALSE;
    }

    deadline_ms = (uint64_t)GetTickCount64() + timeout_ms;

    while (keep_waiting == TRUE) {
        DWORD available_bytes = 0;

        if (PeekNamedPipe(pipe, NULL, 0, NULL, &available_bytes, NULL) == FALSE) {
            disconnected = TRUE;
            keep_waiting = FALSE;
        } else if ((uint64_t)GetTickCount64() >= deadline_ms) {
            keep_waiting = FALSE;
        } else {
            Sleep(10);
        }
    }

    return disconnected;
} /* end of RobustnessWaitForDisconnect() */

/*
 * Fill all 61 client slots, exercise a 64-handle server wait, and prove that an
 * additional connection cannot become a command-capable client.
 */
static int TestMaximumClientAndWaitHandleCapacity(void)
{
    HANDLE clients[TIMER_SERVER_MAX_CLIENTS] = { NULL };
    HANDLE excess_client = INVALID_HANDLE_VALUE;
    TimerId_type long_timer_id = TIMER_ID_INVALID;
    TimerId_type probe_timer_id = TIMER_ID_INVALID;
    uint32_t client_index = 0;

    PRINTOUT_TRACE("[robustness] opening %u raw clients\n", TIMER_SERVER_MAX_CLIENTS);

    for (client_index = 0; client_index < TIMER_SERVER_MAX_CLIENTS; ++client_index) {
        clients[client_index] = RobustnessOpenRawPipe(ROBUSTNESS_PIPE_TIMEOUT_MS);
        CHECK(clients[client_index] != INVALID_HANDLE_VALUE);
    }

    /* An active timer adds the waitable timer to 61 client events, one listener,
     * and shutdown, exercising the full 64-handle WaitForMultipleObjects set. */
    CHECK(RobustnessRawCreateTimer(clients[0], 1000, &long_timer_id) == TRUE);
    CHECK(RobustnessRawStartTimer(clients[0], 1001, long_timer_id, 60000) == TRUE);

    /* A second client must still receive ACKs while all 64 wait slots are used. */
    CHECK(RobustnessRawCreateTimer(clients[1], 1002, &probe_timer_id) == TRUE);
    CHECK(RobustnessRawDeleteTimer(clients[1], 1003, probe_timer_id) == TRUE);

    excess_client = RobustnessOpenRawPipe(ROBUSTNESS_PIPE_TIMEOUT_MS);

    if (excess_client == INVALID_HANDLE_VALUE) {
        PRINTOUT_TRACE("[robustness] excess client rejected during connection\n");
    } else {
        CHECK(RobustnessWaitForDisconnect(
            excess_client,
            ROBUSTNESS_CAPACITY_REJECT_TIMEOUT_MS) == TRUE);
        CloseHandle(excess_client);
    }

    /* Existing clients remain usable after the rejected connection. */
    CHECK(RobustnessRawCreateTimer(clients[1], 1004, &probe_timer_id) == TRUE);
    CHECK(RobustnessRawDeleteTimer(clients[1], 1005, probe_timer_id) == TRUE);
    CHECK(RobustnessRawStopTimer(clients[0], 1006, long_timer_id) == TRUE);
    CHECK(RobustnessRawDeleteTimer(clients[0], 1007, long_timer_id) == TRUE);

    for (client_index = 0; client_index < TIMER_SERVER_MAX_CLIENTS; ++client_index) {
        CloseHandle(clients[client_index]);
    }

    return 0;
} /* end of TestMaximumClientAndWaitHandleCapacity() */

static int TestMaximumClientCapacityRecoversAfterDisconnect(void)
{
    HANDLE clients[TIMER_SERVER_MAX_CLIENTS] = { NULL };
    HANDLE replacement_client = INVALID_HANDLE_VALUE;
    TimerId_type probe_timer_id = TIMER_ID_INVALID;
    uint32_t client_index = 0;

    PRINTOUT_TRACE("[robustness] opening %u raw clients for capacity recovery\n", TIMER_SERVER_MAX_CLIENTS);

    for (client_index = 0; client_index < TIMER_SERVER_MAX_CLIENTS; ++client_index) {
        clients[client_index] = RobustnessOpenRawPipe(ROBUSTNESS_PIPE_TIMEOUT_MS);
        CHECK(clients[client_index] != INVALID_HANDLE_VALUE);
    }

    /* Release one admitted client and give the server loop time to process the
     * disconnect cleanup before attempting a replacement connection. */
    CloseHandle(clients[TIMER_SERVER_MAX_CLIENTS - 1]);
    clients[TIMER_SERVER_MAX_CLIENTS - 1] = INVALID_HANDLE_VALUE;
    Sleep(100);

    replacement_client = RobustnessOpenRawPipe(ROBUSTNESS_PIPE_TIMEOUT_MS);
    CHECK(replacement_client != INVALID_HANDLE_VALUE);
    CHECK(RobustnessRawCreateTimer(replacement_client, 2000, &probe_timer_id) == TRUE);
    CHECK(RobustnessRawDeleteTimer(replacement_client, 2001, probe_timer_id) == TRUE);
    CloseHandle(replacement_client);

    for (client_index = 0; client_index < TIMER_SERVER_MAX_CLIENTS; ++client_index) {
        if (clients[client_index] != INVALID_HANDLE_VALUE) {
            CloseHandle(clients[client_index]);
        }
    }

    return 0;
} /* end of TestMaximumClientCapacityRecoversAfterDisconnect() */

static int TestDisconnectCleanupRemovesClientTimers(void)
{
    HANDLE owner_client = RobustnessOpenRawPipe(ROBUSTNESS_PIPE_TIMEOUT_MS);
    HANDLE surviving_client = RobustnessOpenRawPipe(ROBUSTNESS_PIPE_TIMEOUT_MS);
    TimerId_type owner_active_timer = TIMER_ID_INVALID;
    TimerId_type owner_inactive_timer = TIMER_ID_INVALID;
    TimerId_type surviving_timer = TIMER_ID_INVALID;
    vdrDeleteTimerReq_type delete_request;
    vdrAckRsp_type ack;

    CHECK(owner_client != INVALID_HANDLE_VALUE);
    CHECK(surviving_client != INVALID_HANDLE_VALUE);
    CHECK(RobustnessRawCreateTimer(owner_client, 2100, &owner_active_timer) == TRUE);
    CHECK(RobustnessRawCreateTimer(owner_client, 2101, &owner_inactive_timer) == TRUE);
    CHECK(RobustnessRawCreateTimer(surviving_client, 2102, &surviving_timer) == TRUE);
    CHECK(RobustnessRawStartTimer(owner_client, 2103, owner_active_timer, 60000) == TRUE);
    CHECK(RobustnessRawStartTimer(surviving_client, 2104, surviving_timer, 60000) == TRUE);

    /* Closing the pipe simulates an application process disappearing. The
     * server must delete both active and inactive timers owned by that pipe. */
    CloseHandle(owner_client);
    owner_client = INVALID_HANDLE_VALUE;
    Sleep(150);

    memset(&delete_request, 0, sizeof(delete_request));
    RobustnessFillHeader(
        &delete_request.header,
        vdrMESSAGE_TYPE_DELETE_TIMER_REQ,
        2105,
        sizeof(delete_request));
    delete_request.timer_id = owner_inactive_timer;

    CHECK(RobustnessSendRawCommand(
        surviving_client,
        &delete_request,
        sizeof(delete_request),
        &ack) == TRUE);
    CHECK(ack.header.request_id == 2105);
    CHECK(ack.ack_status == vdrACK_STATUS_INVALID_TIMER_ID);
    CHECK(ack.timer_id == owner_inactive_timer);

    CHECK(RobustnessRawStopTimer(surviving_client, 2106, surviving_timer) == TRUE);
    CHECK(RobustnessRawDeleteTimer(surviving_client, 2107, surviving_timer) == TRUE);
    CloseHandle(surviving_client);

    return 0;
} /* end of TestDisconnectCleanupRemovesClientTimers() */

static int TestNegativeAckOwnershipAndRawDuration(void)
{
    HANDLE owner_client = RobustnessOpenRawPipe(ROBUSTNESS_PIPE_TIMEOUT_MS);
    HANDLE other_client = RobustnessOpenRawPipe(ROBUSTNESS_PIPE_TIMEOUT_MS);
    TimerId_type owner_timer = TIMER_ID_INVALID;
    vdrStartTimerReq_type start_request;
    vdrDeleteTimerReq_type delete_request;
    vdrAckRsp_type ack;

    CHECK(owner_client != INVALID_HANDLE_VALUE);
    CHECK(other_client != INVALID_HANDLE_VALUE);
    CHECK(RobustnessRawCreateTimer(owner_client, 2200, &owner_timer) == TRUE);

    memset(&start_request, 0, sizeof(start_request));
    RobustnessFillHeader(
        &start_request.header,
        vdrMESSAGE_TYPE_START_TIMER_REQ,
        2201,
        sizeof(start_request));
    start_request.timer_id = owner_timer;
    start_request.duration_ms = 0;
    CHECK(RobustnessSendRawCommand(owner_client, &start_request, sizeof(start_request), &ack) == TRUE);
    CHECK(ack.header.request_id == 2201);
    CHECK(ack.ack_status == vdrACK_STATUS_INVALID_DURATION);
    CHECK(ack.timer_id == owner_timer);

    start_request.header.request_id = 2202;
    start_request.duration_ms = 1000;
    start_request.api_start_timestamp_ms = RobustnessGetTimeMs();
    CHECK(RobustnessSendRawCommand(other_client, &start_request, sizeof(start_request), &ack) == TRUE);
    CHECK(ack.header.request_id == 2202);
    CHECK(ack.ack_status == vdrACK_STATUS_TIMER_NOT_OWNED);
    CHECK(ack.timer_id == owner_timer);

    memset(&delete_request, 0, sizeof(delete_request));
    RobustnessFillHeader(
        &delete_request.header,
        vdrMESSAGE_TYPE_DELETE_TIMER_REQ,
        2203,
        sizeof(delete_request));
    delete_request.timer_id = owner_timer + 100000;
    CHECK(RobustnessSendRawCommand(owner_client, &delete_request, sizeof(delete_request), &ack) == TRUE);
    CHECK(ack.header.request_id == 2203);
    CHECK(ack.ack_status == vdrACK_STATUS_INVALID_TIMER_ID);
    CHECK(ack.timer_id == delete_request.timer_id);

    CHECK(RobustnessRawDeleteTimer(owner_client, 2204, owner_timer) == TRUE);
    CloseHandle(owner_client);
    CloseHandle(other_client);

    return 0;
} /* end of TestNegativeAckOwnershipAndRawDuration() */

static int TestStartTimerAlreadyExpiredTimestampDeliversImmediateExpiry(void)
{
    HANDLE client = RobustnessOpenRawPipe(ROBUSTNESS_PIPE_TIMEOUT_MS);
    TimerId_type timer_id = TIMER_ID_INVALID;
    const uint64_t now_ms = RobustnessGetTimeMs();

    PRINTOUT_TRACE("[robustness] immediate expiry timestamp test now_ms=%llu\n",
        (unsigned long long)now_ms);

    CHECK(client != INVALID_HANDLE_VALUE);
    CHECK(RobustnessRawCreateTimer(client, 2300, &timer_id) == TRUE);

    /* Simulate the API having accepted StartTimer long enough ago that the
     * requested duration is already consumed before the server dispatches it. */
    CHECK(RobustnessRawStartTimerWithTimestamp(
        client,
        2301,
        timer_id,
        10,
        (now_ms > 1000) ? (now_ms - 1000) : 0) == TRUE);
    CHECK(RobustnessReadExpiredEvent(client, timer_id) == TRUE);
    CHECK(RobustnessRawDeleteTimer(client, 2302, timer_id) == TRUE);
    CloseHandle(client);

    return 0;
} /* end of TestStartTimerAlreadyExpiredTimestampDeliversImmediateExpiry() */

static int TestRestartActiveTimerAlreadyExpiredTimestampClearsOldSchedule(void)
{
    HANDLE client = RobustnessOpenRawPipe(ROBUSTNESS_PIPE_TIMEOUT_MS);
    TimerId_type timer_id = TIMER_ID_INVALID;
    const uint64_t now_ms = RobustnessGetTimeMs();

    PRINTOUT_TRACE("[robustness] active restart immediate expiry timestamp test now_ms=%llu\n",
        (unsigned long long)now_ms);

    CHECK(client != INVALID_HANDLE_VALUE);
    CHECK(RobustnessRawCreateTimer(client, 2400, &timer_id) == TRUE);
    CHECK(RobustnessRawStartTimer(client, 2401, timer_id, 60000) == TRUE);

    /* Restarting an active timer with an already-expired API timestamp must
     * remove the old active schedule, ACK the command, and deliver exactly one
     * immediate expiry indication for the restarted timer. */
    CHECK(RobustnessRawStartTimerWithTimestamp(
        client,
        2402,
        timer_id,
        10,
        (now_ms > 1000) ? (now_ms - 1000) : 0) == TRUE);
    CHECK(RobustnessReadExpiredEvent(client, timer_id) == TRUE);
    CHECK(RobustnessRawDeleteTimer(client, 2403, timer_id) == TRUE);
    CloseHandle(client);

    return 0;
} /* end of TestRestartActiveTimerAlreadyExpiredTimestampClearsOldSchedule() */

/*
 * Execute one complete timer lifecycle from a concurrently released worker.
 */
static DWORD WINAPI RobustnessConcurrentWorker(void* parameter)
{
    vdrConcurrentWorkerContext_type* context =
        (vdrConcurrentWorkerContext_type*)parameter;

    assert(context != NULL);

    if (context == NULL) {
        return 1;
    }

    context->status = vdrACK_STATUS_INTERNAL_ERROR;

    if (WaitForSingleObject(context->start_event, ROBUSTNESS_CONCURRENT_WAIT_MS) == WAIT_OBJECT_0) {
        context->status = vdrTimerManager_CreateTimer(&context->timer_id);

        if (context->status == vdrACK_STATUS_OK) {
            context->status = vdrTimerManager_StartTimer(context->timer_id, 60000);

            if (context->status == vdrACK_STATUS_OK) {
                context->status = vdrTimerManager_StopTimer(context->timer_id);
            }

            if (context->status == vdrACK_STATUS_OK) {
                context->status = vdrTimerManager_DeleteTimer(context->timer_id);
            } else {
                (void)vdrTimerManager_DeleteTimer(context->timer_id);
            }
        }
    }

    return (context->status == vdrACK_STATUS_OK) ? 0 : 1;
} /* end of RobustnessConcurrentWorker() */

/*
 * Release eight threads into their first public CreateTimer call together.
 *
 * This covers both lazy-initialization serialization and command serialization
 * over the one process-wide pipe and ACK handoff.
 */
static int TestConcurrentPublicApiCalls(void)
{
    HANDLE start_event = CreateEventW(NULL, TRUE, FALSE, NULL);
    HANDLE threads[ROBUSTNESS_CONCURRENT_THREAD_COUNT] = { NULL };
    vdrConcurrentWorkerContext_type contexts[ROBUSTNESS_CONCURRENT_THREAD_COUNT] = {0};
    uint32_t thread_index = 0;
    uint32_t compare_index = 0;

    CHECK(start_event != NULL);

    for (thread_index = 0; thread_index < ROBUSTNESS_CONCURRENT_THREAD_COUNT; ++thread_index) {
        contexts[thread_index].start_event = start_event;
        contexts[thread_index].timer_id = TIMER_ID_INVALID;
        contexts[thread_index].status = vdrACK_STATUS_INTERNAL_ERROR;
        threads[thread_index] = CreateThread(
            NULL,
            0,
            RobustnessConcurrentWorker,
            &contexts[thread_index],
            0,
            NULL);
        CHECK(threads[thread_index] != NULL);
    }

    /* Release every worker so the first API calls race into lazy initialization. */
    CHECK(SetEvent(start_event) == TRUE);
    CHECK(WaitForMultipleObjects(
        ROBUSTNESS_CONCURRENT_THREAD_COUNT,
        threads,
        TRUE,
        ROBUSTNESS_CONCURRENT_WAIT_MS) == WAIT_OBJECT_0);

    for (thread_index = 0; thread_index < ROBUSTNESS_CONCURRENT_THREAD_COUNT; ++thread_index) {
        DWORD thread_exit_code = 1;

        CHECK(GetExitCodeThread(threads[thread_index], &thread_exit_code) == TRUE);
        CHECK(thread_exit_code == 0);
        CHECK(contexts[thread_index].status == vdrACK_STATUS_OK);
        CHECK(contexts[thread_index].timer_id != TIMER_ID_INVALID);

        for (compare_index = thread_index + 1;
             compare_index < ROBUSTNESS_CONCURRENT_THREAD_COUNT;
             ++compare_index) {
            CHECK(contexts[thread_index].timer_id != contexts[compare_index].timer_id);
        }

        CloseHandle(threads[thread_index]);
    }

    CloseHandle(start_event);
    return 0;
} /* end of TestConcurrentPublicApiCalls() */

static const RobustnessTestCase_type g_robustness_tests[] = {
    { "MalformedMagicRejected", TestMalformedMagicRejected },
    { "MalformedVersionRejected", TestMalformedVersionRejected },
    { "MalformedTypeRejected", TestMalformedTypeRejected },
    { "MalformedTotalSizeRejected", TestMalformedTotalSizeRejected },
    { "MalformedPayloadSizeRejected", TestMalformedPayloadSizeRejected },
    { "MalformedTruncatedHeaderRejected", TestMalformedTruncatedHeaderRejected },
    { "MalformedOversizedMessageRejected", TestMalformedOversizedMessageRejected },
    { "MalformedPayloadTooSmallRejected", TestMalformedPayloadTooSmallRejected },
    { "MalformedPayloadTooLargeRejected", TestMalformedPayloadTooLargeRejected },
    { "MalformedOldStartTimerSizeRejected", TestMalformedOldStartTimerSizeRejected },
    { "MalformedAckSentToServerRejected", TestMalformedAckSentToServerRejected },
    { "MalformedExpirySentToServerRejected", TestMalformedExpirySentToServerRejected },
    { "MaximumClientAndWaitHandleCapacity", TestMaximumClientAndWaitHandleCapacity },
    { "MaximumClientCapacityRecoversAfterDisconnect", TestMaximumClientCapacityRecoversAfterDisconnect },
    { "DisconnectCleanupRemovesClientTimers", TestDisconnectCleanupRemovesClientTimers },
    { "NegativeAckOwnershipAndRawDuration", TestNegativeAckOwnershipAndRawDuration },
    { "StartTimerAlreadyExpiredTimestampDeliversImmediateExpiry", TestStartTimerAlreadyExpiredTimestampDeliversImmediateExpiry },
    { "RestartActiveTimerAlreadyExpiredTimestampClearsOldSchedule", TestRestartActiveTimerAlreadyExpiredTimestampClearsOldSchedule },
    { "ConcurrentPublicApiCalls", TestConcurrentPublicApiCalls }
};

static int RunOneRobustnessTest(const char* name)
{
    size_t test_index = 0;

    assert(name != NULL);

    if (name == NULL) {
        return 1;
    }

    for (test_index = 0;
         test_index < (sizeof(g_robustness_tests) / sizeof(g_robustness_tests[0]));
         ++test_index) {
        if (strcmp(name, g_robustness_tests[test_index].name) == 0) {
            int result = g_robustness_tests[test_index].function();

            printf("\n\n");
            fflush(stdout);
            return result;
        }
    }

    PRINTOUT_TRACE("[robustness] unknown scenario: %s\n", name);
    return 1;
} /* end of RunOneRobustnessTest() */

int main(int argc, char** argv)
{
    size_t test_index = 0;
    int result = 0;

    if (argc == 2) {
        result = RunOneRobustnessTest(argv[1]);
    } else {
        for (test_index = 0;
             test_index < (sizeof(g_robustness_tests) / sizeof(g_robustness_tests[0]));
             ++test_index) {
            if (RunOneRobustnessTest(g_robustness_tests[test_index].name) == 0) {
                result = 0;
            } else {
                result = 1;
                break;
            }
        }
    }

    return result;
} /* end of main() */
