/*
 * File:        sample_client.c
 * Module:      Timer Manager Connection State Machine Sample
 * Author:      Vitaly Drapkin
 * Copyright:   Copyright (c) 2026 Vitaly Drapkin
 * Purpose:     Demonstrate timer-driven connection-management state transitions.
 *
 * Design Overview:
 *   - Maintains IDLE, WAITING_FOR_CONNECTION_ESTABLISHMENT, and CONNECTED states.
 *   - Uses five server-managed one-shot timers to simulate connection messages,
 *     retransmission guarding, connection responses, and connection release.
 *   - Waits for timer notifications and Ctrl+C through WaitForMultipleObjects().
 *
 * Concurrency:
 *   The state machine runs on the main application thread. The client API owns
 *   a receiver thread that queues timer expiries and signals its shared event.
 *   The Windows console control handler only signals the shutdown event.
 *
 * Assumptions:
 *   - vdrapkinTimerManager is running before this application starts.
 *   - Timer durations are expressed in milliseconds by the public API.
 *   - CONNECTION_* messages are simulated by trace output and carry no payload.
 *
 * Revision History:
 *   - 2026-07-01: Wrapped shared sample state behind an accessor.
 *   - 2026-06-30: Added simultaneous/stale response expiry self-test.
 *   - 2026-06-30: Replaced the two-timer demo with a connection state machine.
 */
#include <windows.h>
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "vdr_timer_manager_api.h"

#define SAMPLE_WAIT_HANDLE_COUNT 2
#define SAMPLE_NOTIFICATION_EVENT_INDEX 0
#define SAMPLE_SHUTDOWN_EVENT_INDEX 1

#define SAMPLE_T1_MIN_SECONDS 3
#define SAMPLE_T1_MAX_SECONDS 10
#define SAMPLE_T2_MIN_SECONDS 3
#define SAMPLE_T2_MAX_SECONDS 5
#define SAMPLE_RESPONSE_MIN_SECONDS 1
#define SAMPLE_RESPONSE_MAX_SECONDS 24
#define SAMPLE_T5_MIN_SECONDS 5
#define SAMPLE_T5_MAX_SECONDS 15
#define SAMPLE_MAX_T2_EXPIRIES 5
#define SAMPLE_DATA_EXCHANGE_INTERVAL_MS 1000
#define SAMPLE_SIMULTANEOUS_RESPONSE_TEST_DURATION_MS 100
#define SAMPLE_SIMULTANEOUS_RESPONSE_TEST_WAIT_MS 10000

/*
 * Prefix each trace with its source file and call-site line number, then flush
 * immediately so execution progress remains visible in a live console or log.
 */
#define PRINTOUT_TRACE(...) do { \
    printf("[%s:%d] ", __FILE__, __LINE__); \
    printf(__VA_ARGS__); \
    fflush(stdout); \
} while (0)

#define SAMPLE_MESSAGE_TRACE_COLOR \
    (FOREGROUND_RED | FOREGROUND_GREEN | FOREGROUND_INTENSITY)
#define SAMPLE_STATE_TRACE_COLOR \
    (FOREGROUND_GREEN | FOREGROUND_BLUE | FOREGROUND_INTENSITY)
#define SAMPLE_TIMER_EXPIRY_TRACE_COLOR \
    (FOREGROUND_RED | FOREGROUND_BLUE | FOREGROUND_INTENSITY)

/*
 * Temporarily select a bright console color for one application-logic trace.
 *
 * The original attributes are restored immediately after the trace. If stdout
 * is redirected or console information is unavailable, output remains plain.
 */
#define SAMPLE_COLORED_TRACE(trace_color, ...) do { \
    HANDLE sample_console_output = GetStdHandle(STD_OUTPUT_HANDLE); \
    CONSOLE_SCREEN_BUFFER_INFO sample_original_console_info = {0}; \
    BOOL sample_restore_console_color = FALSE; \
    if ((sample_console_output == NULL) || \
        (sample_console_output == INVALID_HANDLE_VALUE)) { \
        sample_restore_console_color = FALSE; \
    } else if (GetConsoleScreenBufferInfo( \
        sample_console_output, \
        &sample_original_console_info) == TRUE) { \
        /* Select the category color before printing the complete trace line. */ \
        sample_restore_console_color = SetConsoleTextAttribute( \
            sample_console_output, \
            (WORD)(trace_color)); \
    } \
    PRINTOUT_TRACE(__VA_ARGS__); \
    if (sample_restore_console_color == TRUE) { \
        /* Restore prior attributes so unrelated diagnostics keep their color. */ \
        (void)SetConsoleTextAttribute( \
            sample_console_output, \
            sample_original_console_info.wAttributes); \
    } \
} while (0)

/* All simulated CONNECTION_* messages use one bright yellow color. */
#define SAMPLE_MESSAGE_TRACE(...) \
    SAMPLE_COLORED_TRACE(SAMPLE_MESSAGE_TRACE_COLOR, __VA_ARGS__)

/* Initial-state and state-transition lines use one bright cyan color. */
#define SAMPLE_STATE_TRACE(...) \
    SAMPLE_COLORED_TRACE(SAMPLE_STATE_TRACE_COLOR, __VA_ARGS__)

/* Timer-expiry events use bright magenta to identify state-machine triggers. */
#define SAMPLE_TIMER_EXPIRY_TRACE(...) \
    SAMPLE_COLORED_TRACE(SAMPLE_TIMER_EXPIRY_TRACE_COLOR, __VA_ARGS__)

typedef enum vdrSampleConnectionState_tag {
    vdrSAMPLE_STATE_IDLE = 0,
    vdrSAMPLE_STATE_WAITING_FOR_CONNECTION_ESTABLISHMENT,
    vdrSAMPLE_STATE_CONNECTED
} vdrSampleConnectionState_type;

typedef struct vdrSampleTimerSet_tag {
    /* T1 limits how long the state machine remains in IDLE. */
    TimerId_type t1_timer_id;

    /* T2 guards each CONNECTION_REQUEST retransmission interval. */
    TimerId_type t2_timer_id;

    /* T3 simulates reception of CONNECTION_ACCEPT. */
    TimerId_type t3_timer_id;

    /* T4 simulates reception of CONNECTION_REJECT. */
    TimerId_type t4_timer_id;

    /* T5 limits the connected data-exchange period. */
    TimerId_type t5_timer_id;
} vdrSampleTimerSet_type;

typedef struct vdrSampleStateMachine_tag {
    /* Current state controls which timer expiries are meaningful. */
    vdrSampleConnectionState_type state;

    /* Server timer IDs remain allocated for the life of the sample. */
    vdrSampleTimerSet_type timers;

    /* Counts T2 expiries for the current connection attempt. */
    uint32_t t2_expiry_count;

    /* Next GetTickCount64 deadline for the once-per-second data trace. */
    uint64_t next_data_exchange_ms;

    /* Number of T3/T4 expiries that performed a response state transition. */
    uint32_t response_transition_count;

    /* Number of queued T3/T4 expiries ignored after a response transition. */
    uint32_t stale_response_expiry_count;
} vdrSampleStateMachine_type;

typedef struct vdrSampleGlobalState_tag {
    /* Main publishes this handle for the console control handler, then clears
     * it after removing the handler during cleanup. */
    HANDLE shutdown_event;
} vdrSampleGlobalState_type;

/* The sample has one process-lifetime state object. Operational code reaches
 * it only through SampleGetGlobalState(). */
static vdrSampleGlobalState_type g_sample_global_state;

/*
 * Return the process-lifetime sample state shared with the console handler.
 */
static vdrSampleGlobalState_type* SampleGetGlobalState(void)
{
    return &g_sample_global_state;
} /* end of SampleGetGlobalState() */

/*
 * Signal application shutdown when the console receives a terminating event.
 *
 * The handler does not run state-machine or Timer Manager logic. It only wakes
 * the main thread, which performs orderly timer deletion and handle cleanup.
 */
static BOOL WINAPI SampleConsoleControlHandler(DWORD control_type)
{
    vdrSampleGlobalState_type* global_state = SampleGetGlobalState();
    BOOL handled = FALSE;

    if ((control_type == CTRL_C_EVENT) ||
        (control_type == CTRL_BREAK_EVENT) ||
        (control_type == CTRL_CLOSE_EVENT)) {
        if (global_state->shutdown_event == NULL) {
            handled = FALSE;
        } else {
            /* Signal the shutdown handle to release WaitForMultipleObjects() so
             * the main thread can leave its event loop and clean up safely. */
            handled = SetEvent(global_state->shutdown_event);
        }
    }

    return handled;
} /* end of SampleConsoleControlHandler() */

/*
 * Convert a state value to text used by state-transition trace messages.
 */
static const char* SampleStateName(vdrSampleConnectionState_type state)
{
    const char* state_name = "UNKNOWN";

    switch (state) {
        case vdrSAMPLE_STATE_IDLE:
            state_name = "IDLE";
            break;

        case vdrSAMPLE_STATE_WAITING_FOR_CONNECTION_ESTABLISHMENT:
            state_name = "WAITING_FOR_CONNECTION_ESTABLISHMENT";
            break;

        case vdrSAMPLE_STATE_CONNECTED:
            state_name = "CONNECTED";
            break;

        default:
            assert(FALSE);
            break;
    }

    return state_name;
} /* end of SampleStateName() */

/*
 * Return the symbolic T1 through T5 name associated with one allocated timer.
 */
static const char* SampleTimerName(
    const vdrSampleStateMachine_type* context,
    TimerId_type timer_id)
{
    const char* timer_name = "UNKNOWN";

    assert(context != NULL);
    assert(timer_id != TIMER_ID_INVALID);

    if ((context == NULL) || (timer_id == TIMER_ID_INVALID)) {
        timer_name = "UNKNOWN";
    } else if (timer_id == context->timers.t1_timer_id) {
        timer_name = "T1";
    } else if (timer_id == context->timers.t2_timer_id) {
        timer_name = "T2";
    } else if (timer_id == context->timers.t3_timer_id) {
        timer_name = "T3";
    } else if (timer_id == context->timers.t4_timer_id) {
        timer_name = "T4";
    } else if (timer_id == context->timers.t5_timer_id) {
        timer_name = "T5";
    }

    return timer_name;
} /* end of SampleTimerName() */

/*
 * Generate an inclusive random duration range and convert seconds to
 * milliseconds for vdrTimerManager_StartTimer().
 */
static uint64_t SampleRandomDurationMs(uint32_t minimum_seconds, uint32_t maximum_seconds)
{
    uint64_t duration_ms = 0;

    assert(minimum_seconds > 0);
    assert(maximum_seconds >= minimum_seconds);

    if ((minimum_seconds == 0) || (maximum_seconds < minimum_seconds)) {
        duration_ms = 0;
    } else {
        uint32_t duration_span = maximum_seconds - minimum_seconds + 1;
        uint32_t duration_seconds = minimum_seconds + (uint32_t)(rand() % duration_span);

        duration_ms = (uint64_t)duration_seconds * 1000;
    }

    return duration_ms;
} /* end of SampleRandomDurationMs() */

/*
 * Print and apply one state transition.
 *
 * Updating context->state changes how subsequent queued timer expiries are
 * interpreted by SampleProcessExpiredTimer().
 */
static void SampleTransitionState(
    vdrSampleStateMachine_type* context,
    vdrSampleConnectionState_type new_state)
{
    assert(context != NULL);

    if (context == NULL) {
        return;
    }

    SAMPLE_STATE_TRACE("[sample-client][state] %s -> %s\n",
        SampleStateName(context->state),
        SampleStateName(new_state));

    context->state = new_state;
} /* end of SampleTransitionState() */

/*
 * Create one server-managed timer and report the assigned timer ID.
 */
static vdrAckStatus_type SampleCreateTimer(const char* timer_name, TimerId_type* timer_id)
{
    vdrAckStatus_type status = vdrACK_STATUS_INTERNAL_ERROR;

    assert(timer_name != NULL);
    assert(timer_id != NULL);

    if ((timer_name == NULL) || (timer_id == NULL)) {
        status = vdrACK_STATUS_INTERNAL_ERROR;
    } else {
        PRINTOUT_TRACE("[sample-client][timer] creating %s\n", timer_name);
        status = vdrTimerManager_CreateTimer(timer_id);

        if (status == vdrACK_STATUS_OK) {
            PRINTOUT_TRACE("[sample-client][timer] created %s timer=%llu\n",
                timer_name,
                (unsigned long long)*timer_id);
        } else {
            PRINTOUT_TRACE("[sample-client][timer] failed to create %s status=%u\n",
                timer_name,
                (unsigned)status);
        }
    }

    return status;
} /* end of SampleCreateTimer() */

/*
 * Start one logical timer and expose its random duration in the trace.
 */
static vdrAckStatus_type SampleStartTimer(
    const char* timer_name,
    TimerId_type timer_id,
    uint64_t duration_ms)
{
    vdrAckStatus_type status = vdrACK_STATUS_INTERNAL_ERROR;

    assert(timer_name != NULL);
    assert(timer_id != TIMER_ID_INVALID);
    assert(duration_ms > 0);

    if ((timer_name == NULL) ||
        (timer_id == TIMER_ID_INVALID) ||
        (duration_ms == 0)) {
        status = vdrACK_STATUS_INTERNAL_ERROR;
    } else {
        PRINTOUT_TRACE("[sample-client][timer] starting %s timer=%llu duration_ms=%llu\n",
            timer_name,
            (unsigned long long)timer_id,
            (unsigned long long)duration_ms);
        status = vdrTimerManager_StartTimer(timer_id, duration_ms);

        if (status == vdrACK_STATUS_OK) {
            PRINTOUT_TRACE("[sample-client][timer] started %s timer=%llu\n",
                timer_name,
                (unsigned long long)timer_id);
        } else {
            PRINTOUT_TRACE("[sample-client][timer] failed to start %s timer=%llu status=%u\n",
                timer_name,
                (unsigned long long)timer_id,
                (unsigned)status);
        }
    }

    return status;
} /* end of SampleStartTimer() */

/*
 * Stop one timer that must no longer influence the current state.
 *
 * StopTimer is idempotent, so this helper also succeeds if the timer already
 * expired and was moved to the server's inactive list.
 */
static vdrAckStatus_type SampleStopTimer(const char* timer_name, TimerId_type timer_id)
{
    vdrAckStatus_type status = vdrACK_STATUS_INTERNAL_ERROR;

    assert(timer_name != NULL);
    assert(timer_id != TIMER_ID_INVALID);

    if ((timer_name == NULL) || (timer_id == TIMER_ID_INVALID)) {
        status = vdrACK_STATUS_INTERNAL_ERROR;
    } else {
        PRINTOUT_TRACE("[sample-client][timer] stopping %s timer=%llu\n",
            timer_name,
            (unsigned long long)timer_id);
        status = vdrTimerManager_StopTimer(timer_id);

        if (status == vdrACK_STATUS_OK) {
            PRINTOUT_TRACE("[sample-client][timer] stopped %s timer=%llu\n",
                timer_name,
                (unsigned long long)timer_id);
        } else {
            PRINTOUT_TRACE("[sample-client][timer] failed to stop %s timer=%llu status=%u\n",
                timer_name,
                (unsigned long long)timer_id,
                (unsigned)status);
        }
    }

    return status;
} /* end of SampleStopTimer() */

/*
 * Delete one timer during application cleanup.
 *
 * TIMER_ID_INVALID is accepted so partial initialization can use the same
 * cleanup path without attempting an invalid public API call.
 */
static BOOL SampleDeleteTimer(const char* timer_name, TimerId_type* timer_id)
{
    BOOL deleted = TRUE;

    assert(timer_name != NULL);
    assert(timer_id != NULL);

    if ((timer_name == NULL) || (timer_id == NULL)) {
        deleted = FALSE;
    } else if (*timer_id == TIMER_ID_INVALID) {
        deleted = TRUE;
    } else {
        PRINTOUT_TRACE("[sample-client][timer] deleting %s timer=%llu\n",
            timer_name,
            (unsigned long long)*timer_id);

        vdrAckStatus_type status = vdrTimerManager_DeleteTimer(*timer_id);

        if (status == vdrACK_STATUS_OK) {
            PRINTOUT_TRACE("[sample-client][timer] deleted %s timer=%llu\n",
                timer_name,
                (unsigned long long)*timer_id);
            *timer_id = TIMER_ID_INVALID;
        } else {
            PRINTOUT_TRACE("[sample-client][timer] failed to delete %s timer=%llu status=%u\n",
                timer_name,
                (unsigned long long)*timer_id,
                (unsigned)status);
            deleted = FALSE;
        }
    }

    return deleted;
} /* end of SampleDeleteTimer() */

/*
 * Create T1 through T5 in order.
 *
 * The nested success path ensures CreateTimer is the first Timer Manager API
 * call and prevents later creation attempts after an earlier failure.
 */
static BOOL SampleCreateAllTimers(vdrSampleStateMachine_type* context)
{
    BOOL created = FALSE;
    vdrAckStatus_type status = vdrACK_STATUS_INTERNAL_ERROR;

    assert(context != NULL);

    if (context == NULL) {
        created = FALSE;
    } else {
        status = SampleCreateTimer("T1", &context->timers.t1_timer_id);

        if (status == vdrACK_STATUS_OK) {
            status = SampleCreateTimer("T2", &context->timers.t2_timer_id);

            if (status == vdrACK_STATUS_OK) {
                status = SampleCreateTimer("T3", &context->timers.t3_timer_id);

                if (status == vdrACK_STATUS_OK) {
                    status = SampleCreateTimer("T4", &context->timers.t4_timer_id);

                    if (status == vdrACK_STATUS_OK) {
                        status = SampleCreateTimer("T5", &context->timers.t5_timer_id);

                        if (status == vdrACK_STATUS_OK) {
                            created = TRUE;
                        }
                    }
                }
            }
        }
    }

    return created;
} /* end of SampleCreateAllTimers() */

/*
 * Delete every timer that was successfully created.
 *
 * All deletion attempts are made even if one fails, which gives partial
 * initialization and communication-failure paths the best possible cleanup.
 */
static BOOL SampleDeleteAllTimers(vdrSampleStateMachine_type* context)
{
    BOOL deleted = TRUE;

    assert(context != NULL);

    if (context == NULL) {
        deleted = FALSE;
    } else {
        if (SampleDeleteTimer("T1", &context->timers.t1_timer_id) == FALSE) {
            deleted = FALSE;
        }

        if (SampleDeleteTimer("T2", &context->timers.t2_timer_id) == FALSE) {
            deleted = FALSE;
        }

        if (SampleDeleteTimer("T3", &context->timers.t3_timer_id) == FALSE) {
            deleted = FALSE;
        }

        if (SampleDeleteTimer("T4", &context->timers.t4_timer_id) == FALSE) {
            deleted = FALSE;
        }

        if (SampleDeleteTimer("T5", &context->timers.t5_timer_id) == FALSE) {
            deleted = FALSE;
        }
    }

    return deleted;
} /* end of SampleDeleteAllTimers() */

/*
 * Start T1 with a new random IDLE duration for the next connection cycle.
 */
static BOOL SampleStartIdleTimer(vdrSampleStateMachine_type* context)
{
    BOOL started = FALSE;

    assert(context != NULL);

    if (context == NULL) {
        started = FALSE;
    } else {
        uint64_t duration_ms = SampleRandomDurationMs(
            SAMPLE_T1_MIN_SECONDS,
            SAMPLE_T1_MAX_SECONDS);

        if (SampleStartTimer("T1", context->timers.t1_timer_id, duration_ms) == vdrACK_STATUS_OK) {
            started = TRUE;
        }
    }

    return started;
} /* end of SampleStartIdleTimer() */

/*
 * Start a fresh random T2 guard interval after sending CONNECTION_REQUEST.
 */
static BOOL SampleStartGuardTimer(vdrSampleStateMachine_type* context)
{
    BOOL started = FALSE;

    assert(context != NULL);

    if (context == NULL) {
        started = FALSE;
    } else {
        uint64_t duration_ms = SampleRandomDurationMs(
            SAMPLE_T2_MIN_SECONDS,
            SAMPLE_T2_MAX_SECONDS);

        if (SampleStartTimer("T2", context->timers.t2_timer_id, duration_ms) == vdrACK_STATUS_OK) {
            started = TRUE;
        }
    }

    return started;
} /* end of SampleStartGuardTimer() */

/*
 * Enter WAITING_FOR_CONNECTION_ESTABLISHMENT after T1 expiry.
 *
 * T3 and T4 use the same response range. Their durations are forced to differ,
 * so the shorter timer deterministically selects ACCEPT or REJECT unless five
 * T2 expiries end the attempt before either response arrives.
 */
static BOOL SampleBeginConnectionAttempt(vdrSampleStateMachine_type* context)
{
    BOOL started = FALSE;

    assert(context != NULL);

    if (context == NULL) {
        started = FALSE;
    } else {
        uint64_t t3_duration_ms = SampleRandomDurationMs(
            SAMPLE_RESPONSE_MIN_SECONDS,
            SAMPLE_RESPONSE_MAX_SECONDS);
        uint64_t t4_duration_ms = SampleRandomDurationMs(
            SAMPLE_RESPONSE_MIN_SECONDS,
            SAMPLE_RESPONSE_MAX_SECONDS);

        if (t3_duration_ms == t4_duration_ms) {
            uint64_t maximum_response_ms = (uint64_t)SAMPLE_RESPONSE_MAX_SECONDS * 1000;

            if (t4_duration_ms < maximum_response_ms) {
                t4_duration_ms += 1000;
            } else {
                t4_duration_ms -= 1000;
            }
        }

        context->t2_expiry_count = 0;

        SAMPLE_MESSAGE_TRACE("[sample-client][message][TX] CONNECTION_REQUEST attempt=1\n");
        SampleTransitionState(context, vdrSAMPLE_STATE_WAITING_FOR_CONNECTION_ESTABLISHMENT);

        PRINTOUT_TRACE("[sample-client][scenario] T3_accept_ms=%llu T4_reject_ms=%llu expected_response=%s\n",
            (unsigned long long)t3_duration_ms,
            (unsigned long long)t4_duration_ms,
            (t3_duration_ms < t4_duration_ms) ? "CONNECTION_ACCEPT" : "CONNECTION_REJECT");

        if (SampleStartGuardTimer(context) == TRUE) {
            if (SampleStartTimer("T3", context->timers.t3_timer_id, t3_duration_ms) == vdrACK_STATUS_OK) {
                if (SampleStartTimer("T4", context->timers.t4_timer_id, t4_duration_ms) == vdrACK_STATUS_OK) {
                    started = TRUE;
                }
            }
        }
    }

    return started;
} /* end of SampleBeginConnectionAttempt() */

/*
 * Handle T2 expiry by retransmitting CONNECTION_REQUEST.
 *
 * T2 is restarted after expiries one through four. The fifth expiry sends the
 * final retransmission, cancels both response timers, and returns to IDLE.
 */
static BOOL SampleHandleGuardExpiry(vdrSampleStateMachine_type* context)
{
    BOOL handled = FALSE;

    assert(context != NULL);

    if (context == NULL) {
        handled = FALSE;
    } else {
        context->t2_expiry_count++;

        SAMPLE_MESSAGE_TRACE("[sample-client][message][TX] CONNECTION_REQUEST attempt=%u\n",
            (unsigned)(context->t2_expiry_count + 1));

        if (context->t2_expiry_count < SAMPLE_MAX_T2_EXPIRIES) {
            handled = SampleStartGuardTimer(context);
        } else {
            PRINTOUT_TRACE("[sample-client][guard] fifth T2 expiry; connection attempt timed out\n");

            if (SampleStopTimer("T3", context->timers.t3_timer_id) == vdrACK_STATUS_OK) {
                if (SampleStopTimer("T4", context->timers.t4_timer_id) == vdrACK_STATUS_OK) {
                    SampleTransitionState(context, vdrSAMPLE_STATE_IDLE);
                    handled = SampleStartIdleTimer(context);
                }
            }
        }
    }

    return handled;
} /* end of SampleHandleGuardExpiry() */

/*
 * Handle simulated CONNECTION_ACCEPT reception caused by T3 expiry.
 *
 * T4 is stopped first so CONNECTION_REJECT cannot subsequently affect the
 * accepted connection. T2 is then stopped because retransmission is complete.
 */
static BOOL SampleHandleConnectionAccept(vdrSampleStateMachine_type* context)
{
    BOOL handled = FALSE;

    assert(context != NULL);

    if (context == NULL) {
        handled = FALSE;
    } else {
        SAMPLE_MESSAGE_TRACE("[sample-client][message][RX] CONNECTION_ACCEPT\n");

        if (SampleStopTimer("T4", context->timers.t4_timer_id) == vdrACK_STATUS_OK) {
            if (SampleStopTimer("T2", context->timers.t2_timer_id) == vdrACK_STATUS_OK) {
                uint64_t t5_duration_ms = SampleRandomDurationMs(
                    SAMPLE_T5_MIN_SECONDS,
                    SAMPLE_T5_MAX_SECONDS);

                context->response_transition_count++;
                SampleTransitionState(context, vdrSAMPLE_STATE_CONNECTED);
                context->next_data_exchange_ms = (uint64_t)GetTickCount64() +
                    SAMPLE_DATA_EXCHANGE_INTERVAL_MS;

                if (SampleStartTimer("T5", context->timers.t5_timer_id, t5_duration_ms) == vdrACK_STATUS_OK) {
                    handled = TRUE;
                }
            }
        }
    }

    return handled;
} /* end of SampleHandleConnectionAccept() */

/*
 * Handle simulated CONNECTION_REJECT reception caused by T4 expiry.
 *
 * T3 is stopped first so CONNECTION_ACCEPT cannot subsequently change the
 * rejected attempt to CONNECTED. T2 is also stopped before returning to IDLE.
 */
static BOOL SampleHandleConnectionReject(vdrSampleStateMachine_type* context)
{
    BOOL handled = FALSE;

    assert(context != NULL);

    if (context == NULL) {
        handled = FALSE;
    } else {
        SAMPLE_MESSAGE_TRACE("[sample-client][message][RX] CONNECTION_REJECT\n");

        if (SampleStopTimer("T3", context->timers.t3_timer_id) == vdrACK_STATUS_OK) {
            if (SampleStopTimer("T2", context->timers.t2_timer_id) == vdrACK_STATUS_OK) {
                context->response_transition_count++;
                SampleTransitionState(context, vdrSAMPLE_STATE_IDLE);
                handled = SampleStartIdleTimer(context);
            }
        }
    }

    return handled;
} /* end of SampleHandleConnectionReject() */

/*
 * Handle simulated CONNECTION_RELEASE reception caused by T5 expiry.
 */
static BOOL SampleHandleConnectionRelease(vdrSampleStateMachine_type* context)
{
    BOOL handled = FALSE;

    assert(context != NULL);

    if (context == NULL) {
        handled = FALSE;
    } else {
        SAMPLE_MESSAGE_TRACE("[sample-client][message][RX] CONNECTION_RELEASE\n");
        SampleTransitionState(context, vdrSAMPLE_STATE_IDLE);
        context->next_data_exchange_ms = 0;
        handled = SampleStartIdleTimer(context);
    }

    return handled;
} /* end of SampleHandleConnectionRelease() */

/*
 * Dispatch one expired timer according to the current state.
 *
 * A known timer received in a different state is treated as a stale queued
 * expiry. This can occur if two timers become due while one transition stops
 * the other timer, so the stale event is traced and ignored safely.
 */
static BOOL SampleProcessExpiredTimer(
    vdrSampleStateMachine_type* context,
    TimerId_type expired_timer_id)
{
    BOOL processed = TRUE;

    assert(context != NULL);
    assert(expired_timer_id != TIMER_ID_INVALID);

    if ((context == NULL) || (expired_timer_id == TIMER_ID_INVALID)) {
        processed = FALSE;
    } else if (expired_timer_id == context->timers.t1_timer_id) {
        if (context->state == vdrSAMPLE_STATE_IDLE) {
            processed = SampleBeginConnectionAttempt(context);
        } else {
            PRINTOUT_TRACE("[sample-client][timer] ignoring stale T1 expiry in state=%s\n",
                SampleStateName(context->state));
        }
    } else if (expired_timer_id == context->timers.t2_timer_id) {
        if (context->state == vdrSAMPLE_STATE_WAITING_FOR_CONNECTION_ESTABLISHMENT) {
            processed = SampleHandleGuardExpiry(context);
        } else {
            PRINTOUT_TRACE("[sample-client][timer] ignoring stale T2 expiry in state=%s\n",
                SampleStateName(context->state));
        }
    } else if (expired_timer_id == context->timers.t3_timer_id) {
        if (context->state == vdrSAMPLE_STATE_WAITING_FOR_CONNECTION_ESTABLISHMENT) {
            processed = SampleHandleConnectionAccept(context);
        } else {
            context->stale_response_expiry_count++;
            PRINTOUT_TRACE("[sample-client][timer] ignoring stale T3 expiry in state=%s\n",
                SampleStateName(context->state));
        }
    } else if (expired_timer_id == context->timers.t4_timer_id) {
        if (context->state == vdrSAMPLE_STATE_WAITING_FOR_CONNECTION_ESTABLISHMENT) {
            processed = SampleHandleConnectionReject(context);
        } else {
            context->stale_response_expiry_count++;
            PRINTOUT_TRACE("[sample-client][timer] ignoring stale T4 expiry in state=%s\n",
                SampleStateName(context->state));
        }
    } else if (expired_timer_id == context->timers.t5_timer_id) {
        if (context->state == vdrSAMPLE_STATE_CONNECTED) {
            processed = SampleHandleConnectionRelease(context);
        } else {
            PRINTOUT_TRACE("[sample-client][timer] ignoring stale T5 expiry in state=%s\n",
                SampleStateName(context->state));
        }
    } else {
        PRINTOUT_TRACE("[sample-client][timer] unknown expired timer=%llu\n",
            (unsigned long long)expired_timer_id);
        processed = FALSE;
    }

    return processed;
} /* end of SampleProcessExpiredTimer() */

/*
 * Drain every timer ID represented by the shared manual-reset notification.
 *
 * Draining is required because one event represents a queue that may contain
 * several expiries. GetExpiredTimer resets the event after removing the last ID.
 */
static BOOL SampleDrainExpiredTimers(vdrSampleStateMachine_type* context)
{
    BOOL drained = TRUE;
    BOOL continue_draining = TRUE;

    assert(context != NULL);

    if (context == NULL) {
        drained = FALSE;
    } else {
        while (continue_draining == TRUE) {
            TimerId_type expired_timer_id = TIMER_ID_INVALID;
            vdrAckStatus_type status = vdrTimerManager_GetExpiredTimer(&expired_timer_id);

            if (status == vdrACK_STATUS_OK) {
                SAMPLE_TIMER_EXPIRY_TRACE("[sample-client][timer-expiry] %s expired timer=%llu\n",
                    SampleTimerName(context, expired_timer_id),
                    (unsigned long long)expired_timer_id);

                if (SampleProcessExpiredTimer(context, expired_timer_id) == FALSE) {
                    drained = FALSE;
                    continue_draining = FALSE;
                }
            } else if (status == vdrACK_STATUS_NO_EXPIRED_TIMERS) {
                continue_draining = FALSE;
            } else {
                PRINTOUT_TRACE("[sample-client][timer] GetExpiredTimer failed status=%u\n",
                    (unsigned)status);
                drained = FALSE;
                continue_draining = FALSE;
            }
        }
    }

    return drained;
} /* end of SampleDrainExpiredTimers() */

/*
 * Return the next main-loop wait timeout.
 *
 * Non-CONNECTED states wait indefinitely for timer or shutdown events. While
 * CONNECTED, the timeout wakes the loop at the next one-second data trace.
 */
static DWORD SampleGetWaitTimeout(const vdrSampleStateMachine_type* context)
{
    DWORD wait_timeout_ms = INFINITE;

    assert(context != NULL);

    if (context == NULL) {
        wait_timeout_ms = 0;
    } else if (context->state == vdrSAMPLE_STATE_CONNECTED) {
        uint64_t now_ms = (uint64_t)GetTickCount64();

        if (now_ms >= context->next_data_exchange_ms) {
            wait_timeout_ms = 0;
        } else {
            uint64_t remaining_ms = context->next_data_exchange_ms - now_ms;

            if (remaining_ms > MAXDWORD) {
                wait_timeout_ms = MAXDWORD;
            } else {
                wait_timeout_ms = (DWORD)remaining_ms;
            }
        }
    }

    return wait_timeout_ms;
} /* end of SampleGetWaitTimeout() */

/*
 * Print the connected-state activity message and schedule its next deadline.
 */
static void SamplePrintDataExchange(vdrSampleStateMachine_type* context)
{
    assert(context != NULL);
    assert((context == NULL) || (context->state == vdrSAMPLE_STATE_CONNECTED));

    if (context == NULL) {
        return;
    }

    if (context->state == vdrSAMPLE_STATE_CONNECTED) {
        PRINTOUT_TRACE("[sample-client][data] Data Exchange is ongoing...\n");
        context->next_data_exchange_ms = (uint64_t)GetTickCount64() +
            SAMPLE_DATA_EXCHANGE_INTERVAL_MS;
    }
} /* end of SamplePrintDataExchange() */

/*
 * Deterministically verify simultaneous T3/T4 handling through the real API.
 *
 * Both response timers expire together. The first queued response performs one
 * state transition and stops its peer; the second queued response must then be
 * classified as stale and must not perform another transition.
 */
static int SampleRunSimultaneousResponseExpiryTest(void)
{
    vdrSampleStateMachine_type context = {0};
    HANDLE notification_event = NULL;
    HANDLE wait_handles[1] = { NULL };
    DWORD wait_result = WAIT_FAILED;
    int exit_code = 1;

    context.state = vdrSAMPLE_STATE_WAITING_FOR_CONNECTION_ESTABLISHMENT;

    PRINTOUT_TRACE("[sample-client][self-test] simultaneous response expiry: begin\n");

    /* CreateTimer remains the first public API operation in self-test mode. */
    if (SampleCreateAllTimers(&context) == TRUE) {
        if (vdrTimerManager_GetNotificationEvent(&notification_event) == vdrACK_STATUS_OK) {
            wait_handles[0] = notification_event;

            if (SampleStartTimer(
                "T3",
                context.timers.t3_timer_id,
                SAMPLE_SIMULTANEOUS_RESPONSE_TEST_DURATION_MS) == vdrACK_STATUS_OK) {
                if (SampleStartTimer(
                    "T4",
                    context.timers.t4_timer_id,
                    SAMPLE_SIMULTANEOUS_RESPONSE_TEST_DURATION_MS) == vdrACK_STATUS_OK) {
                    wait_result = WaitForMultipleObjects(
                        1,
                        wait_handles,
                        FALSE,
                        SAMPLE_SIMULTANEOUS_RESPONSE_TEST_WAIT_MS);

                    if (wait_result == WAIT_OBJECT_0) {
                        /* Let the receiver queue both same-deadline indications
                         * before draining them through production dispatch. */
                        Sleep(SAMPLE_SIMULTANEOUS_RESPONSE_TEST_DURATION_MS);

                        if (SampleDrainExpiredTimers(&context) == TRUE) {
                            if ((context.response_transition_count == 1) &&
                                (context.stale_response_expiry_count == 1) &&
                                ((context.state == vdrSAMPLE_STATE_CONNECTED) ||
                                 (context.state == vdrSAMPLE_STATE_IDLE))) {
                                PRINTOUT_TRACE("[sample-client][self-test] simultaneous response expiry: pass\n");
                                exit_code = 0;
                            } else {
                                PRINTOUT_TRACE("[sample-client][self-test] failed transitions=%u stale=%u state=%s\n",
                                    (unsigned)context.response_transition_count,
                                    (unsigned)context.stale_response_expiry_count,
                                    SampleStateName(context.state));
                            }
                        }
                    } else {
                        PRINTOUT_TRACE("[sample-client][self-test] response wait failed result=%lu\n",
                            (unsigned long)wait_result);
                    }
                }
            }
        }
    }

    if (SampleDeleteAllTimers(&context) == FALSE) {
        exit_code = 1;
    }

    return exit_code;
} /* end of SampleRunSimultaneousResponseExpiryTest() */

int main(int argc, char** argv)
{
    vdrSampleGlobalState_type* global_state = SampleGetGlobalState();
    vdrSampleStateMachine_type context = {0};
    HANDLE notification_event = NULL;
    HANDLE wait_handles[SAMPLE_WAIT_HANDLE_COUNT] = { NULL, NULL };
    BOOL control_handler_installed = FALSE;
    BOOL initialized = FALSE;
    BOOL running = FALSE;
    BOOL shutdown_requested = FALSE;
    int exit_code = 1;

    assert(argv != NULL);

    if (argv == NULL) {
        return 1;
    }

    if ((argc == 2) &&
        (strcmp(argv[1], "--test-simultaneous-response-expiry") == 0)) {
        return SampleRunSimultaneousResponseExpiryTest();
    }

    if (argc != 1) {
        PRINTOUT_TRACE("[sample-client][main] unsupported command-line arguments\n");
        return 1;
    }

    context.state = vdrSAMPLE_STATE_IDLE;

    /* Randomness affects only simulated timer durations and scenario ordering. */
    srand((unsigned)GetTickCount());

    /* Publish the shutdown handle before installing the control handler. This
     * lets Ctrl+C wake the same WaitForMultipleObjects loop used for expiries. */
    global_state->shutdown_event = CreateEvent(NULL, TRUE, FALSE, NULL);

    if (global_state->shutdown_event == NULL) {
        PRINTOUT_TRACE("[sample-client][main] CreateEvent failed error=%lu\n",
            (unsigned long)GetLastError());
    } else {
        if (SetConsoleCtrlHandler(SampleConsoleControlHandler, TRUE) == TRUE) {
            control_handler_installed = TRUE;

            /* CreateTimer is the first Timer Manager API operation. It lazily
             * initializes the client API and connects to the running server. */
            if (SampleCreateAllTimers(&context) == TRUE) {
                PRINTOUT_TRACE("[sample-client][main] requesting Timer Manager notification event\n");

                if (vdrTimerManager_GetNotificationEvent(&notification_event) == vdrACK_STATUS_OK) {
                    wait_handles[SAMPLE_NOTIFICATION_EVENT_INDEX] = notification_event;
                    wait_handles[SAMPLE_SHUTDOWN_EVENT_INDEX] = global_state->shutdown_event;

                    SAMPLE_STATE_TRACE("[sample-client][state] initial state=%s\n",
                        SampleStateName(context.state));

                    if (SampleStartIdleTimer(&context) == TRUE) {
                        initialized = TRUE;
                        running = TRUE;
                    }
                } else {
                    PRINTOUT_TRACE("[sample-client][main] GetNotificationEvent failed\n");
                }
            }
        } else {
            PRINTOUT_TRACE("[sample-client][main] SetConsoleCtrlHandler failed error=%lu\n",
                (unsigned long)GetLastError());
        }
    }

    while (running == TRUE) {
        DWORD wait_timeout_ms = SampleGetWaitTimeout(&context);

        /* Wait for either queued Timer Manager expiries or application shutdown.
         * More Windows waitable objects can be added by extending this handle
         * array and assigning each new handle its own dispatch index. */
        DWORD wait_result = WaitForMultipleObjects(
            SAMPLE_WAIT_HANDLE_COUNT,
            wait_handles,
            FALSE,
            wait_timeout_ms);

        if (wait_result == (WAIT_OBJECT_0 + SAMPLE_NOTIFICATION_EVENT_INDEX)) {
            if (SampleDrainExpiredTimers(&context) == FALSE) {
                running = FALSE;
            }
        } else if (wait_result == (WAIT_OBJECT_0 + SAMPLE_SHUTDOWN_EVENT_INDEX)) {
            PRINTOUT_TRACE("[sample-client][main] shutdown requested\n");
            shutdown_requested = TRUE;
            running = FALSE;
        } else if (wait_result == WAIT_TIMEOUT) {
            SamplePrintDataExchange(&context);
        } else {
            PRINTOUT_TRACE("[sample-client][main] WaitForMultipleObjects failed result=%lu error=%lu\n",
                (unsigned long)wait_result,
                (unsigned long)GetLastError());
            running = FALSE;
        }

        printf("\n\n");
        fflush(stdout);
    }

    if (initialized == TRUE) {
        if (shutdown_requested == TRUE) {
            exit_code = 0;
        }
    }

    if (SampleDeleteAllTimers(&context) == FALSE) {
        exit_code = 1;
    }

    if (control_handler_installed == TRUE) {
        (void)SetConsoleCtrlHandler(SampleConsoleControlHandler, FALSE);
        control_handler_installed = FALSE;
    }

    if (global_state->shutdown_event == NULL) {
        PRINTOUT_TRACE("[sample-client][main] sample stopped before shutdown-event creation\n");
    } else {
        HANDLE shutdown_event_to_close = global_state->shutdown_event;

        /* The handler has been removed, so clear the shared handle before it is
         * closed and prevent any later control callback from signaling it. */
        global_state->shutdown_event = NULL;
        CloseHandle(shutdown_event_to_close);
    }

    PRINTOUT_TRACE("[sample-client][main] sample exiting code=%d\n", exit_code);

    return exit_code;
} /* end of main() */
