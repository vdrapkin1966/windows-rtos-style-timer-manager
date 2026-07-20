/*
 * File:        client_api_integration_tests.c
 * Module:      Timer Manager Integration Tests
 * Author:      Vitaly Drapkin
 * Copyright:   Copyright (c) 2026 Vitaly Drapkin
 * Purpose:     Verify user-visible behavior across the client/server boundary.
 *
 * Design Overview:
 *   - Exercises the public client API against a running server process.
 *   - Verifies command ACKs, timer lifecycle rules, and expiry notifications.
 *   - Runs each scenario independently through the CTest test registration.
 *
 * Concurrency:
 *   Test scenarios run sequentially. The client API receiver thread processes
 *   server messages concurrently with the test thread waiting for results.
 *
 * Assumptions:
 *   - vdrapkinTimerManager is already running and accepting pipe connections.
 *   - Each scenario cleans up the timers and client state that it creates.
 *
 * Revision History:
 *   - Initial implementation.
 */
#include <windows.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "vdr_timer_manager_api.h"

/*
 * Client/API integration tests.
 *
 * These scenarios require vdrapkinTimerManager.exe to already be running.
 * Unlike timer_core_tests.c, these tests use only the public client API and
 * therefore exercise the named-pipe protocol, the server command dispatcher,
 * ACK delivery, and the client expiry-notification event.
 */

#define CHECK(expr) do { \
    if (!(expr)) { \
        printf("CHECK failed at %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        return 1; \
    } \
} while (0)

/*
 * Prefix each trace with its source file and call-site line number, then flush
 * immediately so execution progress remains visible in a live console or log.
 */
#define PRINTOUT_TRACE(...) do { \
    printf("[%s:%d] ", __FILE__, __LINE__); \
    printf(__VA_ARGS__); \
    fflush(stdout); \
} while (0)

typedef int (*IntegrationTestFunction_type)(void);

typedef struct IntegrationTestCase_tag {
    const char* name;
    IntegrationTestFunction_type function;
} IntegrationTestCase_type;

static int WaitForExpiryEvent(HANDLE notification_event, DWORD timeout_ms)
{
    HANDLE wait_handles[1];
    DWORD wait_result;

    CHECK(notification_event != NULL);

    /* Integration tests wait on the same API-owned notification event an
     * application would pass to WaitForMultipleObjects. A signal means at least
     * one expired timer ID should be queued in the client API. */
    wait_handles[0] = notification_event;
    wait_result = WaitForMultipleObjects(1, wait_handles, FALSE, timeout_ms);
    CHECK(wait_result == WAIT_OBJECT_0);

    return 0;
} /* end of WaitForExpiryEvent() */

static int CheckNoExpiryEvent(HANDLE notification_event, DWORD timeout_ms)
{
    HANDLE wait_handles[1];
    DWORD wait_result;

    CHECK(notification_event != NULL);

    /* Negative expiry checks prove that StopTimer/DeleteTimer removed the timer
     * from server scheduling and therefore did not signal the client event. */
    wait_handles[0] = notification_event;
    wait_result = WaitForMultipleObjects(1, wait_handles, FALSE, timeout_ms);
    CHECK(wait_result == WAIT_TIMEOUT);

    return 0;
} /* end of CheckNoExpiryEvent() */

static int TestCreateDeleteRoundTrip(void)
{
    TimerId_type timer_id;

    PRINTOUT_TRACE("[integration] CreateDeleteRoundTrip: begin\n");

    timer_id = TIMER_ID_INVALID;
    CHECK(vdrTimerManager_CreateTimer(&timer_id) == vdrACK_STATUS_OK);
    CHECK(timer_id != TIMER_ID_INVALID);
    CHECK(vdrTimerManager_DeleteTimer(timer_id) == vdrACK_STATUS_OK);

    PRINTOUT_TRACE("[integration] CreateDeleteRoundTrip: pass\n");
    return 0;
} /* end of TestCreateDeleteRoundTrip() */

static int TestStartExpireDeleteRoundTrip(void)
{
    TimerId_type timer_id;
    TimerId_type expired_id;
    HANDLE notification_event;

    PRINTOUT_TRACE("[integration] StartExpireDeleteRoundTrip: begin\n");

    timer_id = TIMER_ID_INVALID;
    expired_id = TIMER_ID_INVALID;
    notification_event = NULL;

    CHECK(vdrTimerManager_CreateTimer(&timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_GetNotificationEvent(&notification_event) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_StartTimer(timer_id, 100) == vdrACK_STATUS_OK);
    CHECK(WaitForExpiryEvent(notification_event, 2000) == 0);
    CHECK(vdrTimerManager_GetExpiredTimer(&expired_id) == vdrACK_STATUS_OK);
    CHECK(expired_id == timer_id);
    CHECK(vdrTimerManager_GetExpiredTimer(&expired_id) == vdrACK_STATUS_NO_EXPIRED_TIMERS);
    CHECK(vdrTimerManager_DeleteTimer(timer_id) == vdrACK_STATUS_OK);

    PRINTOUT_TRACE("[integration] StartExpireDeleteRoundTrip: pass\n");
    return 0;
} /* end of TestStartExpireDeleteRoundTrip() */

static int TestStopActivePreventsExpiry(void)
{
    TimerId_type timer_id;
    HANDLE notification_event;

    PRINTOUT_TRACE("[integration] StopActivePreventsExpiry: begin\n");

    timer_id = TIMER_ID_INVALID;
    notification_event = NULL;

    CHECK(vdrTimerManager_CreateTimer(&timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_GetNotificationEvent(&notification_event) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_StartTimer(timer_id, 300) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_StopTimer(timer_id) == vdrACK_STATUS_OK);
    CHECK(CheckNoExpiryEvent(notification_event, 600) == 0);
    CHECK(vdrTimerManager_DeleteTimer(timer_id) == vdrACK_STATUS_OK);

    PRINTOUT_TRACE("[integration] StopActivePreventsExpiry: pass\n");
    return 0;
} /* end of TestStopActivePreventsExpiry() */

static int TestStopInactiveRoundTrip(void)
{
    TimerId_type timer_id;

    PRINTOUT_TRACE("[integration] StopInactiveRoundTrip: begin\n");

    timer_id = TIMER_ID_INVALID;
    CHECK(vdrTimerManager_CreateTimer(&timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_StopTimer(timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_DeleteTimer(timer_id) == vdrACK_STATUS_OK);

    PRINTOUT_TRACE("[integration] StopInactiveRoundTrip: pass\n");
    return 0;
} /* end of TestStopInactiveRoundTrip() */

static int TestRestartActiveUsesNewDuration(void)
{
    TimerId_type timer_id;
    TimerId_type expired_id;
    HANDLE notification_event;

    PRINTOUT_TRACE("[integration] RestartActiveUsesNewDuration: begin\n");

    timer_id = TIMER_ID_INVALID;
    expired_id = TIMER_ID_INVALID;
    notification_event = NULL;

    CHECK(vdrTimerManager_CreateTimer(&timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_GetNotificationEvent(&notification_event) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_StartTimer(timer_id, 1000) == vdrACK_STATUS_OK);
    Sleep(100);
    CHECK(vdrTimerManager_StartTimer(timer_id, 100) == vdrACK_STATUS_OK);
    CHECK(WaitForExpiryEvent(notification_event, 1500) == 0);
    CHECK(vdrTimerManager_GetExpiredTimer(&expired_id) == vdrACK_STATUS_OK);
    CHECK(expired_id == timer_id);

    /* The restart must replace the old schedule, not add a second expiry. */
    CHECK(CheckNoExpiryEvent(notification_event, 500) == 0);
    CHECK(vdrTimerManager_DeleteTimer(timer_id) == vdrACK_STATUS_OK);

    PRINTOUT_TRACE("[integration] RestartActiveUsesNewDuration: pass\n");
    return 0;
} /* end of TestRestartActiveUsesNewDuration() */

static int TestDeleteActivePreventsExpiry(void)
{
    TimerId_type timer_id;
    HANDLE notification_event;

    PRINTOUT_TRACE("[integration] DeleteActivePreventsExpiry: begin\n");

    timer_id = TIMER_ID_INVALID;
    notification_event = NULL;

    CHECK(vdrTimerManager_CreateTimer(&timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_GetNotificationEvent(&notification_event) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_StartTimer(timer_id, 300) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_DeleteTimer(timer_id) == vdrACK_STATUS_OK);
    CHECK(CheckNoExpiryEvent(notification_event, 600) == 0);

    PRINTOUT_TRACE("[integration] DeleteActivePreventsExpiry: pass\n");
    return 0;
} /* end of TestDeleteActivePreventsExpiry() */

static int TestMultipleTimersExpireThroughQueue(void)
{
    TimerId_type first_id;
    TimerId_type second_id;
    TimerId_type expired_id;
    HANDLE notification_event;

    PRINTOUT_TRACE("[integration] MultipleTimersExpireThroughQueue: begin\n");

    first_id = TIMER_ID_INVALID;
    second_id = TIMER_ID_INVALID;
    expired_id = TIMER_ID_INVALID;
    notification_event = NULL;

    CHECK(vdrTimerManager_CreateTimer(&first_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_CreateTimer(&second_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_GetNotificationEvent(&notification_event) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_StartTimer(first_id, 100) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_StartTimer(second_id, 200) == vdrACK_STATUS_OK);

    CHECK(WaitForExpiryEvent(notification_event, 2000) == 0);
    CHECK(vdrTimerManager_GetExpiredTimer(&expired_id) == vdrACK_STATUS_OK);
    CHECK(expired_id == first_id);

    CHECK(WaitForExpiryEvent(notification_event, 2000) == 0);
    CHECK(vdrTimerManager_GetExpiredTimer(&expired_id) == vdrACK_STATUS_OK);
    CHECK(expired_id == second_id);
    CHECK(vdrTimerManager_GetExpiredTimer(&expired_id) == vdrACK_STATUS_NO_EXPIRED_TIMERS);

    CHECK(vdrTimerManager_DeleteTimer(first_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_DeleteTimer(second_id) == vdrACK_STATUS_OK);

    PRINTOUT_TRACE("[integration] MultipleTimersExpireThroughQueue: pass\n");
    return 0;
} /* end of TestMultipleTimersExpireThroughQueue() */

static int TestQueuedExpiriesKeepNotificationEventSignaled(void)
{
    TimerId_type first_id;
    TimerId_type second_id;
    TimerId_type expired_id;
    HANDLE notification_event;

    PRINTOUT_TRACE("[integration] QueuedExpiriesKeepNotificationEventSignaled: begin\n");

    first_id = TIMER_ID_INVALID;
    second_id = TIMER_ID_INVALID;
    expired_id = TIMER_ID_INVALID;
    notification_event = NULL;

    CHECK(vdrTimerManager_CreateTimer(&first_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_CreateTimer(&second_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_GetNotificationEvent(&notification_event) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_StartTimer(first_id, 100) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_StartTimer(second_id, 100) == vdrACK_STATUS_OK);

    CHECK(WaitForExpiryEvent(notification_event, 2000) == 0);
    Sleep(150);

    /* Both timers have the same scheduled expiry. After the first ID is popped,
     * the manual-reset event must remain signaled because the queue still has
     * another expiry for the application to retrieve. */
    CHECK(vdrTimerManager_GetExpiredTimer(&expired_id) == vdrACK_STATUS_OK);
    CHECK(expired_id == first_id);
    CHECK(WaitForSingleObject(notification_event, 0) == WAIT_OBJECT_0);

    CHECK(vdrTimerManager_GetExpiredTimer(&expired_id) == vdrACK_STATUS_OK);
    CHECK(expired_id == second_id);
    CHECK(WaitForSingleObject(notification_event, 0) == WAIT_TIMEOUT);
    CHECK(vdrTimerManager_GetExpiredTimer(&expired_id) == vdrACK_STATUS_NO_EXPIRED_TIMERS);

    CHECK(vdrTimerManager_DeleteTimer(first_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_DeleteTimer(second_id) == vdrACK_STATUS_OK);

    PRINTOUT_TRACE("[integration] QueuedExpiriesKeepNotificationEventSignaled: pass\n");
    return 0;
} /* end of TestQueuedExpiriesKeepNotificationEventSignaled() */

static int TestNegativeAckStatusesFromUserPerspective(void)
{
    TimerId_type timer_id;
    TimerId_type expired_id;

    PRINTOUT_TRACE("[integration] NegativeAckStatusesFromUserPerspective: begin\n");

    timer_id = TIMER_ID_INVALID;
    expired_id = TIMER_ID_INVALID;

    CHECK(vdrTimerManager_CreateTimer(&timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_StartTimer(TIMER_ID_INVALID, 10) == vdrACK_STATUS_INVALID_TIMER_ID);
    CHECK(vdrTimerManager_StartTimer(timer_id, 0) == vdrACK_STATUS_INVALID_DURATION);
    CHECK(vdrTimerManager_StartTimer(timer_id + 100000, 10) == vdrACK_STATUS_INVALID_TIMER_ID);
    CHECK(vdrTimerManager_DeleteTimer(timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerManager_DeleteTimer(timer_id) == vdrACK_STATUS_INVALID_TIMER_ID);
    CHECK(vdrTimerManager_GetExpiredTimer(&expired_id) == vdrACK_STATUS_NO_EXPIRED_TIMERS);

    PRINTOUT_TRACE("[integration] NegativeAckStatusesFromUserPerspective: pass\n");
    return 0;
} /* end of TestNegativeAckStatusesFromUserPerspective() */

static const IntegrationTestCase_type g_integration_tests[] = {
    { "CreateDeleteRoundTrip", TestCreateDeleteRoundTrip },
    { "StartExpireDeleteRoundTrip", TestStartExpireDeleteRoundTrip },
    { "StopActivePreventsExpiry", TestStopActivePreventsExpiry },
    { "StopInactiveRoundTrip", TestStopInactiveRoundTrip },
    { "RestartActiveUsesNewDuration", TestRestartActiveUsesNewDuration },
    { "DeleteActivePreventsExpiry", TestDeleteActivePreventsExpiry },
    { "MultipleTimersExpireThroughQueue", TestMultipleTimersExpireThroughQueue },
    { "QueuedExpiriesKeepNotificationEventSignaled", TestQueuedExpiriesKeepNotificationEventSignaled },
    { "NegativeAckStatusesFromUserPerspective", TestNegativeAckStatusesFromUserPerspective }
};

static int RunOneIntegrationTest(const char* name)
{
    size_t index;
    int result;

    for (index = 0; index < (sizeof(g_integration_tests) / sizeof(g_integration_tests[0])); ++index) {
        if (strcmp(name, g_integration_tests[index].name) == 0) {
            result = g_integration_tests[index].function();
            printf("\n\n");
            fflush(stdout);
            return result;
        }
    }

    PRINTOUT_TRACE("[integration] unknown scenario: %s\n", name);
    printf("\n\n");
    fflush(stdout);
    return 1;
} /* end of RunOneIntegrationTest() */

int main(int argc, char** argv)
{
    size_t index;

    if (argc == 2) {
        return RunOneIntegrationTest(argv[1]);
    }

    PRINTOUT_TRACE("[integration] vdrTimerManagerIntegrationTests: begin all scenarios\n");

    for (index = 0; index < (sizeof(g_integration_tests) / sizeof(g_integration_tests[0])); ++index) {
        if (RunOneIntegrationTest(g_integration_tests[index].name) != 0) {
            PRINTOUT_TRACE("[integration] vdrTimerManagerIntegrationTests: failed\n");
            return 1;
        }
    }

    PRINTOUT_TRACE("[integration] vdrTimerManagerIntegrationTests: all scenarios passed\n");
    return 0;
} /* end of main() */
