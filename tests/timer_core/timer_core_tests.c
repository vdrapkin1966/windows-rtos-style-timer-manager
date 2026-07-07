/*
 * File:        timer_core_tests.c
 * Module:      Timer Manager Core Unit Tests
 * Author:      Vitaly Drapkin
 * Copyright:   Copyright (c) 2026 Vitaly Drapkin
 * Purpose:     Verify timer-core behavior without named-pipe dependencies.
 *
 * Design Overview:
 *   - Exercises timer creation, start, restart, stop, delete, and expiry paths.
 *   - Checks active-list ordering, delta timing, ownership, and error handling.
 *   - Supports individual CTest scenarios and an all-scenarios console run.
 *
 * Concurrency:
 *   Tests are single-threaded because the timer core requires its caller to
 *   serialize access.
 *
 * Assumptions:
 *   - Synthetic timestamps and pipe handles are valid deterministic test data.
 *   - Every allocated timer is released before a test scenario finishes.
 *
 * Revision History:
 *   - 2026-07-01: Wrapped mutable callback-test state behind an accessor.
 *   - 2026-06-30: Added UINT64_MAX arithmetic boundary coverage.
 *   - Initial implementation.
 */
#include <windows.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "vdr_timer_core.h"

/*
 * Lightweight unit tests for the timer core.
 *
 * The test executable accepts one scenario name on the command line. CTest uses
 * that mode so every user-visible scenario appears as a separate test result.
 * Running the executable without arguments executes all scenarios in order and
 * prints each step to the console.
 */

#define CHECK(expr) do { \
    if (!(expr)) { \
        printf("CHECK failed at %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        return 1; \
    } \
} while (0)

typedef int (*TestFunction_type)(void);

typedef struct TestCase_tag {
    const char* name;
    TestFunction_type function;
} TestCase_type;

typedef struct ExpiredCallbackRecord_tag {
    TimerId_type timer_id;
    HANDLE server_pipe;
} ExpiredCallbackRecord_type;

typedef struct TestGlobalState_tag {
    /* Records expiry callback arguments for assertions in the current test. */
    ExpiredCallbackRecord_type expired_records[8];

    /* Number of valid entries currently stored in expired_records. */
    uint32_t expired_record_count;
} TestGlobalState_type;

/* Mutable test state is accessed only through TestGetGlobalState(). */
static TestGlobalState_type g_test_global_state;

/*
 * Return the process-lifetime mutable state used by callback-based tests.
 */
static TestGlobalState_type* TestGetGlobalState(void)
{
    return &g_test_global_state;
} /* end of TestGetGlobalState() */

static void ResetExpiredCallbackRecords(void)
{
    TestGlobalState_type* test_state = TestGetGlobalState();

    memset(test_state->expired_records, 0, sizeof(test_state->expired_records));
    test_state->expired_record_count = 0;
} /* end of ResetExpiredCallbackRecords() */

static void TestExpiredCallback(TimerId_type timer_id, HANDLE server_pipe)
{
    TestGlobalState_type* test_state = TestGetGlobalState();

    assert(timer_id != TIMER_ID_INVALID);
    assert(server_pipe != NULL);
    assert(server_pipe != INVALID_HANDLE_VALUE);

    if ((timer_id == TIMER_ID_INVALID) ||
        (server_pipe == NULL) ||
        (server_pipe == INVALID_HANDLE_VALUE) ||
        (test_state->expired_record_count >= 8)) {
        return;
    }

    test_state->expired_records[test_state->expired_record_count].timer_id = timer_id;
    test_state->expired_records[test_state->expired_record_count].server_pipe = server_pipe;
    test_state->expired_record_count++;
} /* end of TestExpiredCallback() */

static HANDLE TestPipe(uintptr_t value)
{
    /* Unit tests do not need real pipe handles. The timer core only compares
     * owner handles for equality, so small nonzero pseudo-handles are enough. */
    return (HANDLE)value;
} /* end of TestPipe() */

static int TestCreateTimerCreatesInactiveTimer(void)
{
    vdrTimerCore_type core;
    TimerId_type timer_id;

    printf("[test] CreateTimerCreatesInactiveTimer: begin\n");
    vdrTimerCore_Init(&core);

    timer_id = TIMER_ID_INVALID;
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &timer_id) == vdrACK_STATUS_OK);
    CHECK(timer_id != TIMER_ID_INVALID);
    CHECK(vdrTimerCore_CountInactive(&core) == 1);
    CHECK(vdrTimerCore_CountActive(&core) == 0);
    CHECK(vdrTimerCore_TimerIdInUse(&core, timer_id) == TRUE);

    vdrTimerCore_Deinit(&core);
    printf("[test] CreateTimerCreatesInactiveTimer: pass\n");
    return 0;
} /* end of TestCreateTimerCreatesInactiveTimer() */

static int TestCreateTimerAllocatesRunningIds(void)
{
    vdrTimerCore_type core;
    TimerId_type first;
    TimerId_type second;
    TimerId_type third;

    printf("[test] CreateTimerAllocatesRunningIds: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &first) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &second) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &third) == vdrACK_STATUS_OK);
    CHECK(first == 1);
    CHECK(second == 2);
    CHECK(third == 3);
    CHECK(vdrTimerCore_CountInactive(&core) == 3);

    vdrTimerCore_Deinit(&core);
    printf("[test] CreateTimerAllocatesRunningIds: pass\n");
    return 0;
} /* end of TestCreateTimerAllocatesRunningIds() */

static int TestTimerIdWrapSkipsInvalidId(void)
{
    vdrTimerCore_type core;
    TimerId_type first;
    TimerId_type second;

    printf("[test] TimerIdWrapSkipsInvalidId: begin\n");
    vdrTimerCore_Init(&core);

    core.next_timer_id = UINT64_MAX;
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &first) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &second) == vdrACK_STATUS_OK);
    CHECK(first == UINT64_MAX);
    CHECK(second != TIMER_ID_INVALID);
    CHECK(second == 1);

    vdrTimerCore_Deinit(&core);
    printf("[test] TimerIdWrapSkipsInvalidId: pass\n");
    return 0;
} /* end of TestTimerIdWrapSkipsInvalidId() */

static int TestTimerIdWrapSkipsIdAlreadyInUse(void)
{
    vdrTimerCore_type core;
    TimerId_type existing_id;
    TimerId_type wrapped_id;

    printf("[test] TimerIdWrapSkipsIdAlreadyInUse: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &existing_id) == vdrACK_STATUS_OK);
    CHECK(existing_id == 1);

    /* Force the allocator back to one while timer ID one is still allocated.
     * The next allocation must skip the live ID instead of reusing it. */
    core.next_timer_id = 1;
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &wrapped_id) == vdrACK_STATUS_OK);
    CHECK(wrapped_id == 2);

    vdrTimerCore_Deinit(&core);
    printf("[test] TimerIdWrapSkipsIdAlreadyInUse: pass\n");
    return 0;
} /* end of TestTimerIdWrapSkipsIdAlreadyInUse() */

static int TestStartTimerMovesInactiveToActive(void)
{
    vdrTimerCore_type core;
    TimerId_type timer_id;

    printf("[test] StartTimerMovesInactiveToActive: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), timer_id, 100, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CountInactive(&core) == 0);
    CHECK(vdrTimerCore_CountActive(&core) == 1);
    CHECK(core.active_head != NULL);
    CHECK(core.active_head->id == timer_id);
    CHECK(core.active_head->time_remainder_ms == 100);

    vdrTimerCore_Deinit(&core);
    printf("[test] StartTimerMovesInactiveToActive: pass\n");
    return 0;
} /* end of TestStartTimerMovesInactiveToActive() */

static int TestStartTimerRejectsZeroDuration(void)
{
    vdrTimerCore_type core;
    TimerId_type timer_id;

    printf("[test] StartTimerRejectsZeroDuration: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), timer_id, 0, 1000) == vdrACK_STATUS_INVALID_DURATION);
    CHECK(vdrTimerCore_CountInactive(&core) == 1);
    CHECK(vdrTimerCore_CountActive(&core) == 0);

    vdrTimerCore_Deinit(&core);
    printf("[test] StartTimerRejectsZeroDuration: pass\n");
    return 0;
} /* end of TestStartTimerRejectsZeroDuration() */

static int TestInvalidTimerIdRejectedByCommands(void)
{
    vdrTimerCore_type core;

    printf("[test] InvalidTimerIdRejectedByCommands: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), TIMER_ID_INVALID, 10, 1000) == vdrACK_STATUS_INVALID_TIMER_ID);
    CHECK(vdrTimerCore_StopTimer(&core, TestPipe(1), TIMER_ID_INVALID, 1000) == vdrACK_STATUS_INVALID_TIMER_ID);
    CHECK(vdrTimerCore_DeleteTimer(&core, TestPipe(1), TIMER_ID_INVALID, 1000) == vdrACK_STATUS_INVALID_TIMER_ID);

    vdrTimerCore_Deinit(&core);
    printf("[test] InvalidTimerIdRejectedByCommands: pass\n");
    return 0;
} /* end of TestInvalidTimerIdRejectedByCommands() */

static int TestWrongOwnerRejectedByCommands(void)
{
    vdrTimerCore_type core;
    TimerId_type timer_id;

    printf("[test] WrongOwnerRejectedByCommands: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(2), timer_id, 10, 1000) == vdrACK_STATUS_TIMER_NOT_OWNED);
    CHECK(vdrTimerCore_StopTimer(&core, TestPipe(2), timer_id, 1000) == vdrACK_STATUS_TIMER_NOT_OWNED);
    CHECK(vdrTimerCore_DeleteTimer(&core, TestPipe(2), timer_id, 1000) == vdrACK_STATUS_TIMER_NOT_OWNED);

    vdrTimerCore_Deinit(&core);
    printf("[test] WrongOwnerRejectedByCommands: pass\n");
    return 0;
} /* end of TestWrongOwnerRejectedByCommands() */

static int TestSortedDeltaListUsesScheduledExpiryDeltas(void)
{
    vdrTimerCore_type core;
    TimerId_type a;
    TimerId_type b;
    TimerId_type c;
    uint64_t wait_ms;

    printf("[test] SortedDeltaListUsesScheduledExpiryDeltas: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &a) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &b) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &c) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), a, 400, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), b, 100, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), c, 250, 1000) == vdrACK_STATUS_OK);

    CHECK(core.active_head != NULL);
    CHECK(core.active_head->id == b);
    CHECK(core.active_head->time_remainder_ms == 100);
    CHECK(core.active_head->next_timer != NULL);
    CHECK(core.active_head->next_timer->id == c);
    CHECK(core.active_head->next_timer->time_remainder_ms == 150);
    CHECK(core.active_head->next_timer->next_timer != NULL);
    CHECK(core.active_head->next_timer->next_timer->id == a);
    CHECK(core.active_head->next_timer->next_timer->time_remainder_ms == 150);
    CHECK(vdrTimerCore_GetNextWaitDuration(&core, 1050, &wait_ms) == TRUE);
    CHECK(wait_ms == 50);

    vdrTimerCore_Deinit(&core);
    printf("[test] SortedDeltaListUsesScheduledExpiryDeltas: pass\n");
    return 0;
} /* end of TestSortedDeltaListUsesScheduledExpiryDeltas() */

static int TestEqualExpiryTimersExpireTogether(void)
{
    TestGlobalState_type* test_state = TestGetGlobalState();
    vdrTimerCore_type core;
    TimerId_type first;
    TimerId_type second;

    printf("[test] EqualExpiryTimersExpireTogether: begin\n");
    vdrTimerCore_Init(&core);
    ResetExpiredCallbackRecords();

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &first) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &second) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), first, 100, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), second, 100, 1000) == vdrACK_STATUS_OK);
    CHECK(core.active_head != NULL);
    CHECK(core.active_head->next_timer != NULL);
    CHECK(core.active_head->next_timer->time_remainder_ms == 0);

    vdrTimerCore_ProcessExpired(&core, 1100, TestExpiredCallback);
    CHECK(test_state->expired_record_count == 2);
    CHECK(vdrTimerCore_CountActive(&core) == 0);
    CHECK(vdrTimerCore_CountInactive(&core) == 2);

    vdrTimerCore_Deinit(&core);
    printf("[test] EqualExpiryTimersExpireTogether: pass\n");
    return 0;
} /* end of TestEqualExpiryTimersExpireTogether() */

static int TestProcessExpiredDoesNotExpireEarly(void)
{
    TestGlobalState_type* test_state = TestGetGlobalState();
    vdrTimerCore_type core;
    TimerId_type timer_id;

    printf("[test] ProcessExpiredDoesNotExpireEarly: begin\n");
    vdrTimerCore_Init(&core);
    ResetExpiredCallbackRecords();

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), timer_id, 100, 1000) == vdrACK_STATUS_OK);
    vdrTimerCore_ProcessExpired(&core, 1099, TestExpiredCallback);
    CHECK(test_state->expired_record_count == 0);
    CHECK(vdrTimerCore_CountActive(&core) == 1);
    CHECK(vdrTimerCore_CountInactive(&core) == 0);

    vdrTimerCore_Deinit(&core);
    printf("[test] ProcessExpiredDoesNotExpireEarly: pass\n");
    return 0;
} /* end of TestProcessExpiredDoesNotExpireEarly() */

static int TestProcessExpiredMovesTimerBackInactive(void)
{
    TestGlobalState_type* test_state = TestGetGlobalState();
    vdrTimerCore_type core;
    TimerId_type timer_id;

    printf("[test] ProcessExpiredMovesTimerBackInactive: begin\n");
    vdrTimerCore_Init(&core);
    ResetExpiredCallbackRecords();

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), timer_id, 100, 1000) == vdrACK_STATUS_OK);
    vdrTimerCore_ProcessExpired(&core, 1100, TestExpiredCallback);
    CHECK(test_state->expired_record_count == 1);
    CHECK(test_state->expired_records[0].timer_id == timer_id);
    CHECK(test_state->expired_records[0].server_pipe == TestPipe(1));
    CHECK(vdrTimerCore_CountActive(&core) == 0);
    CHECK(vdrTimerCore_CountInactive(&core) == 1);

    vdrTimerCore_Deinit(&core);
    printf("[test] ProcessExpiredMovesTimerBackInactive: pass\n");
    return 0;
} /* end of TestProcessExpiredMovesTimerBackInactive() */

static int TestProcessExpiredWithNullCallbackMovesTimerInactive(void)
{
    vdrTimerCore_type core;
    TimerId_type timer_id;

    printf("[test] ProcessExpiredWithNullCallbackMovesTimerInactive: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), timer_id, 100, 1000) == vdrACK_STATUS_OK);

    /* A NULL callback represents a core-only use case. Expiry processing must
     * still move the one-shot timer back to inactive state. */
    vdrTimerCore_ProcessExpired(&core, 1100, NULL);
    CHECK(vdrTimerCore_CountActive(&core) == 0);
    CHECK(vdrTimerCore_CountInactive(&core) == 1);
    CHECK(vdrTimerCore_TimerIdInUse(&core, timer_id) == TRUE);

    vdrTimerCore_Deinit(&core);
    printf("[test] ProcessExpiredWithNullCallbackMovesTimerInactive: pass\n");
    return 0;
} /* end of TestProcessExpiredWithNullCallbackMovesTimerInactive() */

static int TestProcessExpiredMultipleStaggeredTimersInOrder(void)
{
    TestGlobalState_type* test_state = TestGetGlobalState();
    vdrTimerCore_type core;
    TimerId_type first;
    TimerId_type second;
    TimerId_type third;

    printf("[test] ProcessExpiredMultipleStaggeredTimersInOrder: begin\n");
    vdrTimerCore_Init(&core);
    ResetExpiredCallbackRecords();

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &first) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &second) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &third) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), second, 200, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), third, 300, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), first, 100, 1000) == vdrACK_STATUS_OK);

    /* Processing late should expire all due timers once, in scheduled order. */
    vdrTimerCore_ProcessExpired(&core, 1400, TestExpiredCallback);
    CHECK(test_state->expired_record_count == 3);
    CHECK(test_state->expired_records[0].timer_id == first);
    CHECK(test_state->expired_records[1].timer_id == second);
    CHECK(test_state->expired_records[2].timer_id == third);
    CHECK(vdrTimerCore_CountActive(&core) == 0);
    CHECK(vdrTimerCore_CountInactive(&core) == 3);

    vdrTimerCore_Deinit(&core);
    printf("[test] ProcessExpiredMultipleStaggeredTimersInOrder: pass\n");
    return 0;
} /* end of TestProcessExpiredMultipleStaggeredTimersInOrder() */

static int TestPartialExpiryLeavesLaterTimersActive(void)
{
    TestGlobalState_type* test_state = TestGetGlobalState();
    vdrTimerCore_type core;
    TimerId_type early;
    TimerId_type late;

    printf("[test] PartialExpiryLeavesLaterTimersActive: begin\n");
    vdrTimerCore_Init(&core);
    ResetExpiredCallbackRecords();

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &early) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &late) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), early, 100, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), late, 300, 1000) == vdrACK_STATUS_OK);

    vdrTimerCore_ProcessExpired(&core, 1100, TestExpiredCallback);
    CHECK(test_state->expired_record_count == 1);
    CHECK(test_state->expired_records[0].timer_id == early);
    CHECK(vdrTimerCore_CountActive(&core) == 1);
    CHECK(vdrTimerCore_CountInactive(&core) == 1);
    CHECK(core.active_head != NULL);
    CHECK(core.active_head->id == late);
    CHECK(core.active_head->time_remainder_ms == 200);

    vdrTimerCore_Deinit(&core);
    printf("[test] PartialExpiryLeavesLaterTimersActive: pass\n");
    return 0;
} /* end of TestPartialExpiryLeavesLaterTimersActive() */

static int TestExpiredTimerCanBeRestarted(void)
{
    TestGlobalState_type* test_state = TestGetGlobalState();
    vdrTimerCore_type core;
    TimerId_type timer_id;

    printf("[test] ExpiredTimerCanBeRestarted: begin\n");
    vdrTimerCore_Init(&core);
    ResetExpiredCallbackRecords();

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), timer_id, 100, 1000) == vdrACK_STATUS_OK);
    vdrTimerCore_ProcessExpired(&core, 1100, TestExpiredCallback);
    CHECK(test_state->expired_record_count == 1);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), timer_id, 50, 1200) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CountActive(&core) == 1);
    CHECK(vdrTimerCore_CountInactive(&core) == 0);

    vdrTimerCore_Deinit(&core);
    printf("[test] ExpiredTimerCanBeRestarted: pass\n");
    return 0;
} /* end of TestExpiredTimerCanBeRestarted() */

static int TestRestartActiveTimerWithShorterDuration(void)
{
    vdrTimerCore_type core;
    TimerId_type first;
    TimerId_type second;

    printf("[test] RestartActiveTimerWithShorterDuration: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &first) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &second) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), first, 100, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), second, 200, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), second, 50, 1020) == vdrACK_STATUS_OK);
    CHECK(core.active_head != NULL);
    CHECK(core.active_head->id == second);
    CHECK(core.active_head->time_remainder_ms == 50);

    vdrTimerCore_Deinit(&core);
    printf("[test] RestartActiveTimerWithShorterDuration: pass\n");
    return 0;
} /* end of TestRestartActiveTimerWithShorterDuration() */

static int TestRestartActiveTimerWithLongerDuration(void)
{
    vdrTimerCore_type core;
    TimerId_type first;
    TimerId_type second;

    printf("[test] RestartActiveTimerWithLongerDuration: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &first) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &second) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), first, 100, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), second, 200, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), first, 500, 1020) == vdrACK_STATUS_OK);
    CHECK(core.active_head != NULL);
    CHECK(core.active_head->id == second);
    CHECK(core.active_head->next_timer != NULL);
    CHECK(core.active_head->next_timer->id == first);

    vdrTimerCore_Deinit(&core);
    printf("[test] RestartActiveTimerWithLongerDuration: pass\n");
    return 0;
} /* end of TestRestartActiveTimerWithLongerDuration() */

static int TestStopActiveTimerMovesItInactive(void)
{
    vdrTimerCore_type core;
    TimerId_type timer_id;

    printf("[test] StopActiveTimerMovesItInactive: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), timer_id, 100, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StopTimer(&core, TestPipe(1), timer_id, 1050) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CountActive(&core) == 0);
    CHECK(vdrTimerCore_CountInactive(&core) == 1);

    vdrTimerCore_Deinit(&core);
    printf("[test] StopActiveTimerMovesItInactive: pass\n");
    return 0;
} /* end of TestStopActiveTimerMovesItInactive() */

static int TestStopMiddleActiveTimerPreservesFollowingDelta(void)
{
    vdrTimerCore_type core;
    TimerId_type first;
    TimerId_type middle;
    TimerId_type last;

    printf("[test] StopMiddleActiveTimerPreservesFollowingDelta: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &first) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &middle) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &last) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), first, 100, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), middle, 300, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), last, 500, 1000) == vdrACK_STATUS_OK);

    CHECK(vdrTimerCore_StopTimer(&core, TestPipe(1), middle, 1050) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CountActive(&core) == 2);
    CHECK(vdrTimerCore_CountInactive(&core) == 1);
    CHECK(core.active_head != NULL);
    CHECK(core.active_head->id == first);
    CHECK(core.active_head->time_remainder_ms == 50);
    CHECK(core.active_head->next_timer != NULL);
    CHECK(core.active_head->next_timer->id == last);
    CHECK(core.active_head->next_timer->time_remainder_ms == 400);

    vdrTimerCore_Deinit(&core);
    printf("[test] StopMiddleActiveTimerPreservesFollowingDelta: pass\n");
    return 0;
} /* end of TestStopMiddleActiveTimerPreservesFollowingDelta() */

static int TestStopInactiveTimerIsSuccessful(void)
{
    vdrTimerCore_type core;
    TimerId_type timer_id;

    printf("[test] StopInactiveTimerIsSuccessful: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StopTimer(&core, TestPipe(1), timer_id, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CountInactive(&core) == 1);

    vdrTimerCore_Deinit(&core);
    printf("[test] StopInactiveTimerIsSuccessful: pass\n");
    return 0;
} /* end of TestStopInactiveTimerIsSuccessful() */

static int TestDeleteInactiveTimerRemovesIt(void)
{
    vdrTimerCore_type core;
    TimerId_type timer_id;

    printf("[test] DeleteInactiveTimerRemovesIt: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_DeleteTimer(&core, TestPipe(1), timer_id, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CountInactive(&core) == 0);
    CHECK(vdrTimerCore_TimerIdInUse(&core, timer_id) == FALSE);
    CHECK(vdrTimerCore_DeleteTimer(&core, TestPipe(1), timer_id, 1000) == vdrACK_STATUS_INVALID_TIMER_ID);

    vdrTimerCore_Deinit(&core);
    printf("[test] DeleteInactiveTimerRemovesIt: pass\n");
    return 0;
} /* end of TestDeleteInactiveTimerRemovesIt() */

static int TestDeleteActiveTimerRemovesItAndKeepsOthers(void)
{
    vdrTimerCore_type core;
    TimerId_type first;
    TimerId_type second;
    uint64_t wait_ms;

    printf("[test] DeleteActiveTimerRemovesItAndKeepsOthers: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &first) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &second) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), first, 100, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), second, 300, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_DeleteTimer(&core, TestPipe(1), first, 1050) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CountActive(&core) == 1);
    CHECK(vdrTimerCore_TimerIdInUse(&core, first) == FALSE);
    CHECK(vdrTimerCore_TimerIdInUse(&core, second) == TRUE);
    CHECK(vdrTimerCore_GetNextWaitDuration(&core, 1050, &wait_ms) == TRUE);
    CHECK(wait_ms == 250);

    vdrTimerCore_Deinit(&core);
    printf("[test] DeleteActiveTimerRemovesItAndKeepsOthers: pass\n");
    return 0;
} /* end of TestDeleteActiveTimerRemovesItAndKeepsOthers() */

static int TestDeleteMiddleActiveTimerPreservesFollowingDelta(void)
{
    vdrTimerCore_type core;
    TimerId_type first;
    TimerId_type middle;
    TimerId_type last;
    uint64_t wait_ms;

    printf("[test] DeleteMiddleActiveTimerPreservesFollowingDelta: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &first) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &middle) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &last) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), first, 100, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), middle, 300, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), last, 500, 1000) == vdrACK_STATUS_OK);

    CHECK(vdrTimerCore_DeleteTimer(&core, TestPipe(1), middle, 1050) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CountActive(&core) == 2);
    CHECK(vdrTimerCore_TimerIdInUse(&core, middle) == FALSE);
    CHECK(core.active_head != NULL);
    CHECK(core.active_head->id == first);
    CHECK(core.active_head->time_remainder_ms == 50);
    CHECK(core.active_head->next_timer != NULL);
    CHECK(core.active_head->next_timer->id == last);
    CHECK(core.active_head->next_timer->time_remainder_ms == 400);
    CHECK(vdrTimerCore_GetNextWaitDuration(&core, 1050, &wait_ms) == TRUE);
    CHECK(wait_ms == 50);

    vdrTimerCore_Deinit(&core);
    printf("[test] DeleteMiddleActiveTimerPreservesFollowingDelta: pass\n");
    return 0;
} /* end of TestDeleteMiddleActiveTimerPreservesFollowingDelta() */

static int TestGetNextWaitDurationWhenNoActiveTimer(void)
{
    vdrTimerCore_type core;
    uint64_t wait_ms;

    printf("[test] GetNextWaitDurationWhenNoActiveTimer: begin\n");
    vdrTimerCore_Init(&core);

    wait_ms = 123;
    CHECK(vdrTimerCore_GetNextWaitDuration(&core, 1000, &wait_ms) == FALSE);
    CHECK(wait_ms == 0);

    vdrTimerCore_Deinit(&core);
    printf("[test] GetNextWaitDurationWhenNoActiveTimer: pass\n");
    return 0;
} /* end of TestGetNextWaitDurationWhenNoActiveTimer() */

static int TestRemoveTimersForPipeRemovesOnlyThatOwner(void)
{
    vdrTimerCore_type core;
    TimerId_type owner1_active;
    TimerId_type owner2_active;
    TimerId_type owner1_inactive;

    printf("[test] RemoveTimersForPipeRemovesOnlyThatOwner: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &owner1_active) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(2), &owner2_active) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &owner1_inactive) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), owner1_active, 100, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(2), owner2_active, 100, 1000) == vdrACK_STATUS_OK);

    vdrTimerCore_RemoveTimersForPipe(&core, TestPipe(1));
    CHECK(vdrTimerCore_TimerIdInUse(&core, owner1_active) == FALSE);
    CHECK(vdrTimerCore_TimerIdInUse(&core, owner1_inactive) == FALSE);
    CHECK(vdrTimerCore_TimerIdInUse(&core, owner2_active) == TRUE);
    CHECK(vdrTimerCore_CountActive(&core) == 1);

    vdrTimerCore_Deinit(&core);
    printf("[test] RemoveTimersForPipeRemovesOnlyThatOwner: pass\n");
    return 0;
} /* end of TestRemoveTimersForPipeRemovesOnlyThatOwner() */

static int TestRemoveTimersForPipePreservesRemainingActiveDeltas(void)
{
    vdrTimerCore_type core;
    TimerId_type first_owner_timer;
    TimerId_type removed_middle_timer;
    TimerId_type second_owner_timer;

    printf("[test] RemoveTimersForPipePreservesRemainingActiveDeltas: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &first_owner_timer) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(2), &removed_middle_timer) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &second_owner_timer) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), first_owner_timer, 100, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(2), removed_middle_timer, 300, 1000) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), second_owner_timer, 500, 1000) == vdrACK_STATUS_OK);

    /* Disconnect cleanup removes all timers for one owner. Removing the middle
     * active node must not leave a stale delta on the following active timer. */
    vdrTimerCore_RemoveTimersForPipe(&core, TestPipe(2));
    CHECK(vdrTimerCore_TimerIdInUse(&core, removed_middle_timer) == FALSE);
    CHECK(vdrTimerCore_CountActive(&core) == 2);
    CHECK(core.active_head != NULL);
    CHECK(core.active_head->id == first_owner_timer);
    CHECK(core.active_head->next_timer != NULL);
    CHECK(core.active_head->next_timer->id == second_owner_timer);
    CHECK(core.active_head->next_timer->time_remainder_ms == 400);

    vdrTimerCore_Deinit(&core);
    printf("[test] RemoveTimersForPipePreservesRemainingActiveDeltas: pass\n");
    return 0;
} /* end of TestRemoveTimersForPipePreservesRemainingActiveDeltas() */

/*
 * Verify that start_time + duration saturates instead of wrapping to the past.
 */
static int TestExpiryArithmeticSaturatesAtUint64Max(void)
{
    TestGlobalState_type* test_state = TestGetGlobalState();
    vdrTimerCore_type core;
    TimerId_type timer_id;
    uint64_t wait_ms;
    const uint64_t start_ms = UINT64_MAX - 10;

    printf("[test] ExpiryArithmeticSaturatesAtUint64Max: begin\n");
    vdrTimerCore_Init(&core);
    ResetExpiredCallbackRecords();

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), timer_id, 20, start_ms) == vdrACK_STATUS_OK);

    /* The mathematical expiry exceeds UINT64_MAX. Saturation makes the timer
     * wait ten representable milliseconds instead of wrapping and expiring. */
    CHECK(vdrTimerCore_GetNextWaitDuration(&core, start_ms, &wait_ms) == TRUE);
    CHECK(wait_ms == 10);

    vdrTimerCore_ProcessExpired(&core, UINT64_MAX - 1, TestExpiredCallback);
    CHECK(test_state->expired_record_count == 0);
    CHECK(vdrTimerCore_CountActive(&core) == 1);

    vdrTimerCore_ProcessExpired(&core, UINT64_MAX, TestExpiredCallback);
    CHECK(test_state->expired_record_count == 1);
    CHECK(test_state->expired_records[0].timer_id == timer_id);
    CHECK(vdrTimerCore_CountActive(&core) == 0);
    CHECK(vdrTimerCore_CountInactive(&core) == 1);

    vdrTimerCore_Deinit(&core);
    printf("[test] ExpiryArithmeticSaturatesAtUint64Max: pass\n");
    return 0;
} /* end of TestExpiryArithmeticSaturatesAtUint64Max() */

/*
 * Verify ordering and delta rebuilding around the largest uint64_t timestamp.
 */
static int TestNearUint64MaxTimersRemainOrdered(void)
{
    TestGlobalState_type* test_state = TestGetGlobalState();
    vdrTimerCore_type core;
    TimerId_type saturated_timer_id;
    TimerId_type earlier_timer_id;
    uint64_t wait_ms;
    const uint64_t start_ms = UINT64_MAX - 10;

    printf("[test] NearUint64MaxTimersRemainOrdered: begin\n");
    vdrTimerCore_Init(&core);
    ResetExpiredCallbackRecords();

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &saturated_timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &earlier_timer_id) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), saturated_timer_id, 20, start_ms) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), earlier_timer_id, 5, start_ms) == vdrACK_STATUS_OK);

    CHECK(core.active_head != NULL);
    CHECK(core.active_head->id == earlier_timer_id);
    CHECK(core.active_head->time_remainder_ms == 5);
    CHECK(core.active_head->next_timer != NULL);
    CHECK(core.active_head->next_timer->id == saturated_timer_id);
    CHECK(core.active_head->next_timer->time_remainder_ms == 5);

    vdrTimerCore_ProcessExpired(&core, UINT64_MAX - 5, TestExpiredCallback);
    CHECK(test_state->expired_record_count == 1);
    CHECK(test_state->expired_records[0].timer_id == earlier_timer_id);
    CHECK(vdrTimerCore_GetNextWaitDuration(&core, UINT64_MAX - 5, &wait_ms) == TRUE);
    CHECK(wait_ms == 5);

    vdrTimerCore_ProcessExpired(&core, UINT64_MAX, TestExpiredCallback);
    CHECK(test_state->expired_record_count == 2);
    CHECK(test_state->expired_records[1].timer_id == saturated_timer_id);
    CHECK(vdrTimerCore_CountActive(&core) == 0);

    vdrTimerCore_Deinit(&core);
    printf("[test] NearUint64MaxTimersRemainOrdered: pass\n");
    return 0;
} /* end of TestNearUint64MaxTimersRemainOrdered() */
static int TestDeinitRemovesAllTimers(void)
{
    vdrTimerCore_type core;
    TimerId_type first;
    TimerId_type second;

    printf("[test] DeinitRemovesAllTimers: begin\n");
    vdrTimerCore_Init(&core);

    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(1), &first) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_CreateTimer(&core, TestPipe(2), &second) == vdrACK_STATUS_OK);
    CHECK(vdrTimerCore_StartTimer(&core, TestPipe(1), first, 100, 1000) == vdrACK_STATUS_OK);
    vdrTimerCore_Deinit(&core);
    CHECK(vdrTimerCore_CountActive(&core) == 0);
    CHECK(vdrTimerCore_CountInactive(&core) == 0);
    CHECK(core.next_timer_id == 1);

    printf("[test] DeinitRemovesAllTimers: pass\n");
    return 0;
} /* end of TestDeinitRemovesAllTimers() */

static const TestCase_type g_test_cases[] = {
    { "CreateTimerCreatesInactiveTimer", TestCreateTimerCreatesInactiveTimer },
    { "CreateTimerAllocatesRunningIds", TestCreateTimerAllocatesRunningIds },
    { "TimerIdWrapSkipsInvalidId", TestTimerIdWrapSkipsInvalidId },
    { "TimerIdWrapSkipsIdAlreadyInUse", TestTimerIdWrapSkipsIdAlreadyInUse },
    { "StartTimerMovesInactiveToActive", TestStartTimerMovesInactiveToActive },
    { "StartTimerRejectsZeroDuration", TestStartTimerRejectsZeroDuration },
    { "InvalidTimerIdRejectedByCommands", TestInvalidTimerIdRejectedByCommands },
    { "WrongOwnerRejectedByCommands", TestWrongOwnerRejectedByCommands },
    { "SortedDeltaListUsesScheduledExpiryDeltas", TestSortedDeltaListUsesScheduledExpiryDeltas },
    { "EqualExpiryTimersExpireTogether", TestEqualExpiryTimersExpireTogether },
    { "ProcessExpiredDoesNotExpireEarly", TestProcessExpiredDoesNotExpireEarly },
    { "ProcessExpiredMovesTimerBackInactive", TestProcessExpiredMovesTimerBackInactive },
    { "ProcessExpiredWithNullCallbackMovesTimerInactive", TestProcessExpiredWithNullCallbackMovesTimerInactive },
    { "ProcessExpiredMultipleStaggeredTimersInOrder", TestProcessExpiredMultipleStaggeredTimersInOrder },
    { "PartialExpiryLeavesLaterTimersActive", TestPartialExpiryLeavesLaterTimersActive },
    { "ExpiredTimerCanBeRestarted", TestExpiredTimerCanBeRestarted },
    { "RestartActiveTimerWithShorterDuration", TestRestartActiveTimerWithShorterDuration },
    { "RestartActiveTimerWithLongerDuration", TestRestartActiveTimerWithLongerDuration },
    { "StopActiveTimerMovesItInactive", TestStopActiveTimerMovesItInactive },
    { "StopMiddleActiveTimerPreservesFollowingDelta", TestStopMiddleActiveTimerPreservesFollowingDelta },
    { "StopInactiveTimerIsSuccessful", TestStopInactiveTimerIsSuccessful },
    { "DeleteInactiveTimerRemovesIt", TestDeleteInactiveTimerRemovesIt },
    { "DeleteActiveTimerRemovesItAndKeepsOthers", TestDeleteActiveTimerRemovesItAndKeepsOthers },
    { "DeleteMiddleActiveTimerPreservesFollowingDelta", TestDeleteMiddleActiveTimerPreservesFollowingDelta },
    { "GetNextWaitDurationWhenNoActiveTimer", TestGetNextWaitDurationWhenNoActiveTimer },
    { "RemoveTimersForPipeRemovesOnlyThatOwner", TestRemoveTimersForPipeRemovesOnlyThatOwner },
    { "RemoveTimersForPipePreservesRemainingActiveDeltas", TestRemoveTimersForPipePreservesRemainingActiveDeltas },
    { "ExpiryArithmeticSaturatesAtUint64Max", TestExpiryArithmeticSaturatesAtUint64Max },
    { "NearUint64MaxTimersRemainOrdered", TestNearUint64MaxTimersRemainOrdered },
    { "DeinitRemovesAllTimers", TestDeinitRemovesAllTimers }
};

static int RunOneTest(const char* name)
{
    size_t index;
    int result;

    for (index = 0; index < (sizeof(g_test_cases) / sizeof(g_test_cases[0])); ++index) {
        if (strcmp(name, g_test_cases[index].name) == 0) {
            result = g_test_cases[index].function();
            printf("\n\n");
            fflush(stdout);
            return result;
        }
    }

    printf("[test] unknown scenario: %s\n", name);
    printf("[test] available scenarios:\n");

    for (index = 0; index < (sizeof(g_test_cases) / sizeof(g_test_cases[0])); ++index) {
        printf("[test]   %s\n", g_test_cases[index].name);
    }

    printf("\n\n");
    fflush(stdout);
    return 1;
} /* end of RunOneTest() */

int main(int argc, char** argv)
{
    size_t index;
    int failed;

    if (argc == 2) {
        return RunOneTest(argv[1]);
    }

    printf("[test] vdrTimerManagerTests: begin all scenarios\n");
    failed = 0;

    for (index = 0; index < (sizeof(g_test_cases) / sizeof(g_test_cases[0])); ++index) {
        if (RunOneTest(g_test_cases[index].name) != 0) {
            failed = 1;
            break;
        }
    }

    if (failed != 0) {
        printf("[test] vdrTimerManagerTests: failed\n");
        return 1;
    }

    printf("[test] vdrTimerManagerTests: all scenarios passed\n");
    return 0;
} /* end of main() */
