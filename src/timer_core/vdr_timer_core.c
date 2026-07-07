/*
 * File:        vdr_timer_core.c
 * Module:      Timer Manager Core
 * Author:      Vitaly Drapkin
 * Copyright:   Copyright (c) 2026 Vitaly Drapkin
 * Purpose:     Maintain timer ownership, state, scheduling, and expiry order.
 *
 * Design Overview:
 *   - Stores allocated timers in active and inactive intrusive linked lists.
 *   - Maintains the active list in expiry order using delta time remainders.
 *   - Moves expired timers to the inactive list and invokes an optional callback.
 *
 * Concurrency:
 *   The core provides no internal synchronization. Its owning server thread
 *   must serialize all calls and list access.
 *
 * Assumptions:
 *   - Timers are one-shot and may be restarted after becoming inactive.
 *   - A server pipe handle uniquely identifies a timer's owning client.
 *
 * Revision History:
 *   - Initial implementation.
 */
#include "vdr_timer_core.h"

#include <assert.h>
#include <stdlib.h>

/*
 * The timer core keeps two intrusive singly-linked lists:
 *
 *   inactive_head: timers that exist but are not scheduled.
 *   active_head:   timers sorted by scheduled expiry time.
 *
 * For active timers, time_remainder_ms is a delta value. For the head it is the
 * remaining time from "now" to the head expiry. For every other node it is the
 * difference between the previous scheduled expiry and this scheduled expiry.
 */

/*
 * Validate an owner pipe handle stored in a timer entry.
 *
 * The server uses the pipe handle as the client ownership token. A timer with
 * a NULL or INVALID_HANDLE_VALUE owner cannot be associated with a real client,
 * so public timer-core entry points reject those handles before list logic is
 * allowed to run.
 */
static BOOL vdrIsInvalidOwnerPipe(HANDLE owner_pipe)
{
    return (owner_pipe == NULL) || (owner_pipe == INVALID_HANDLE_VALUE);
} /* end of vdrIsInvalidOwnerPipe() */

/*
 * Add two unsigned 64-bit values without wrapping.
 *
 * Timer expiry is calculated as start_time + duration. In a sample project this
 * should never realistically overflow, but saturation makes the helper robust:
 * an absurdly large duration becomes "very far in the future" instead of
 * wrapping around and expiring immediately.
 */
static uint64_t vdrSaturatingAddU64(uint64_t left, uint64_t right)
{
    if ((UINT64_MAX - left) < right) {
        return UINT64_MAX;
    }

    return left + right;
} /* end of vdrSaturatingAddU64() */

/*
 * Return the absolute scheduled expiry time for a timer.
 *
 * Parameter validation is intentionally simple because this is an internal
 * helper. Public functions validate user-facing inputs before reaching here.
 */
static uint64_t vdrTimerScheduledExpiry(const Timer_type* timer)
{
    assert(timer != NULL);
    return vdrSaturatingAddU64(timer->system_start_time_ms, timer->duration_ms);
} /* end of vdrTimerScheduledExpiry() */

/*
 * Search one timer list for a timer owned by a specific client.
 *
 * If detach is FALSE, the function is a pure lookup.
 * If detach is TRUE, the function removes the matching node from the list and
 * clears its next pointer. The caller then owns that detached node and must
 * either insert it into another list or free it.
 */
static Timer_type* vdrFindAndMaybeDetach(
    Timer_type** head,
    TimerId_type timer_id,
    HANDLE owner_pipe,
    BOOL detach)
{
    Timer_type* previous;
    Timer_type* current;

    assert(head != NULL);
    assert(timer_id != TIMER_ID_INVALID);
    assert(!vdrIsInvalidOwnerPipe(owner_pipe));

    previous = NULL;
    current = *head;

    /* Walk the intrusive singly-linked list while remembering the previous link
     * so a matching node can be removed in O(1) once found. */
    while (current != NULL) {
        if ((current->id == timer_id) && (current->server_pipe == owner_pipe)) {
            if (detach == TRUE) {
                /* Removing the head updates the caller's head pointer. Removing
                 * a middle/tail node links the previous node around current. */
                if (previous == NULL) {
                    *head = current->next_timer;
                } else {
                    previous->next_timer = current->next_timer;
                }

                current->next_timer = NULL;
            }

            return current;
        }

        previous = current;
        current = current->next_timer;
    }

    return NULL;
} /* end of vdrFindAndMaybeDetach() */

/*
 * Search one timer list by timer ID without checking ownership.
 *
 * This helper is used after an owner-specific lookup failed. If the timer ID is
 * present under a different owner, the user-visible error is TIMER_NOT_OWNED
 * instead of INVALID_TIMER_ID.
 */
static Timer_type* vdrFindByIdAnyOwner(
    Timer_type* head,
    TimerId_type timer_id)
{
    Timer_type* current;

    assert(timer_id != TIMER_ID_INVALID);

    current = head;

    while (current != NULL) {
        if (current->id == timer_id) {
            return current;
        }

        current = current->next_timer;
    }

    return NULL;
} /* end of vdrFindByIdAnyOwner() */

/*
 * Insert a timer at the front of a list.
 *
 * Inactive timers do not need sorted order, so the core uses push-front for
 * inactive insertions. Active insertions use vdrInsertActiveSorted instead.
 */
static void vdrListPushFront(Timer_type** head, Timer_type* timer)
{
    assert(head != NULL);
    assert(timer != NULL);

    timer->next_timer = *head;
    *head = timer;
} /* end of vdrListPushFront() */

/*
 * Rebuild all active-list delta values from absolute expiry times.
 *
 * This keeps the correctness easy to inspect. Instead of trying to patch one
 * neighbor's time_remainder in every insert/remove corner case, all active
 * entries are recalculated after a structural change. For this sample, the list
 * is expected to be short, and clarity is more important than micro-optimizing.
 */
static void vdrRecalculateActiveDeltas(vdrTimerCore_type* core, uint64_t now_ms)
{
    Timer_type* current;
    uint64_t previous_expiry;
    BOOL first;

    assert(core != NULL);

    current = core->active_head;
    previous_expiry = now_ms;
    first = TRUE;

    while (current != NULL) {
        const uint64_t current_expiry = vdrTimerScheduledExpiry(current);

        if (first == TRUE) {
            /* The head delta is relative to current system time because the
             * Windows waitable timer is always armed for the active head. */
            current->time_remainder_ms = (current_expiry > now_ms) ? (current_expiry - now_ms) : 0;
            first = FALSE;
        } else {
            /* Non-head deltas are relative to the preceding scheduled expiry,
             * exactly matching the RTOS delta-list definition in the design. */
            current->time_remainder_ms =
                (current_expiry > previous_expiry) ? (current_expiry - previous_expiry) : 0;
        }

        previous_expiry = current_expiry;
        current = current->next_timer;
    }
} /* end of vdrRecalculateActiveDeltas() */

/*
 * Insert a timer into the active list sorted by absolute scheduled expiry.
 *
 * Timers with equal expiry times are inserted after existing timers with the
 * same expiry. The subsequent delta recalculation will assign them a zero
 * time_remainder_ms, so they expire together when the head is processed.
 */
static void vdrInsertActiveSorted(vdrTimerCore_type* core, Timer_type* timer, uint64_t now_ms)
{
    Timer_type* previous;
    Timer_type* current;
    const uint64_t new_expiry = vdrTimerScheduledExpiry(timer);

    assert(core != NULL);
    assert(timer != NULL);

    previous = NULL;
    current = core->active_head;

    /* Keep walking while existing timers expire before or at the same time as
     * the new timer. Stopping only on strictly-less preserves FIFO-ish ordering
     * among equal-expiry timers. */
    while (current != NULL) {
        const uint64_t current_expiry = vdrTimerScheduledExpiry(current);

        if (new_expiry < current_expiry) {
            break;
        }

        previous = current;
        current = current->next_timer;
    }

    if (previous == NULL) {
        timer->next_timer = core->active_head;
        core->active_head = timer;
    } else {
        timer->next_timer = previous->next_timer;
        previous->next_timer = timer;
    }

    vdrRecalculateActiveDeltas(core, now_ms);
} /* end of vdrInsertActiveSorted() */

/*
 * Allocate a nonzero timer ID from the running counter.
 *
 * TIMER_ID_INVALID is permanently reserved. If the counter wraps, this function
 * skips the invalid value and also skips any currently active/inactive ID that
 * might still be in use.
 */
static TimerId_type vdrAllocateTimerId(vdrTimerCore_type* core)
{
    TimerId_type candidate;

    assert(core != NULL);

    do {
        /* Candidate is taken before incrementing so next_timer_id always points
         * at the next value to try on the following allocation. */
        candidate = core->next_timer_id;
        core->next_timer_id++;

        if (core->next_timer_id == TIMER_ID_INVALID) {
            core->next_timer_id++;
        }
    } while ((candidate == TIMER_ID_INVALID) || vdrTimerCore_TimerIdInUse(core, candidate));

    return candidate;
} /* end of vdrAllocateTimerId() */

/*
 * Initialize caller-provided timer-core storage.
 *
 * The caller owns the vdrTimerCore_type object itself. This function only
 * initializes list heads and the running timer-ID counter.
 */
void vdrTimerCore_Init(vdrTimerCore_type* core)
{
    assert(core != NULL);

    if (core == NULL) {
        return;
    }

    core->inactive_head = NULL;
    core->active_head = NULL;
    core->next_timer_id = 1;
} /* end of vdrTimerCore_Init() */

/*
 * Release every timer entry owned by the core and reset it to empty state.
 *
 * Passing INVALID_HANDLE_VALUE into RemoveTimersForPipe is the module's internal
 * convention for "remove all owners." External callers should remove a specific
 * client by passing that client's pipe handle.
 */
void vdrTimerCore_Deinit(vdrTimerCore_type* core)
{
    assert(core != NULL);

    if (core == NULL) {
        return;
    }

    vdrTimerCore_RemoveTimersForPipe(core, INVALID_HANDLE_VALUE);
    core->inactive_head = NULL;
    core->active_head = NULL;
    core->next_timer_id = 1;
} /* end of vdrTimerCore_Deinit() */

/*
 * Create a new logical timer for a client.
 *
 * The timer starts inactive. It does not consume the Windows waitable timer
 * until StartTimer moves it to the active list. On success the caller receives
 * the newly assigned nonzero timer ID.
 */
vdrAckStatus_type vdrTimerCore_CreateTimer(
    vdrTimerCore_type* core,
    HANDLE owner_pipe,
    TimerId_type* out_timer_id)
{
    Timer_type* timer;

    assert(core != NULL);
    assert(!vdrIsInvalidOwnerPipe(owner_pipe));
    assert(out_timer_id != NULL);

    if ((core == NULL) || vdrIsInvalidOwnerPipe(owner_pipe) || (out_timer_id == NULL)) {
        return vdrACK_STATUS_INTERNAL_ERROR;
    }

    /* Always initialize output parameters before work starts. This guarantees
     * callers never observe an uninitialized timer ID on failure. */
    *out_timer_id = TIMER_ID_INVALID;

    timer = (Timer_type*)malloc(sizeof(Timer_type));

    if (timer == NULL) {
        return vdrACK_STATUS_INTERNAL_ERROR;
    }

    ZeroMemory(timer, sizeof(Timer_type));
    timer->id = vdrAllocateTimerId(core);
    timer->server_pipe = owner_pipe;
    timer->duration_ms = 0;
    timer->system_start_time_ms = 0;
    timer->time_remainder_ms = 0;
    timer->next_timer = NULL;

    vdrListPushFront(&core->inactive_head, timer);
    *out_timer_id = timer->id;

    return vdrACK_STATUS_OK;
} /* end of vdrTimerCore_CreateTimer() */

/*
 * Start or restart a logical timer.
 *
 * If the timer is inactive, it is moved from inactive to active. If it is
 * already active, it is removed from its current active position and reinserted
 * with a new absolute expiry. That implements the agreed "StartTimer restarts
 * an active timer" behavior.
 */
vdrAckStatus_type vdrTimerCore_StartTimer(
    vdrTimerCore_type* core,
    HANDLE owner_pipe,
    TimerId_type timer_id,
    uint64_t duration_ms,
    uint64_t now_ms)
{
    Timer_type* timer;

    assert(core != NULL);
    assert(!vdrIsInvalidOwnerPipe(owner_pipe));

    if ((core == NULL) || vdrIsInvalidOwnerPipe(owner_pipe)) {
        return vdrACK_STATUS_INTERNAL_ERROR;
    }

    if (timer_id == TIMER_ID_INVALID) {
        return vdrACK_STATUS_INVALID_TIMER_ID;
    }

    if (duration_ms == 0) {
        return vdrACK_STATUS_INVALID_DURATION;
    }

    /* Prefer inactive lookup first because newly created and expired timers are
     * inactive. If not found there, try active list to support restart. */
    timer = vdrFindAndMaybeDetach(&core->inactive_head, timer_id, owner_pipe, TRUE);

    if (timer == NULL) {
        timer = vdrFindAndMaybeDetach(&core->active_head, timer_id, owner_pipe, TRUE);

        if (timer != NULL) {
            /* Removing an active timer changes neighboring deltas. Recalculate
             * before the timer is reinserted at its new expiry position. */
            vdrRecalculateActiveDeltas(core, now_ms);
        }
    }

    if (timer == NULL) {
        /* Distinguish "the ID exists, but belongs to another pipe" from "the ID
         * is unknown." That gives the server a more precise negative ACK. */
        if (vdrFindByIdAnyOwner(core->active_head, timer_id) ||
            vdrFindByIdAnyOwner(core->inactive_head, timer_id)) {
            return vdrACK_STATUS_TIMER_NOT_OWNED;
        }

        return vdrACK_STATUS_INVALID_TIMER_ID;
    }

    timer->duration_ms = duration_ms;
    timer->system_start_time_ms = now_ms;
    timer->time_remainder_ms = 0;
    timer->next_timer = NULL;

    vdrInsertActiveSorted(core, timer, now_ms);

    return vdrACK_STATUS_OK;
} /* end of vdrTimerCore_StartTimer() */

/*
 * Stop a logical timer.
 *
 * Stopping an active timer moves it back to inactive. Stopping an already
 * inactive timer is idempotent and returns OK, matching the agreed API behavior.
 */
vdrAckStatus_type vdrTimerCore_StopTimer(
    vdrTimerCore_type* core,
    HANDLE owner_pipe,
    TimerId_type timer_id,
    uint64_t now_ms)
{
    Timer_type* timer;

    assert(core != NULL);
    assert(!vdrIsInvalidOwnerPipe(owner_pipe));

    if ((core == NULL) || vdrIsInvalidOwnerPipe(owner_pipe)) {
        return vdrACK_STATUS_INTERNAL_ERROR;
    }

    if (timer_id == TIMER_ID_INVALID) {
        return vdrACK_STATUS_INVALID_TIMER_ID;
    }

    timer = vdrFindAndMaybeDetach(&core->active_head, timer_id, owner_pipe, TRUE);

    if (timer != NULL) {
        /* The active list just lost one node, so the remaining delta values must
         * be rebuilt relative to the supplied current time. */
        vdrRecalculateActiveDeltas(core, now_ms);
        timer->time_remainder_ms = 0;
        vdrListPushFront(&core->inactive_head, timer);
        return vdrACK_STATUS_OK;
    }

    timer = vdrFindAndMaybeDetach(&core->inactive_head, timer_id, owner_pipe, FALSE);

    if (timer != NULL) {
        return vdrACK_STATUS_OK;
    }

    if (vdrFindByIdAnyOwner(core->active_head, timer_id) ||
        vdrFindByIdAnyOwner(core->inactive_head, timer_id)) {
        return vdrACK_STATUS_TIMER_NOT_OWNED;
    }

    return vdrACK_STATUS_INVALID_TIMER_ID;
} /* end of vdrTimerCore_StopTimer() */

/*
 * Delete a logical timer from whichever list currently owns it.
 *
 * Delete is valid for active and inactive timers. Once removed, the timer entry
 * is freed and the timer ID becomes invalid for future commands.
 */
vdrAckStatus_type vdrTimerCore_DeleteTimer(
    vdrTimerCore_type* core,
    HANDLE owner_pipe,
    TimerId_type timer_id,
    uint64_t now_ms)
{
    Timer_type* timer;

    assert(core != NULL);
    assert(!vdrIsInvalidOwnerPipe(owner_pipe));

    if ((core == NULL) || vdrIsInvalidOwnerPipe(owner_pipe)) {
        return vdrACK_STATUS_INTERNAL_ERROR;
    }

    if (timer_id == TIMER_ID_INVALID) {
        return vdrACK_STATUS_INVALID_TIMER_ID;
    }

    timer = vdrFindAndMaybeDetach(&core->inactive_head, timer_id, owner_pipe, TRUE);

    if (timer == NULL) {
        timer = vdrFindAndMaybeDetach(&core->active_head, timer_id, owner_pipe, TRUE);

        if (timer != NULL) {
            /* Active deletion can affect the wait duration for the next head. */
            vdrRecalculateActiveDeltas(core, now_ms);
        }
    }

    if (timer == NULL) {
        if (vdrFindByIdAnyOwner(core->active_head, timer_id) ||
            vdrFindByIdAnyOwner(core->inactive_head, timer_id)) {
            return vdrACK_STATUS_TIMER_NOT_OWNED;
        }

        return vdrACK_STATUS_INVALID_TIMER_ID;
    }

    free(timer);
    return vdrACK_STATUS_OK;
} /* end of vdrTimerCore_DeleteTimer() */

/*
 * Move all timers that are due at now_ms back to inactive.
 *
 * The server calls this when the Windows waitable timer is signaled. If a
 * callback is supplied, each expired timer is reported after the timer entry is
 * already back in the inactive list.
 */
void vdrTimerCore_ProcessExpired(
    vdrTimerCore_type* core,
    uint64_t now_ms,
    vdrTimerCore_ExpiredCallback_type expired_callback)
{
    assert(core != NULL);

    if (core == NULL) {
        return;
    }

    while (core->active_head != NULL) {
        Timer_type* timer = core->active_head;
        const uint64_t expiry = vdrTimerScheduledExpiry(timer);
        TimerId_type expired_timer_id;
        HANDLE expired_server_pipe;

        if (expiry > now_ms) {
            break;
        }

        /* The active list is sorted by expiry, so once the head is not due, no
         * following timer can be due either. Every due head is detached. */
        core->active_head = timer->next_timer;
        timer->next_timer = NULL;

        expired_timer_id = timer->id;
        expired_server_pipe = timer->server_pipe;

        /* Expired one-shot timers become inactive. Higher-level restart policy
         * belongs to the client application, not the server. */
        timer->time_remainder_ms = 0;
        vdrListPushFront(&core->inactive_head, timer);

        if (expired_callback != NULL) {
            /* Notify after the timer is already inactive so external callback
             * logic observes a consistent timer-core state. */
            expired_callback(expired_timer_id, expired_server_pipe);
        }
    }

    /* If any active timers remain, rebuild their deltas from current time so
     * the server can arm the waitable timer for the new head. */
    vdrRecalculateActiveDeltas(core, now_ms);
} /* end of vdrTimerCore_ProcessExpired() */

/*
 * Remove all timers associated with a client pipe.
 *
 * The server uses this on client disconnect. The special owner value
 * INVALID_HANDLE_VALUE is used internally by Deinit to remove every timer
 * regardless of owner.
 */
void vdrTimerCore_RemoveTimersForPipe(vdrTimerCore_type* core, HANDLE owner_pipe)
{
    Timer_type** lists[2];
    uint32_t list_index;
    BOOL active_list_changed;

    assert(core != NULL);

    if (core == NULL) {
        return;
    }

    lists[0] = &core->inactive_head;
    lists[1] = &core->active_head;
    active_list_changed = FALSE;

    for (list_index = 0; list_index < 2; ++list_index) {
        Timer_type** link = lists[list_index];

        /* Link-to-link iteration lets us remove nodes without a separate
         * previous pointer and without special-casing head removal. */
        while (*link != NULL) {
            Timer_type* current = *link;

            if ((owner_pipe == INVALID_HANDLE_VALUE) || (current->server_pipe == owner_pipe)) {
                *link = current->next_timer;

                if (lists[list_index] == &core->active_head) {
                    /* Removing an active timer during disconnect changes the
                     * documented delta-list relationship for any timers after
                     * it. Remember that the active list needs rebuilding after
                     * all matching nodes have been freed. */
                    active_list_changed = TRUE;
                }

                free(current);
            } else {
                link = &current->next_timer;
            }
        }
    }

    if (active_list_changed == TRUE) {
        /* Disconnect cleanup does not know the current scheduler timestamp.
         * Rebuilding from zero preserves the scheduled-expiry delta invariant
         * between remaining active timers; the server later asks
         * GetNextWaitDuration() with the real current time before arming the
         * waitable timer. */
        vdrRecalculateActiveDeltas(core, 0);
    }
} /* end of vdrTimerCore_RemoveTimersForPipe() */

/*
 * Return the duration for which the server should arm the waitable timer.
 *
 * FALSE means there are no active timers. TRUE means out_duration_ms contains
 * the remaining time for the active-list head; zero means the head is already
 * due and the server should process expiry promptly.
 */
BOOL vdrTimerCore_GetNextWaitDuration(
    vdrTimerCore_type* core,
    uint64_t now_ms,
    uint64_t* out_duration_ms)
{
    Timer_type* head;
    uint64_t expiry;

    assert(core != NULL);
    assert(out_duration_ms != NULL);

    if ((core == NULL) || (out_duration_ms == NULL)) {
        return FALSE;
    }

    head = core->active_head;

    if (head == NULL) {
        *out_duration_ms = 0;
        return FALSE;
    }

    expiry = vdrTimerScheduledExpiry(head);
    *out_duration_ms = (expiry > now_ms) ? (expiry - now_ms) : 0;
    head->time_remainder_ms = *out_duration_ms;

    return TRUE;
} /* end of vdrTimerCore_GetNextWaitDuration() */

/*
 * Test whether a timer ID exists in either active or inactive list.
 *
 * This is used by the ID allocator and unit tests. It does not validate owner,
 * because the caller is asking about global ID use.
 */
BOOL vdrTimerCore_TimerIdInUse(vdrTimerCore_type* core, TimerId_type timer_id)
{
    assert(core != NULL);

    if ((core == NULL) || (timer_id == TIMER_ID_INVALID)) {
        return FALSE;
    }

    return (vdrFindByIdAnyOwner(core->active_head, timer_id) != NULL) ||
           (vdrFindByIdAnyOwner(core->inactive_head, timer_id) != NULL);
} /* end of vdrTimerCore_TimerIdInUse() */

/*
 * Count active timers for tests and diagnostics.
 */
uint32_t vdrTimerCore_CountActive(vdrTimerCore_type* core)
{
    Timer_type* current;
    uint32_t count;

    assert(core != NULL);

    if (core == NULL) {
        return 0;
    }

    count = 0;
    current = core->active_head;

    while (current != NULL) {
        ++count;
        current = current->next_timer;
    }

    return count;
} /* end of vdrTimerCore_CountActive() */

/*
 * Count inactive timers for tests and diagnostics.
 */
uint32_t vdrTimerCore_CountInactive(vdrTimerCore_type* core)
{
    Timer_type* current;
    uint32_t count;

    assert(core != NULL);

    if (core == NULL) {
        return 0;
    }

    count = 0;
    current = core->inactive_head;

    while (current != NULL) {
        ++count;
        current = current->next_timer;
    }

    return count;
} /* end of vdrTimerCore_CountInactive() */
