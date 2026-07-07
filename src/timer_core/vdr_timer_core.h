/*
 * File:        vdr_timer_core.h
 * Module:      Timer Manager Core
 * Author:      Vitaly Drapkin
 * Copyright:   Copyright (c) 2026 Vitaly Drapkin
 * Purpose:     Declare logical timer storage and scheduling operations.
 *
 * Description:
 *   - Defines active and inactive timer-list structures and timer ownership data.
 *   - Declares creation, lifecycle, expiry, cleanup, and diagnostic operations
 *     used by the Timer Manager server and timer-core unit tests.
 *
 * Dependencies:
 *   - windows.h
 *   - vdr_timer_manager_protocol.h
 *
 * Notes:
 *   - Timer-core functions are not internally synchronized; the caller must
 *     serialize access to each vdrTimerCore_type instance.
 *   - The caller owns vdrTimerCore_type storage, while the core allocates and
 *     releases individual Timer_type entries.
 *   - Expiry callbacks execute synchronously on the calling thread.
 */
#ifndef VDR_TIMER_CORE_H
#define VDR_TIMER_CORE_H

/*
 * C-style timer-list core.
 *
 * This module does not own a Windows waitable timer and does not perform pipe
 * I/O. It owns logical timer entries and maintains inactive and active lists.
 * The server supplies current time in milliseconds whenever list scheduling
 * decisions are required.
 */

#include <windows.h>
#include "vdr_timer_manager_protocol.h"

typedef struct Timer_tag {
    /* Server-assigned logical timer ID. Zero is never valid. */
    TimerId_type id;

    /* Owning client pipe. All command operations verify this owner. */
    HANDLE server_pipe;

    /* Last requested one-shot duration in milliseconds. */
    uint64_t duration_ms;

    /* System time, in milliseconds, when the current active period started. */
    uint64_t system_start_time_ms;

    /* Delta-list value. For the active head, this is the delay from now to the
     * head expiry. For later active timers, this is the delay from the previous
     * active timer's scheduled expiry to this timer's scheduled expiry. */
    uint64_t time_remainder_ms;

    /* Intrusive link used by both active and inactive lists. */
    struct Timer_tag* next_timer;
} Timer_type;

typedef struct vdrTimerCore_tag {
    /* Timers that exist but are not currently counting down. */
    Timer_type* inactive_head;

    /* Timers sorted by scheduled expiry. time_remainder_ms values form the
     * active delta list. */
    Timer_type* active_head;

    /* Running ID counter. TIMER_ID_INVALID is skipped if the counter wraps. */
    TimerId_type next_timer_id;
} vdrTimerCore_type;

/*
 * Optional notification callback used while processing expired timers.
 *
 * The timer core remains independent from named-pipe protocol logic. When this
 * callback is NULL, expired timers are only moved from active to inactive. When
 * it is not NULL, the core calls it once for each expired timer after the timer
 * has been returned to the inactive list.
 */
typedef void (*vdrTimerCore_ExpiredCallback_type)(TimerId_type timer_id, HANDLE server_pipe);

/*
 * Initialize caller-owned timer core state.
 *
 * This clears both lists and starts the running timer ID counter at the first
 * valid nonzero timer ID.
 */
extern void vdrTimerCore_Init(vdrTimerCore_type* core);

/*
 * Release all timer entries owned by the timer core.
 *
 * This function frees active and inactive timers and resets the core to an
 * empty state.
 */
extern void vdrTimerCore_Deinit(vdrTimerCore_type* core);

/*
 * Create a new inactive timer owned by owner_pipe.
 *
 * On success, out_timer_id receives the newly allocated nonzero timer ID.
 * The new timer is inserted into the inactive list.
 */
extern vdrAckStatus_type vdrTimerCore_CreateTimer(vdrTimerCore_type* core, HANDLE owner_pipe, TimerId_type* out_timer_id);

/*
 * Start or restart a timer owned by owner_pipe.
 *
 * The timer is inserted into the active list according to its scheduled expiry
 * time. If the timer is already active, it is removed and reinserted with the
 * new duration.
 *
 * now_ms is supplied by the server so the core does not depend on any Windows
 * clock API directly.
 */
extern vdrAckStatus_type vdrTimerCore_StartTimer(vdrTimerCore_type* core, HANDLE owner_pipe, TimerId_type timer_id, uint64_t duration_ms, uint64_t now_ms);

/*
 * Stop a timer owned by owner_pipe.
 *
 * Active timers are moved back to the inactive list. Already inactive timers
 * return success.
 *
 * now_ms is used to recalculate active-list deltas after structural changes.
 */
extern vdrAckStatus_type vdrTimerCore_StopTimer(vdrTimerCore_type* core, HANDLE owner_pipe, TimerId_type timer_id, uint64_t now_ms);

/*
 * Delete a timer owned by owner_pipe.
 *
 * The timer may be active or inactive. A successful delete frees the timer
 * entry and invalidates the timer ID.
 *
 * now_ms is used to recalculate active-list deltas when an active timer is
 * removed.
 */
extern vdrAckStatus_type vdrTimerCore_DeleteTimer(vdrTimerCore_type* core, HANDLE owner_pipe, TimerId_type timer_id, uint64_t now_ms);

/*
 * Process timers whose scheduled expiry time is due at now_ms.
 *
 * Expired timer entries are moved back to inactive state. If expired_callback
 * is not NULL, it is called once per expired timer with the timer ID and owner
 * pipe that should receive the asynchronous expiry indication.
 */
extern void vdrTimerCore_ProcessExpired(vdrTimerCore_type* core, uint64_t now_ms, vdrTimerCore_ExpiredCallback_type expired_callback);

/*
 * Remove all timers owned by owner_pipe.
 *
 * The server calls this during client disconnect cleanup. Passing
 * INVALID_HANDLE_VALUE is the internal convention used to remove all timers.
 * This operation frees the removed timer entries.
 */
extern void vdrTimerCore_RemoveTimersForPipe(vdrTimerCore_type* core, HANDLE owner_pipe);

/*
 * Return the wait duration for the current active-list head.
 *
 * Returns TRUE when an active timer exists and writes the duration to
 * out_duration_ms. Returns FALSE when no active timer exists.
 */
extern BOOL vdrTimerCore_GetNextWaitDuration(vdrTimerCore_type* core, uint64_t now_ms, uint64_t* out_duration_ms);

/*
 * Check whether a timer ID exists in either active or inactive list.
 *
 * Ownership is not checked; this function answers only global ID presence.
 */
extern BOOL vdrTimerCore_TimerIdInUse(vdrTimerCore_type* core, TimerId_type timer_id);

/*
 * Count active timers.
 *
 * This helper is primarily used by unit tests and diagnostics.
 */
extern uint32_t vdrTimerCore_CountActive(vdrTimerCore_type* core);

/*
 * Count inactive timers.
 *
 * This helper is primarily used by unit tests and diagnostics.
 */
extern uint32_t vdrTimerCore_CountInactive(vdrTimerCore_type* core);

#endif /* VDR_TIMER_CORE_H */
