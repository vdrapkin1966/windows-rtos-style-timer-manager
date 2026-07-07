/*
 * File:        vdr_timer_manager_api.h
 * Module:      Timer Manager Client API
 * Author:      Vitaly Drapkin
 * Copyright:   Copyright (c) 2026 Vitaly Drapkin
 * Purpose:     Declare the application-facing timer management interface.
 *
 * Description:
 *   - Declares synchronous APIs for creating, starting, stopping, and deleting
 *     server-managed one-shot timers.
 *   - Declares asynchronous expiry access through a shared notification event
 *     and an expired-timer ID queue.
 *
 * Public APIs:
 *   - vdrTimerManager_CreateTimer() - Create a new inactive timer.
 *   - vdrTimerManager_StartTimer() - Start or restart a one-shot timer.
 *   - vdrTimerManager_StopTimer() - Stop an active timer.
 *   - vdrTimerManager_DeleteTimer() - Delete an existing timer.
 *   - vdrTimerManager_GetNotificationEvent() - Return the expiry event handle.
 *   - vdrTimerManager_GetExpiredTimer() - Retrieve one expired timer ID.
 *
 * Dependencies:
 *   - windows.h
 *   - vdr_timer_manager_protocol.h
 *
 * Notes:
 *   - Public API calls are thread-safe and serialized internally as required.
 *   - vdrTimerManager_CreateTimer() must be the first public API call.
 *   - Event handles returned to the application remain owned by the API.
 */
#ifndef VDR_TIMER_MANAGER_API_H
#define VDR_TIMER_MANAGER_API_H

/*
 * Public client API for the vdr Timer Manager sample.
 *
 * There is no public initialize/deinitialize API. The first legal public call
 * is vdrTimerManager_CreateTimer(), which performs lazy client-side setup and
 * connects to an already-running server.
 *
 * Command APIs are synchronous from the application's point of view: each call
 * sends one command through the named pipe and waits for the matching ACK.
 * Timer expiry is asynchronous: the API receiver thread queues expired timer
 * IDs and signals one shared notification event.
 *
 * Every prototype below is declared with extern intentionally. These are
 * function declarations, not definitions, and the comments document the API
 * contract at the declaration site.
 */

#include <windows.h>
#include "vdr_timer_manager_protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Create a new inactive timer on the server.
 *
 * This is the first legal API call. It performs lazy client API initialization,
 * connects to the already-running server, sends CreateTimer, waits for ACK, and
 * returns the server-assigned timer ID through out_timer_id.
 *
 * out_timer_id must be a valid pointer. On failure, the API leaves any
 * successfully initialized client-side state available for later cleanup.
 */
extern vdrAckStatus_type vdrTimerManager_CreateTimer(TimerId_type* out_timer_id);

/*
 * Start or restart an existing one-shot timer.
 *
 * duration_ms must be nonzero. If the timer is already active, the server
 * restarts it with the new duration and returns a positive or negative ACK.
 * Duration is expressed in milliseconds.
 */
extern vdrAckStatus_type vdrTimerManager_StartTimer(TimerId_type timer_id, uint64_t duration_ms);

/*
 * Stop an existing timer.
 *
 * Stopping an active timer moves it to the inactive list. Stopping an already
 * inactive timer is treated as a successful idempotent operation.
 */
extern vdrAckStatus_type vdrTimerManager_StopTimer(TimerId_type timer_id);

/*
 * Delete an existing timer from either active or inactive state.
 *
 * After successful deletion, the timer ID is no longer valid for subsequent
 * StartTimer, StopTimer, or DeleteTimer calls.
 */
extern vdrAckStatus_type vdrTimerManager_DeleteTimer(TimerId_type timer_id);

/*
 * Return the manual-reset notification event used for timer expiry indication.
 *
 * The returned HANDLE is owned by the API. The application may wait on it with
 * WaitForMultipleObjects(), but must not close it.
 *
 * This is one shared event per client API instance, not one event per timer.
 * When it is signaled, at least one expired timer ID is queued and should be
 * retrieved with vdrTimerManager_GetExpiredTimer().
 */
extern vdrAckStatus_type vdrTimerManager_GetNotificationEvent(HANDLE* out_event);

/*
 * Retrieve one expired timer ID from the client API expiry queue.
 *
 * Returns vdrACK_STATUS_OK when a timer ID is returned. Returns
 * vdrACK_STATUS_NO_EXPIRED_TIMERS when the queue is empty.
 *
 * The notification event remains signaled while the queue is non-empty and is
 * reset by the API after the last queued expiry is removed.
 */
extern vdrAckStatus_type vdrTimerManager_GetExpiredTimer(TimerId_type* out_timer_id);

#ifdef __cplusplus
}

#endif

#endif /* VDR_TIMER_MANAGER_API_H */
