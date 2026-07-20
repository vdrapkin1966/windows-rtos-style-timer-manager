/*
 * File:        vdr_timer_manager_protocol.h
 * Module:      Timer Manager Pipe Protocol
 * Author:      Vitaly Drapkin
 * Copyright:   Copyright (c) 2026 Vitaly Drapkin
 * Purpose:     Define the binary messages shared by clients and the server.
 *
 * Description:
 *   - Defines protocol constants, timer IDs, message types, and ACK statuses.
 *   - Defines fixed-size command, ACK, and expiry structures transferred through
 *     the named pipe in client-to-server and server-to-client unions.
 *
 * Dependencies:
 *   - stdint.h
 *
 * Notes:
 *   - Protocol structures use compiler-native alignment. Both endpoints must
 *     use the same protocol header, compiler ABI, and build settings.
 *   - Every message begins with vdrMessageHeader_type for common validation.
 *   - Both communication endpoints must use the same protocol version.
 */
#ifndef VDR_TIMER_MANAGER_PROTOCOL_H
#define VDR_TIMER_MANAGER_PROTOCOL_H

/*
 * Shared binary protocol for the vdr Timer Manager sample.
 *
 * The protocol intentionally uses fixed-size C-style structures. Each message
 * starts with vdrMessageHeader_type, and the first member of every directional
 * union is the header itself. That lets pipe code inspect message_type before
 * deciding which union member is valid.
 */

#include <stdint.h>

/* Maximum number of handles that can be passed to WaitForMultipleObjects().
 * The server reserves slots for pipe events, one waitable timer, and one
 * shutdown event. */
#define TIMER_SERVER_MAX_WAIT_HANDLES 64

/* Maximum number of connected clients. One additional wait slot is reserved
 * for the listener pipe instance. */
#define TIMER_SERVER_MAX_CLIENTS 61

/* Current fixed binary protocol version. Both client and server reject messages
 * whose header version does not match this value. */
#define TIMER_PROTOCOL_VERSION 2

/* Header guard value carried in every pipe message. "TMGR" lets the receiver
 * reject data that is not a Timer Manager protocol message. */
#define TIMER_PIPE_MAGIC 0x544D4752 /* "TMGR" */

/* Named pipe endpoint used by both the client API and the console server. */
#define PIPE_NAME L"\\\\.\\pipe\\vdrapkinTimerManager"

typedef uint64_t TimerId_type;

/* Timer ID zero is reserved as invalid. The server's running ID counter skips
 * this value if the counter wraps. */
#define TIMER_ID_INVALID ((TimerId_type)0)

typedef enum vdrMessageType_tag {
    /* Client asks the server to allocate one inactive timer. */
    vdrMESSAGE_TYPE_CREATE_TIMER_REQ = 1,

    /* Client asks the server to start or restart one existing timer. */
    vdrMESSAGE_TYPE_START_TIMER_REQ,

    /* Client asks the server to move one timer to inactive state. */
    vdrMESSAGE_TYPE_STOP_TIMER_REQ,

    /* Client asks the server to remove one timer completely. */
    vdrMESSAGE_TYPE_DELETE_TIMER_REQ,

    /* Server response for every command. */
    vdrMESSAGE_TYPE_ACK_RSP,

    /* Server asynchronous indication that one timer expired. */
    vdrMESSAGE_TYPE_TIMER_EXPIRED_EVT
} vdrMessageType_type;

typedef enum vdrAckStatus_tag {
    /* Command completed successfully. */
    vdrACK_STATUS_OK = 0,

    /* Timer ID is zero or not present under any owner. */
    vdrACK_STATUS_INVALID_TIMER_ID,

    /* StartTimer duration is zero or otherwise not accepted. */
    vdrACK_STATUS_INVALID_DURATION,

    /* Timer ID exists but belongs to a different client pipe. */
    vdrACK_STATUS_TIMER_NOT_OWNED,

    /* Reserved for future server capacity/back-pressure examples. */
    vdrACK_STATUS_SERVER_BUSY,

    /* Message header, type, size, or payload is invalid. */
    vdrACK_STATUS_PROTOCOL_ERROR,

    /* Allocation, Windows API, or unexpected internal failure. */
    vdrACK_STATUS_INTERNAL_ERROR,

    /* Client API could not connect to or continue communicating with server. */
    vdrACK_STATUS_SERVER_NOT_AVAILABLE,

    /* Client API expiry queue is empty. */
    vdrACK_STATUS_NO_EXPIRED_TIMERS
} vdrAckStatus_type;

typedef enum vdrPipeOperation_tag {
    /* Pipe instance is waiting for ConnectNamedPipe completion. */
    vdrPIPE_OPERATION_ACCEPT,

    /* Pipe instance is waiting for ReadFile completion. */
    vdrPIPE_OPERATION_READ,

    /* Pipe instance is performing a server-to-client write. */
    vdrPIPE_OPERATION_WRITE
} vdrPipeOperation_type;

typedef struct vdrMessageHeader_tag {
    /* Must be TIMER_PIPE_MAGIC for every protocol message. */
    uint32_t magic;

    /* Must be TIMER_PROTOCOL_VERSION for this sample implementation. */
    uint16_t version;

    /* One value from vdrMessageType_type. This selects the valid union member. */
    uint16_t message_type;

    /* Client-generated command ID. ACK responses echo the matching request ID.
     * Asynchronous timer-expiry events use request ID zero. */
    uint32_t request_id;

    /* Number of bytes after this header. Because all messages are fixed-size
     * structures, this is used for validation rather than variable raw data. */
    uint32_t payload_size;
} vdrMessageHeader_type;

typedef struct vdrCreateTimerReq_tag {
    /* Header identifies this message as vdrMESSAGE_TYPE_CREATE_TIMER_REQ. */
    vdrMessageHeader_type header;
} vdrCreateTimerReq_type;

typedef struct vdrStartTimerReq_tag {
    /* Header identifies this message as vdrMESSAGE_TYPE_START_TIMER_REQ. */
    vdrMessageHeader_type header;

    /* Existing timer to start or restart. */
    TimerId_type timer_id;

    /* One-shot duration in milliseconds. Zero is rejected. */
    uint64_t duration_ms;

    /* Monotonic timestamp captured by the client API immediately before the
     * StartTimer request is sent. The server uses this value to subtract
     * Windows scheduling and pipe-delivery delay from the requested duration. */
    uint64_t api_start_timestamp_ms;
} vdrStartTimerReq_type;

typedef struct vdrStopTimerReq_tag {
    /* Header identifies this message as vdrMESSAGE_TYPE_STOP_TIMER_REQ. */
    vdrMessageHeader_type header;

    /* Existing timer to stop. */
    TimerId_type timer_id;
} vdrStopTimerReq_type;

typedef struct vdrDeleteTimerReq_tag {
    /* Header identifies this message as vdrMESSAGE_TYPE_DELETE_TIMER_REQ. */
    vdrMessageHeader_type header;

    /* Existing timer to delete. */
    TimerId_type timer_id;
} vdrDeleteTimerReq_type;

typedef union vdrClientToServerMessage_tag {
    /* Common header view used before selecting a specific request member. */
    vdrMessageHeader_type header;

    /* CreateTimer command payload. */
    vdrCreateTimerReq_type create_timer_req;

    /* StartTimer command payload. */
    vdrStartTimerReq_type start_timer_req;

    /* StopTimer command payload. */
    vdrStopTimerReq_type stop_timer_req;

    /* DeleteTimer command payload. */
    vdrDeleteTimerReq_type delete_timer_req;
} vdrClientToServerMessage_type;

typedef struct vdrAckRsp_tag {
    /* Header identifies this message as vdrMESSAGE_TYPE_ACK_RSP. */
    vdrMessageHeader_type header;

    /* One value from vdrAckStatus_type. Kept as uint32_t on the wire. */
    uint32_t ack_status;

    /* Reserved for alignment/future protocol extension. Must be zero. */
    uint32_t reserved;

    /* Relevant timer ID when available. TIMER_ID_INVALID is used when no valid
     * timer ID can be returned, for example failed CreateTimer allocation. */
    TimerId_type timer_id;
} vdrAckRsp_type;

typedef struct vdrTimerExpiredEvt_tag {
    /* Header identifies this message as vdrMESSAGE_TYPE_TIMER_EXPIRED_EVT. */
    vdrMessageHeader_type header;

    /* Timer ID that expired and was moved back to inactive state by server. */
    TimerId_type timer_id;
} vdrTimerExpiredEvt_type;

typedef union vdrServerToClientMessage_tag {
    /* Common header view used before selecting ACK or expiry event. */
    vdrMessageHeader_type header;

    /* Generic positive/negative command ACK. */
    vdrAckRsp_type ack_rsp;

    /* Asynchronous timer expiry indication. */
    vdrTimerExpiredEvt_type timer_expired_evt;
} vdrServerToClientMessage_type;

static inline uint32_t vdrPayloadSize(uint32_t total_size)
{
    /* All protocol structures include the header as their first field. Payload
     * size is the structure size minus that common header. */
    return (uint32_t)(total_size - sizeof(vdrMessageHeader_type));
} /* end of vdrPayloadSize() */

#endif /* VDR_TIMER_MANAGER_PROTOCOL_H */
