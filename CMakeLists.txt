cmake_minimum_required(VERSION 3.20)

project(vdrapkinTimerManager LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

set(VDR_TIMER_MANAGER_PUBLIC_HEADERS
    include/vdr_timer_manager_api.h
    include/vdr_timer_manager_protocol.h
)

set(VDR_TIMER_MANAGER_CORE_HEADERS
    src/timer_core/vdr_timer_core.h
)

set(VDR_TIMER_MANAGER_DOCUMENTS
    docs/vdrapkinTimerManager_Project_Manual.pdf
)

source_group("Header Files\\Public" FILES ${VDR_TIMER_MANAGER_PUBLIC_HEADERS})
source_group("Header Files\\Core" FILES ${VDR_TIMER_MANAGER_CORE_HEADERS})
source_group("Documentation" FILES ${VDR_TIMER_MANAGER_DOCUMENTS})
source_group("Source Files\\Client" FILES src/client/vdr_timer_manager_api.c)
source_group("Source Files\\Server" FILES src/server/vdrapkin_timer_manager_server.c)
source_group("Source Files\\Core" FILES src/timer_core/vdr_timer_core.c)
source_group("Source Files\\Samples" FILES samples/sample_client/sample_client.c)
source_group("Source Files\\Tests" FILES
    tests/timer_core/timer_core_tests.c
    tests/integration/client_api_integration_tests.c
    tests/robustness/robustness_tests.c
)

add_library(vdrTimerManagerCore STATIC
    src/timer_core/vdr_timer_core.c
    ${VDR_TIMER_MANAGER_CORE_HEADERS}
    include/vdr_timer_manager_protocol.h
)

target_include_directories(vdrTimerManagerCore
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/src/timer_core
)

add_library(vdrTimerManagerApi STATIC
    src/client/vdr_timer_manager_api.c
    include/vdr_timer_manager_api.h
    include/vdr_timer_manager_protocol.h
)

target_include_directories(vdrTimerManagerApi
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(vdrTimerManagerApi
    PUBLIC
        vdrTimerManagerCore
)

add_executable(vdrapkinTimerManager
    src/server/vdrapkin_timer_manager_server.c
    ${VDR_TIMER_MANAGER_CORE_HEADERS}
    include/vdr_timer_manager_protocol.h
)

target_link_libraries(vdrapkinTimerManager
    PRIVATE
        vdrTimerManagerCore
)

add_executable(vdrTimerManagerSampleClient
    samples/sample_client/sample_client.c
    ${VDR_TIMER_MANAGER_PUBLIC_HEADERS}
)

target_link_libraries(vdrTimerManagerSampleClient
    PRIVATE
        vdrTimerManagerApi
)

add_executable(vdrTimerManagerTests
    tests/timer_core/timer_core_tests.c
    ${VDR_TIMER_MANAGER_CORE_HEADERS}
    include/vdr_timer_manager_protocol.h
)

target_link_libraries(vdrTimerManagerTests
    PRIVATE
        vdrTimerManagerCore
)

add_executable(vdrTimerManagerIntegrationTests
    tests/integration/client_api_integration_tests.c
    ${VDR_TIMER_MANAGER_PUBLIC_HEADERS}
)

target_link_libraries(vdrTimerManagerIntegrationTests
    PRIVATE
        vdrTimerManagerApi
)

add_executable(vdrTimerManagerRobustnessTests
    tests/robustness/robustness_tests.c
    ${VDR_TIMER_MANAGER_PUBLIC_HEADERS}
)

target_link_libraries(vdrTimerManagerRobustnessTests
    PRIVATE
        vdrTimerManagerApi
)

add_custom_target(vdrTimerManagerDocumentation
    SOURCES
        ${VDR_TIMER_MANAGER_DOCUMENTS}
)

enable_testing()

set(VDR_TIMER_MANAGER_TEST_SCENARIOS
    CreateTimerCreatesInactiveTimer
    CreateTimerAllocatesRunningIds
    TimerIdWrapSkipsInvalidId
    TimerIdWrapSkipsIdAlreadyInUse
    StartTimerMovesInactiveToActive
    StartTimerRejectsZeroDuration
    InvalidTimerIdRejectedByCommands
    WrongOwnerRejectedByCommands
    SortedDeltaListUsesScheduledExpiryDeltas
    EqualExpiryTimersExpireTogether
    ProcessExpiredDoesNotExpireEarly
    ProcessExpiredMovesTimerBackInactive
    ProcessExpiredWithNullCallbackMovesTimerInactive
    ProcessExpiredMultipleStaggeredTimersInOrder
    PartialExpiryLeavesLaterTimersActive
    ExpiredTimerCanBeRestarted
    RestartActiveTimerWithShorterDuration
    RestartActiveTimerWithLongerDuration
    StopActiveTimerMovesItInactive
    StopMiddleActiveTimerPreservesFollowingDelta
    StopInactiveTimerIsSuccessful
    DeleteInactiveTimerRemovesIt
    DeleteActiveTimerRemovesItAndKeepsOthers
    DeleteMiddleActiveTimerPreservesFollowingDelta
    GetNextWaitDurationWhenNoActiveTimer
    RemoveTimersForPipeRemovesOnlyThatOwner
    RemoveTimersForPipePreservesRemainingActiveDeltas
    ExpiryArithmeticSaturatesAtUint64Max
    NearUint64MaxTimersRemainOrdered
    DeinitRemovesAllTimers
)

foreach(VDR_TIMER_MANAGER_TEST_SCENARIO IN LISTS VDR_TIMER_MANAGER_TEST_SCENARIOS)
    add_test(
        NAME vdrTimerManagerTests.${VDR_TIMER_MANAGER_TEST_SCENARIO}
        COMMAND vdrTimerManagerTests ${VDR_TIMER_MANAGER_TEST_SCENARIO}
    )
endforeach()

set(VDR_TIMER_MANAGER_INTEGRATION_TEST_SCENARIOS
    CreateDeleteRoundTrip
    StartExpireDeleteRoundTrip
    StopActivePreventsExpiry
    StopInactiveRoundTrip
    RestartActiveUsesNewDuration
    DeleteActivePreventsExpiry
    MultipleTimersExpireThroughQueue
    QueuedExpiriesKeepNotificationEventSignaled
    NegativeAckStatusesFromUserPerspective
)

foreach(VDR_TIMER_MANAGER_INTEGRATION_TEST_SCENARIO IN LISTS VDR_TIMER_MANAGER_INTEGRATION_TEST_SCENARIOS)
    add_test(
        NAME vdrTimerManagerIntegrationTests.${VDR_TIMER_MANAGER_INTEGRATION_TEST_SCENARIO}
        COMMAND vdrTimerManagerIntegrationTests ${VDR_TIMER_MANAGER_INTEGRATION_TEST_SCENARIO}
    )
endforeach()

set(VDR_TIMER_MANAGER_ROBUSTNESS_TEST_SCENARIOS
    MalformedMagicRejected
    MalformedVersionRejected
    MalformedTypeRejected
    MalformedTotalSizeRejected
    MalformedPayloadSizeRejected
    MalformedTruncatedHeaderRejected
    MalformedOversizedMessageRejected
    MalformedPayloadTooSmallRejected
    MalformedPayloadTooLargeRejected
    MalformedOldStartTimerSizeRejected
    MalformedAckSentToServerRejected
    MalformedExpirySentToServerRejected
    MaximumClientAndWaitHandleCapacity
    MaximumClientCapacityRecoversAfterDisconnect
    DisconnectCleanupRemovesClientTimers
    NegativeAckOwnershipAndRawDuration
    StartTimerAlreadyExpiredTimestampDeliversImmediateExpiry
    RestartActiveTimerAlreadyExpiredTimestampClearsOldSchedule
    ConcurrentPublicApiCalls
)

foreach(VDR_TIMER_MANAGER_ROBUSTNESS_TEST_SCENARIO IN LISTS VDR_TIMER_MANAGER_ROBUSTNESS_TEST_SCENARIOS)
    add_test(
        NAME vdrTimerManagerRobustnessTests.${VDR_TIMER_MANAGER_ROBUSTNESS_TEST_SCENARIO}
        COMMAND vdrTimerManagerRobustnessTests ${VDR_TIMER_MANAGER_ROBUSTNESS_TEST_SCENARIO}
    )
    set_tests_properties(
        vdrTimerManagerRobustnessTests.${VDR_TIMER_MANAGER_ROBUSTNESS_TEST_SCENARIO}
        PROPERTIES TIMEOUT 60
    )
endforeach()

add_test(
    NAME vdrTimerManagerSampleClient.SimultaneousResponseExpiries
    COMMAND vdrTimerManagerSampleClient --test-simultaneous-response-expiry
)
set_tests_properties(
    vdrTimerManagerSampleClient.SimultaneousResponseExpiries
    PROPERTIES TIMEOUT 45
)

if(MSVC)
    target_compile_options(vdrTimerManagerCore PRIVATE /W4 /WX)
    target_compile_options(vdrTimerManagerApi PRIVATE /W4 /WX)
    target_compile_options(vdrapkinTimerManager PRIVATE /W4 /WX)
    target_compile_options(vdrTimerManagerSampleClient PRIVATE /W4 /WX)
    target_compile_options(vdrTimerManagerTests PRIVATE /W4 /WX)
    target_compile_options(vdrTimerManagerIntegrationTests PRIVATE /W4 /WX)
    target_compile_options(vdrTimerManagerRobustnessTests PRIVATE /W4 /WX)
else()
    target_compile_options(vdrTimerManagerCore PRIVATE -Wall -Wextra -Werror)
    target_compile_options(vdrTimerManagerApi PRIVATE -Wall -Wextra -Werror)
    target_compile_options(vdrapkinTimerManager PRIVATE -Wall -Wextra -Werror)
    target_compile_options(vdrTimerManagerSampleClient PRIVATE -Wall -Wextra -Werror)
    target_compile_options(vdrTimerManagerTests PRIVATE -Wall -Wextra -Werror)
    target_compile_options(vdrTimerManagerIntegrationTests PRIVATE -Wall -Wextra -Werror)
    target_compile_options(vdrTimerManagerRobustnessTests PRIVATE -Wall -Wextra -Werror)
endif()
