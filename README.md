# vdrapkinTimerManager

Reference implementation of an asynchronous RTOS-style timer service for Windows written in C11.

## What Is This?

`vdrapkinTimerManager` is a Windows C11 sample project that implements a console-based timer manager server and a client API library. An application links with the client API, creates one-shot timers, starts or restarts them with millisecond durations, and receives timer expiry notifications through an event handle that can be used with `WaitForMultipleObjects()`.

The server owns all timer state. Client applications communicate with it through the named pipe `\\.\pipe\vdrapkinTimerManager`. Each client command receives one ACK response, and timer expiry is delivered asynchronously as a server-to-client indication.

This repository is the C version of the project. The server is intentionally built as a console application, not as a Windows service, so the implementation can be studied, debugged, and tested directly.

## Why Did You Build It?

The project was developed as a reference implementation of an RTOS-style timer service hosted on Windows. It is not intended to be a production or commercial timer service. The goal is to demonstrate how a relatively small set of C data structures, Windows synchronization objects, named-pipe messages, and defensive API rules can be combined into a complete client/server timer system.

The design deliberately keeps the implementation explicit. Timers are dynamically allocated with `malloc()` and released with `free()`. Active and inactive timers are stored in linked lists. Timer IDs are generated from a running counter. Public API calls validate parameters before executing internal logic. The implementation emphasizes readability through clear structure, consistent naming, and focused comments that explain design intent, control flow, state transitions, and Windows API interactions.

This project also served as a practical evaluation of an AI-assisted development workflow using OpenAI Codex. Requirements and architectural decisions were developed through iterative discussions before implementation. Throughout the project, Codex was used as an engineering assistant to explore design alternatives, review implementation ideas, suggest test scenarios, refine documentation, and help organize the repository. Final architectural decisions, implementation, verification, and technical content remained the responsibility of the project author. The resulting repository demonstrates both the design and implementation of the timer service and a disciplined engineering workflow in which AI is used to assist—but not replace—engineering judgment.

## What Engineering Problems Does It Demonstrate?

This project demonstrates several practical engineering problems that appear in asynchronous systems:

- A server-owned timer model where clients request timer operations but do not directly own timer internals.
- One-shot timer lifecycle management: create, start, restart, stop, expire, and delete.
- Sorted active-list scheduling using delta values between scheduled expiry times.
- Correct behavior when active timers are removed from the head, middle, or tail of the active list.
- Asynchronous named-pipe communication using overlapped I/O and event completion.
- A single server wait loop based on `WaitForMultipleObjects()` for pipe activity, waitable-timer expiry, and shutdown.
- Event-based expiry notification on the client side, including queuing multiple expired timer IDs behind one notification event.
- Synchronous command/ACK behavior layered on top of asynchronous pipe reception.
- Client disconnect cleanup, including removal of all timers owned by the disconnected pipe.
- Defensive binary protocol validation for magic, version, message type, total size, and payload size.
- Recovery after maximum-client and wait-handle capacity edge cases.
- Public API thread safety through internal serialization and synchronization.
- Regression testing of sunny-day behavior, malformed protocol messages, ownership errors, timer arithmetic near `UINT64_MAX`, and concurrent API use.

## What Technologies Were Used?

The implementation uses:

- C11.
- Windows API.
- Visual Studio 2022.
- CMake.
- Duplex message-mode named pipes.
- Overlapped I/O with event completion.
- `WaitForMultipleObjects()`.
- Windows waitable timers.
- Windows events, mutexes, and critical sections where appropriate.
- CTest-based regression tests.
- Warning-as-error builds: `/W4 /WX` with MSVC, or `-Wall -Wextra -Werror` with non-MSVC compilers.

## How Do I Build It?

Open a Visual Studio Developer PowerShell, then run the following commands from this repository root:

```powershell
cmake -S . -B build -G "Visual Studio 17 2022" -A x64
cmake --build build --config Debug
```

The main Debug executables are created under:

```text
build\Debug\vdrapkinTimerManager.exe
build\Debug\vdrTimerManagerSampleClient.exe
build\Debug\vdrTimerManagerTests.exe
build\Debug\vdrTimerManagerIntegrationTests.exe
build\Debug\vdrTimerManagerRobustnessTests.exe
```

To run the sample manually, start the server first in one console:

```powershell
.\build\Debug\vdrapkinTimerManager.exe
```

Then start the sample client in another console:

```powershell
.\build\Debug\vdrTimerManagerSampleClient.exe
```

The sample client runs a small simulated connection state machine. It creates timers, starts random-duration guard and response timers, waits on the timer-manager notification event, prints message transmissions and state transitions, and repeats the cycle.

To run the full regression suite, start the server first because the integration, robustness, and sample self-test cases connect to `\\.\pipe\vdrapkinTimerManager`:

```powershell
.\build\Debug\vdrapkinTimerManager.exe
```

Then run the tests from another Visual Studio Developer PowerShell in the repository root:

```powershell
ctest --test-dir build -C Debug --output-on-failure
```

The timer-core unit tests do not require the server, but the full regression test suite does.

## What Is In The Repository?

```text
include\
  vdr_timer_manager_api.h        Public client API declarations.
  vdr_timer_manager_protocol.h   Shared command, ACK, event, status, and protocol definitions.

src\
  client\
  vdr_timer_manager_api.c        Client-side API, named-pipe connection, ACK wait path, receiver thread, and expiry queue.

  server\
  vdrapkin_timer_manager_server.c Console server, named-pipe wait loop, command dispatch, timer expiry handling, and cleanup.

  timer_core\
  vdr_timer_core.c               Timer-list ownership, scheduling, restart, stop, delete, and expiry logic.
  vdr_timer_core.h               Internal timer-core declarations.

  common\
  .gitkeep                       Reserved for future shared source files.

samples\sample_client\
  sample_client.c                Example application using the public API and WaitForMultipleObjects-style expiry handling.

tests\
  timer_core\
  timer_core_tests.c             Timer-core unit tests.

  integration\
  client_api_integration_tests.c Client/API/server integration tests.

  robustness\
  robustness_tests.c             Protocol, capacity, disconnect, ownership, and concurrency robustness tests.

  sample\
  .gitkeep                       Reserved for future sample-level tests.

docs\
  vdrapkinTimerManager_Project_Manual.pdf
                                 PDF copy of the full project manual.
  
  architecture\
  system_architecture.png        High-level application, API, server, protocol, and timer-core view.
  client_server_protocol.png     Client/server command, ACK, and expiry-event message flow.
  timer_core_delta_list.png      Active-list delta scheduling model.
  server_wait_set.png            Server WaitForMultipleObjects wait-set model.
  sample_state_machine.png       Sample application's simulated connection state machine.
  timer_lifecycle.png            Timer create, start, restart, stop, expiry, and delete lifecycle.

  screenshots\
  sample_application.png         Sample-client state-machine output.
  server_console.png             Server startup and request-processing output.
  tests.png                      CTest regression-test output.
  build.png                      Visual Studio CMake build output.

tools\
  .gitkeep                       Reserved for optional helper scripts.

CMakeLists.txt                   Build, target, warning, and CTest configuration.
LICENSE                          Project license terms.
README.md                        This overview.
```

The full project manual is available as `docs\vdrapkinTimerManager_Project_Manual.pdf`. It contains the more detailed design narrative, protocol tables, server and client behavior, testing description, figures, and representative execution traces.
