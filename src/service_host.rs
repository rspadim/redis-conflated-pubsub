//! Windows service host.
//!
//! A process the service control manager started must speak the service
//! protocol: register a control handler, report `Running`, and answer `Stop`.
//! A program that does not is left waiting by the manager, which then fails the
//! start with error 1053 ("did not respond to the start request in time") — and
//! that is what a plain console program does.
//!
//! The relay therefore asks the dispatcher first and falls back to the console
//! path when the process was not started by the manager, so the same executable
//! is a service under `sc.exe` and a normal program everywhere else.

use std::ffi::OsString;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use windows_service::define_windows_service;
use windows_service::service::{
    ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus, ServiceType,
};
use windows_service::service_control_handler::{
    self, ServiceControlHandlerResult, ServiceStatusHandle,
};
use windows_service::service_dispatcher;

/// The name the installer registers (`{#ServiceName}` in
/// installer/redis-conflated-pubsub.iss). The service main also receives the
/// registered name from the manager; this is only a fallback.
pub const SERVICE_NAME: &str = "redis-conflated-pubsub";

/// `ERROR_FAILED_SERVICE_CONTROLLER_CONNECT`: the process was not started by the
/// service control manager, so it is a console run.
const NOT_STARTED_BY_SCM: i32 = 1063;

define_windows_service!(ffi_service_main, service_main);

/// What `main` should do after asking the dispatcher.
pub enum Dispatch {
    /// The process was started by the manager and has now stopped.
    RanAsService,
    /// The process was not started by the manager: run as a console program.
    NotAService,
}

/// Hands the process to the service dispatcher. Blocks while the service runs,
/// and returns immediately when the process is not a service.
pub fn dispatch() -> Dispatch {
    match service_dispatcher::start(SERVICE_NAME, ffi_service_main) {
        Ok(()) => Dispatch::RanAsService,
        Err(error) if is_not_started_by_scm(&error) => Dispatch::NotAService,
        Err(error) => {
            eprintln!("error: failed to talk to the service control manager: {error}");
            std::process::exit(1);
        }
    }
}

fn is_not_started_by_scm(error: &windows_service::Error) -> bool {
    matches!(error, windows_service::Error::Winapi(io) if io.raw_os_error() == Some(NOT_STARTED_BY_SCM))
}

fn service_main(arguments: Vec<OsString>) {
    let name = arguments
        .first()
        .map(|value| value.to_string_lossy().into_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| SERVICE_NAME.to_owned());

    if let Err(error) = run(&name) {
        // A service has no console to print to: the manager records the exit code
        // and the relay's own log file holds whatever it managed to write.
        eprintln!("service error: {error}");
    }
}

fn run(name: &str) -> windows_service::Result<()> {
    let (stop_sender, stop_receiver) = tokio::sync::watch::channel(false);
    let status_slot: Arc<OnceLock<ServiceStatusHandle>> = Arc::new(OnceLock::new());

    let handler = {
        let status_slot = Arc::clone(&status_slot);
        move |control: ServiceControl| -> ServiceControlHandlerResult {
            match control {
                ServiceControl::Stop | ServiceControl::Shutdown => {
                    // The manager allows a limited time to stop, so say the stop is
                    // under way before the drain begins.
                    if let Some(handle) = status_slot.get() {
                        let _ = handle.set_service_status(status(
                            ServiceState::StopPending,
                            1,
                            Duration::from_secs(30),
                        ));
                    }
                    let _ = stop_sender.send(true);
                    ServiceControlHandlerResult::NoError
                }
                // Every service must accept Interrogate, even as a no-op.
                ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                _ => ServiceControlHandlerResult::NotImplemented,
            }
        }
    };

    let status_handle = service_control_handler::register(name, handler)?;
    let _ = status_slot.set(status_handle);

    // Running before anything is connected. The relay retries a missing Redis
    // forever, so a service that waited for a connection would never answer the
    // start request in time and every start would fail with 1053.
    status_handle.set_service_status(status(ServiceState::Running, 0, Duration::default()))?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name_fn(crate::runtime_worker_name)
        .build()
        .expect("failed to initialize Tokio runtime");

    let mut final_status = status(ServiceState::Stopped, 0, Duration::default());
    final_status.exit_code = match runtime.block_on(crate::run(Some(stop_receiver))) {
        Ok(()) => ServiceExitCode::Win32(0),
        Err(error) => {
            eprintln!("error: {error:#}");
            ServiceExitCode::Win32(1)
        }
    };
    status_handle.set_service_status(final_status)?;
    Ok(())
}

fn status(state: ServiceState, checkpoint: u32, wait_hint: Duration) -> ServiceStatus {
    ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: state,
        controls_accepted: if state == ServiceState::Running {
            ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN
        } else {
            ServiceControlAccept::empty()
        },
        exit_code: ServiceExitCode::Win32(0),
        checkpoint,
        wait_hint,
        process_id: None,
    }
}
