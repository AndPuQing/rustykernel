mod debug_session;
mod kernel;
mod protocol;
mod worker;

pub use crate::kernel::{
    ChannelEndpoints, ConnectionInfo, KernelError, KernelRuntime, KernelRuntimeInfo, healthcheck,
    parse_connection_file, runtime_info, start_kernel, start_kernel_from_connection_file,
};

#[cfg(feature = "python")]
use pyo3::exceptions::{PyOSError, PyValueError};
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::PyModule;

#[cfg(feature = "python")]
#[pyfunction(name = "healthcheck")]
fn py_healthcheck() -> &'static str {
    healthcheck()
}

#[cfg(feature = "python")]
#[pyfunction(name = "parse_connection_file")]
fn py_parse_connection_file(path: &str) -> PyResult<ConnectionInfo> {
    parse_connection_file(path).map_err(map_kernel_error)
}

#[cfg(feature = "python")]
#[pyfunction(name = "runtime_info")]
fn py_runtime_info() -> KernelRuntimeInfo {
    runtime_info()
}

#[cfg(feature = "python")]
#[pyclass(unsendable)]
struct RunningKernel {
    connection: ConnectionInfo,
    endpoints: ChannelEndpoints,
    runtime: Option<KernelRuntime>,
}

#[cfg(feature = "python")]
#[pymethods]
impl RunningKernel {
    #[getter]
    fn connection(&self) -> ConnectionInfo {
        self.connection.clone()
    }

    #[getter]
    fn endpoints(&self) -> ChannelEndpoints {
        self.endpoints.clone()
    }

    #[getter]
    fn is_running(&self) -> bool {
        self.runtime.as_ref().is_some_and(KernelRuntime::is_running)
    }

    fn stop(&mut self) -> PyResult<()> {
        if let Some(runtime) = self.runtime.as_mut() {
            runtime.stop().map_err(map_kernel_error)?;
        }
        self.runtime = None;
        Ok(())
    }
}

#[cfg(feature = "python")]
#[pyfunction(name = "start_kernel")]
fn py_start_kernel(path: &str) -> PyResult<RunningKernel> {
    let runtime = start_kernel_from_connection_file(path).map_err(map_kernel_error)?;
    let connection = runtime.connection_info().clone();
    let endpoints = runtime.channel_endpoints().clone();

    Ok(RunningKernel {
        connection,
        endpoints,
        runtime: Some(runtime),
    })
}

#[cfg(feature = "python")]
fn map_kernel_error(error: KernelError) -> PyErr {
    match error {
        KernelError::Io(error) => PyOSError::new_err(error.to_string()),
        KernelError::InvalidConnectionConfig(message) => PyValueError::new_err(message),
        KernelError::InvalidConnectionFile(error) => {
            PyValueError::new_err(format!("invalid connection file JSON: {error}"))
        }
        KernelError::Worker(message) => PyOSError::new_err(message),
        KernelError::Protocol(error) => PyValueError::new_err(error.to_string()),
        KernelError::Zmq(error) => PyOSError::new_err(error.to_string()),
        KernelError::HeartbeatThreadPanicked => PyOSError::new_err("heartbeat thread panicked"),
        KernelError::MessageLoopThreadPanicked => {
            PyOSError::new_err("message loop thread panicked")
        }
    }
}

#[cfg(feature = "python")]
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<ChannelEndpoints>()?;
    m.add_class::<ConnectionInfo>()?;
    m.add_class::<KernelRuntimeInfo>()?;
    m.add_class::<RunningKernel>()?;
    m.add_function(wrap_pyfunction!(py_healthcheck, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_connection_file, m)?)?;
    m.add_function(wrap_pyfunction!(py_runtime_info, m)?)?;
    m.add_function(wrap_pyfunction!(py_start_kernel, m)?)?;
    Ok(())
}
