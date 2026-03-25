mod kernel;
mod protocol;

pub use crate::kernel::{
    ConnectionInfo, KernelError, KernelRuntimeInfo, healthcheck, parse_connection_file,
    runtime_info,
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
fn map_kernel_error(error: KernelError) -> PyErr {
    match error {
        KernelError::Io(error) => PyOSError::new_err(error.to_string()),
        KernelError::InvalidConnectionConfig(message) => PyValueError::new_err(message),
        KernelError::InvalidConnectionFile(error) => {
            PyValueError::new_err(format!("invalid connection file JSON: {error}"))
        }
    }
}

#[cfg(feature = "python")]
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<ConnectionInfo>()?;
    m.add_class::<KernelRuntimeInfo>()?;
    m.add_function(wrap_pyfunction!(py_healthcheck, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_connection_file, m)?)?;
    m.add_function(wrap_pyfunction!(py_runtime_info, m)?)?;
    Ok(())
}
