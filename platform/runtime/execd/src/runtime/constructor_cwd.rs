//! Constructor-only filesystem initialization; command cwd overrides stay strict.
use std::io;

pub(crate) const ENV: &str = "ADX_SANDBOX_CWD";

pub(crate) fn initialize_from_environment() -> io::Result<()> {
    let value = std::env::var_os(ENV);
    let cwd = value
        .as_deref()
        .map(|value| {
            value.to_str().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "sandbox cwd must be valid UTF-8",
                )
            })
        })
        .transpose()?;
    initialize(cwd)
}

pub(crate) fn initialize(cwd: Option<&str>) -> io::Result<()> {
    let Some(cwd) = cwd else {
        return Ok(());
    };
    adx_core::sandbox::validate_cwd(cwd)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    std::fs::create_dir_all(cwd).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("initialize sandbox cwd {cwd:?}: {error}"),
        )
    })
}
