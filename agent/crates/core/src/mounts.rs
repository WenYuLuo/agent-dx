//! Template-local host mounts. Parameters carry no user or storage lifecycle semantics.
use crate::ValidationResult;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub type Parameters = BTreeMap<String, String>;
const MAX_PATH: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MountType {
    Bind,
}

/// Agent templates currently expose Local bind mounts; Platform also supports other sources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mount {
    #[serde(rename = "type")]
    pub kind: MountType,
    pub host_path: String,
    pub target: String,
    #[serde(default)]
    pub options: Vec<String>,
    /// Create a missing Local source directory on the execution node.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub create_if_missing: bool,
}

pub(crate) fn parameter_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
}

fn relative_path(path: &str) -> ValidationResult {
    if path.is_empty()
        || path.len() > MAX_PATH
        || path
            .chars()
            .any(|c| c.is_control() || matches!(c, '\\' | '$' | '{' | '}'))
        || path.split('/').any(|part| matches!(part, "" | "." | ".."))
    {
        return Err(
            "mount paths require nonempty components without traversal or expansion".into(),
        );
    }
    Ok(())
}

fn absolute_path(path: &str) -> ValidationResult {
    if path.len() > MAX_PATH {
        return Err("mount path too long".into());
    }
    relative_path(
        path.strip_prefix('/')
            .ok_or("mount path must be absolute")?,
    )
}

/// Parameters are bounded strings. Each template field validates its rendered value.
pub fn validate_parameters(parameters: &Parameters) -> ValidationResult {
    if parameters.len() > 64 {
        return Err("at most 64 binding parameters are allowed".into());
    }
    for (key, value) in parameters {
        if !parameter_name(key) {
            return Err("invalid binding parameter name".into());
        }
        if value.len() > 8192 || value.contains('\0') {
            return Err("invalid parameter value".into());
        }
    }
    Ok(())
}

impl Mount {
    fn render_path(&self, parameters: Option<&Parameters>) -> Result<String, String> {
        let mut remaining = self.host_path.as_str();
        let mut output = String::new();
        if let Some(first) = remaining.find("${") {
            let prefix = &remaining[..first];
            let root_end = prefix
                .rfind('/')
                .ok_or("mount requires a fixed absolute root")?;
            absolute_path(&prefix[..root_end])?;
        }
        while let Some(start) = remaining.find("${") {
            let literal = &remaining[..start];
            if literal.contains(['$', '{', '}']) {
                return Err("invalid mount placeholder".into());
            }
            output.push_str(literal);
            let rest = &remaining[start + 2..];
            let end = rest.find('}').ok_or("unterminated mount placeholder")?;
            let name = &rest[..end];
            if !parameter_name(name) {
                return Err("invalid mount parameter name".into());
            }
            output.push_str(match parameters {
                Some(parameters) => {
                    let value = parameters
                        .get(name)
                        .ok_or_else(|| format!("missing mount parameter: {name}"))?;
                    relative_path(value)?;
                    value
                }
                None => "parameter",
            });
            remaining = &rest[end + 1..];
        }
        if remaining.contains(['$', '{', '}']) {
            return Err("invalid mount placeholder".into());
        }
        output.push_str(remaining);
        absolute_path(&output)?;
        Ok(output)
    }

    fn validate_options(&self) -> ValidationResult {
        absolute_path(&self.target)?;
        let mut seen = BTreeSet::new();
        for option in &self.options {
            if !matches!(option.as_str(), "ro" | "rw" | "nosuid" | "nodev" | "noexec")
                || !seen.insert(option.as_str())
            {
                return Err("unsupported or duplicate Agent mount option".into());
            }
        }
        if seen.contains("ro") && seen.contains("rw") {
            return Err("mount cannot be both read-only and read-write".into());
        }
        Ok(())
    }

    /// Validate syntax at template publication without resolving parameters.
    pub fn validate_template(&self) -> ValidationResult {
        if self.host_path.len() > MAX_PATH {
            return Err("mount path too long".into());
        }
        self.validate_options()?;
        self.render_path(None).map(|_| ())
    }

    /// Execution specs must contain only concrete paths, never unresolved placeholders.
    pub fn validate(&self) -> ValidationResult {
        self.validate_options()?;
        absolute_path(&self.host_path)
    }

    /// Missing parameters or invalid paths fail before any Sandbox side effect.
    pub fn render(&self, parameters: &Parameters) -> Result<Self, String> {
        self.validate_template()?;
        validate_parameters(parameters)?;
        self.render_validated(parameters)
    }

    pub(crate) fn render_validated(&self, parameters: &Parameters) -> Result<Self, String> {
        let mut result = self.clone();
        result.host_path = self.render_path(Some(parameters))?;
        result.validate()?;
        Ok(result)
    }
}

pub fn validate_mounts(mounts: &[Mount]) -> ValidationResult {
    if mounts.len() > 32 {
        return Err("at most 32 Agent mounts are allowed".into());
    }
    let mut targets = BTreeSet::new();
    for mount in mounts {
        mount.validate_template()?;
        if !targets.insert(&mount.target) {
            return Err("duplicate mount target".into());
        }
    }
    Ok(())
}
