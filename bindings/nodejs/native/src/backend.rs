use lfs_sqlite::{Error, Result};
use lfs_storage::{OpenDalTransport, Options, S3Credentials, WebdavCredentials};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub(crate) enum Backend {
    Fs {
        root: String,
    },
    Webdav {
        endpoint: String,
        credentials: Credentials,
        #[serde(default)]
        allow_loopback_http: bool,
    },
    S3 {
        endpoint: String,
        region: String,
        bucket: String,
        credentials: S3Snapshot,
        #[serde(default)]
        allow_loopback_http: bool,
    },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct S3Snapshot {
    access_key_id: String,
    secret_access_key: String,
    #[serde(default, deserialize_with = "present_token")]
    session_token: Option<String>,
}
fn present_token<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    String::deserialize(deserializer).map(Some)
}
#[derive(Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub(crate) enum Credentials {
    Basic { username: String, password: String },
    Bearer { token: String },
}
fn bounded(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 8192 {
        return Err(Error::Invalid("backend"));
    }
    Ok(())
}
impl Backend {
    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Fs { root } => {
                crate::safe_path(root)?;
                if !std::path::Path::new(root).is_absolute() {
                    return Err(Error::Invalid("root"));
                }
            }
            Self::Webdav { .. } | Self::S3 { .. } => {
                self.transport()?;
            }
        }
        Ok(())
    }
    pub(crate) fn transport(&self) -> Result<OpenDalTransport> {
        match self {
            Self::Fs { root } => {
                OpenDalTransport::fs(root, Options::default()).map_err(Error::Network)
            }
            Self::S3 {
                endpoint,
                region,
                bucket,
                credentials,
                allow_loopback_http,
            } => {
                for value in [
                    endpoint,
                    region,
                    bucket,
                    &credentials.access_key_id,
                    &credentials.secret_access_key,
                ] {
                    bounded(value)?;
                }
                if let Some(token) = &credentials.session_token {
                    bounded(token)?;
                }
                OpenDalTransport::s3(
                    endpoint,
                    region,
                    bucket,
                    S3Credentials {
                        access_key_id: credentials.access_key_id.clone(),
                        secret_access_key: credentials.secret_access_key.clone(),
                        session_token: credentials.session_token.clone(),
                    },
                    Options {
                        allow_loopback_http: *allow_loopback_http,
                        ..Options::default()
                    },
                )
                .map_err(Error::Network)
            }
            Self::Webdav {
                endpoint,
                credentials,
                allow_loopback_http,
            } => {
                bounded(endpoint)?;
                let credentials = match credentials {
                    Credentials::Bearer { token } => {
                        bounded(token)?;
                        let body = token.trim_end_matches('=');
                        if body.is_empty()
                            || !body
                                .bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b"-._~+/".contains(&b))
                        {
                            return Err(Error::Invalid("backend"));
                        }
                        WebdavCredentials::Bearer(token.clone())
                    }
                    Credentials::Basic { username, password } => {
                        bounded(username)?;
                        bounded(password)?;
                        if username.contains(':') {
                            return Err(Error::Invalid("backend"));
                        }
                        WebdavCredentials::Basic {
                            username: username.clone(),
                            password: password.clone(),
                        }
                    }
                };
                OpenDalTransport::webdav(
                    endpoint,
                    credentials,
                    Options {
                        allow_loopback_http: *allow_loopback_http,
                        ..Options::default()
                    },
                )
                .map_err(Error::Network)
            }
        }
    }
}
