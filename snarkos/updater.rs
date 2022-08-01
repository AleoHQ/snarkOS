// Copyright (C) 2019-2022 Aleo Systems Inc.
// This file is part of the snarkOS library.

// The snarkOS library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// The snarkOS library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with the snarkOS library. If not, see <https://www.gnu.org/licenses/>.

#[cfg(feature = "auto-update")]
use anyhow::anyhow;
use colored::Colorize;
use self_update::{backends::github, version::bump_is_greater, Status};
use std::fmt::Write;
#[cfg(feature = "auto-update")]
use std::{collections::HashMap, str};
#[cfg(feature = "auto-update")]
use tokio::process::Command;

const SNARKOS_BIN_NAME: &str = "snarkos";
const SNARKOS_REPO_NAME: &str = "snarkOS";
const SNARKOS_REPO_OWNER: &str = "AleoHQ";

#[cfg(feature = "auto-update")]
const SNARKOS_CURRENT_BRANCH: &str = "testnet3";
#[cfg(feature = "auto-update")]
pub(crate) const AUTO_UPDATE_INTERVAL_SECS: u64 = 60 * 15; // 15 minutes

#[cfg(feature = "auto-update")]
pub struct AutoUpdater {
    reqwest_client: reqwest::Client,
    pub latest_sha: String,
}

pub struct Updater;

#[cfg(feature = "auto-update")]
impl AutoUpdater {
    pub async fn get_build_sha() -> anyhow::Result<String> {
        let local_repo_path = env!("CARGO_MANIFEST_DIR");
        let local_repo_sha_bytes = Command::new("git")
            .args(&["rev-parse", "HEAD"])
            .current_dir(local_repo_path)
            .output()
            .await?
            .stdout;
        let local_repo_sha = str::from_utf8(&local_repo_sha_bytes)?.trim_end();

        debug!("[auto-updater]: The local repo SHA is {}", local_repo_sha);

        Ok(local_repo_sha.into())
    }

    pub async fn new() -> anyhow::Result<Self> {
        let reqwest_client = reqwest::Client::builder().user_agent("curl").build()?;
        let latest_sha = Self::get_build_sha().await?;

        Ok(Self {
            reqwest_client,
            latest_sha,
        })
    }

    pub async fn check_for_updates(&mut self) -> anyhow::Result<bool> {
        let check_endpoint = format!(
            "https://api.github.com/repos/{}/{}/commits/{}",
            SNARKOS_REPO_OWNER, SNARKOS_REPO_NAME, SNARKOS_CURRENT_BRANCH
        );
        let req = self.reqwest_client.get(check_endpoint).build()?;
        let remote_repo_sha = self
            .reqwest_client
            .execute(req)
            .await?
            .json::<HashMap<String, serde_json::Value>>()
            .await?
            .remove("sha")
            .and_then(|v| v.as_str().map(|s| s.to_owned()))
            .ok_or_else(|| anyhow!("GitHub's API didn't return the latest SHA"))?;

        debug!("[auto-updater]: The remote repo SHA is {}", remote_repo_sha);

        if self.latest_sha != remote_repo_sha {
            self.latest_sha = remote_repo_sha;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn update_local_repo(&self) -> anyhow::Result<()> {
        let local_repo_path = env!("CARGO_MANIFEST_DIR");
        let repo_addr = format!("https://github.com/{}/{}", SNARKOS_REPO_OWNER, SNARKOS_REPO_NAME);

        Command::new("git")
            .args(&["pull", &repo_addr, SNARKOS_CURRENT_BRANCH])
            .current_dir(local_repo_path)
            .output()
            .await?;

        debug!("[auto-updater]: Updated the local repo");

        Ok(())
    }

    pub async fn rebuild_local_repo(&self) -> anyhow::Result<()> {
        let local_repo_path = env!("CARGO_MANIFEST_DIR");

        info!("[auto-updater]: Rebuilding the local repo...");

        Command::new("cargo")
            .args(&["build", "--release"])
            .current_dir(local_repo_path)
            .output()
            .await?;

        info!("[auto-updater]: Rebuilt the local repo; your snarkOS client is now up to date");

        Ok(())
    }

    #[cfg(target_family = "unix")]
    pub fn restart(&self) -> anyhow::Result<()> {
        use std::{env, os::unix::process::CommandExt, path::PathBuf};

        let mut binary_path: PathBuf = env!("CARGO_MANIFEST_DIR").parse().unwrap();
        binary_path.push("target");
        binary_path.push("release");

        let original_args = env::args()
            .skip(1)
            .flat_map(|arg| arg.split('=').map(|s| s.to_owned()).collect::<Vec<_>>())
            .collect::<Vec<String>>();

        std::process::Command::new("cargo")
            .args(&["run", "--release", "--"])
            .args(&original_args)
            .current_dir(binary_path)
            .exec();

        Ok(())
    }
}

impl Updater {
    /// Show all available releases for `snarkos`.
    pub fn show_available_releases() -> Result<String, UpdaterError> {
        let releases = github::ReleaseList::configure()
            .repo_owner(SNARKOS_REPO_OWNER)
            .repo_name(SNARKOS_REPO_NAME)
            .build()?
            .fetch()?;

        let mut output = "List of available versions\n".to_string();
        for release in releases {
            let _ = writeln!(output, "  * {}", release.version);
        }
        Ok(output)
    }

    /// Update `snarkOS` to the specified release.
    pub fn update_to_release(show_output: bool, version: Option<String>) -> Result<Status, UpdaterError> {
        let mut update_builder = github::Update::configure();

        update_builder
            .repo_owner(SNARKOS_REPO_OWNER)
            .repo_name(SNARKOS_REPO_NAME)
            .bin_name(SNARKOS_BIN_NAME)
            .current_version(env!("CARGO_PKG_VERSION"))
            .show_download_progress(show_output)
            .no_confirm(true)
            .show_output(show_output);

        let status = match version {
            None => update_builder.build()?.update()?,
            Some(v) => update_builder.target_version_tag(&v).build()?.update()?,
        };

        Ok(status)
    }

    /// Check if there is an available update for `aleo` and return the newest release.
    pub fn update_available() -> Result<String, UpdaterError> {
        let updater = github::Update::configure()
            .repo_owner(SNARKOS_REPO_OWNER)
            .repo_name(SNARKOS_REPO_NAME)
            .bin_name(SNARKOS_BIN_NAME)
            .current_version(env!("CARGO_PKG_VERSION"))
            .build()?;

        let current_version = updater.current_version();
        let latest_release = updater.get_latest_release()?;

        if bump_is_greater(&current_version, &latest_release.version)? {
            Ok(latest_release.version)
        } else {
            Err(UpdaterError::OldReleaseVersion(current_version, latest_release.version))
        }
    }

    /// Display the CLI message.
    pub fn print_cli() -> String {
        if let Ok(latest_version) = Self::update_available() {
            let mut output = "🟢 A new version is available! Run".bold().green().to_string();
            output += &" `aleo update` ".bold().white();
            output += &format!("to update to v{}.", latest_version).bold().green();
            output
        } else {
            String::new()
        }
    }
}

#[derive(Debug, Error)]
pub enum UpdaterError {
    #[error("{}: {}", _0, _1)]
    Crate(&'static str, String),

    #[error("The current version {} is more recent than the release version {}", _0, _1)]
    OldReleaseVersion(String, String),
}

impl From<self_update::errors::Error> for UpdaterError {
    fn from(error: self_update::errors::Error) -> Self {
        UpdaterError::Crate("self_update", error.to_string())
    }
}
