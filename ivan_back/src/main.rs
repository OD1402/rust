#[allow(unused_imports)]
use anyhow::{anyhow, bail, Context, Error, Result};
#[allow(unused_imports)]
use tracing::{debug, error, info, span, trace, warn, Level};

pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
use built_info::*;

use ivan_back::*;

use clap::{Parser, Subcommand};
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Workdir where to read .env and config, relative to current dir
    #[arg(short, long)]
    pub workdir: Option<std::path::PathBuf>,

    /// Config, relative to current dir (or workdir if it is set)
    #[arg(short, long)]
    pub config: Option<std::path::PathBuf>,

    /// Test config
    #[arg(short, long)]
    pub test_config: bool,

    /// No show opts
    #[arg(short, long)]
    pub no_show_opts: bool,

    #[command(subcommand)]
    pub cmd: Option<Command>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    Server {
        #[arg(short, long)]
        port: Option<u16>,

        #[arg(long)]
        op_mode: Option<op_mode::OpMode>,
    },
}

use common_macros4::*;
declare_env_settings_for_server! {
    config_path Option: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    if let Some((_base_dir, cli)) = get_base_dir_and_cli!(cli: Cli, {
        if let Some(config) = cli.config.clone() {
            config
        } else if let Some(config_path) = env_settings!(config_path).clone() {
            config_path
        } else {
            std::path::PathBuf::from("config")
        }
    })? {
        if let Some(cmd) = cli.cmd.clone() {
            match cmd {
                Command::Server { port, op_mode } => {
                    let op_mode = op_mode::OpMode::get_actual(&op_mode);
                    EnvSettings::set_port(port, op_mode);
                    let port = EnvSettings::port(op_mode);
                    server(port, op_mode, PKG_NAME, PKG_VERSION).await?;
                }
            }
        }
    }

    Ok(())
}
