use super::*;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = PKG_NAME)]
pub struct Opt {
    /// Workdir where to read .env
    #[structopt(short, long, parse(from_os_str))]
    pub workdir: Option<std::path::PathBuf>,

    /// Test config
    #[structopt(short, long)]
    pub test_config: bool,

    /// No show opts
    #[structopt(short, long)]
    pub no_show_opts: bool,

    #[structopt(subcommand)]
    pub cmd: Option<Command>,
}

lazy_static::lazy_static! {
    pub static ref OPT: std::sync::RwLock<Option<Opt>> = std::sync::RwLock::new(None);
    pub static ref PARAMS: std::sync::RwLock<Option<Params>> = std::sync::RwLock::new(None);
}

pub struct Params {
    pub run_dir: std::path::PathBuf,
}

pub async fn main_helper() -> Result<()> {
    let opt = Opt::from_args();
    *PARAMS.write().unwrap() = Some(Params {
        run_dir: std::env::current_dir()?,
    });
    if let Some(workdir) = &opt.workdir {
        std::env::set_current_dir(workdir)
            .map_err(|err| anyhow!("failed to set {:?} for current dir: {}", opt.workdir, err))?;
    }
    dotenv::dotenv().context("file .env")?;
    pretty_env_logger::init_timed();
    if !opt.no_show_opts {
        info!(
            "{} {}\ncurrent dir: {:?}\nenv_settings: {:#?}",
            built_info::PKG_NAME,
            built_info::PKG_VERSION,
            std::env::current_dir().unwrap(),
            *(ENV_SETTINGS.read().unwrap())
        );
    }
    load_settings()?;
    if !opt.no_show_opts {
        info!(
            "settings from {:?}:\n{:#?}",
            std::path::PathBuf::from(&env_settings!(settings_toml_path)),
            (*SETTINGS.read().unwrap()).as_ref().unwrap().content
        );
        info!("opt: {:#?}", opt);
    }
    if opt.test_config {
        return Ok(());
    }
    *(OPT.write().unwrap()) = Some(opt);
    pasitos::pasos::run().await?;
    Ok(())
}
