use s3util_rs::Config;

pub fn is_progress_indicator_needed(config: &Config) -> bool {
    if !config.show_progress {
        return false;
    }

    if config.tracing_config.is_none() {
        return true;
    }

    if log::Level::Warn < config.tracing_config.as_ref().unwrap().tracing_level {
        return false;
    }

    !config.tracing_config.as_ref().unwrap().json_tracing
}

pub fn is_show_result_needed(config: &Config) -> bool {
    if !config.show_progress {
        return false;
    }

    if config.tracing_config.is_none() {
        return true;
    }

    !config.tracing_config.as_ref().unwrap().json_tracing
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use s3util_rs::config::args::{Cli, Commands};

    use super::*;

    fn cp_args_from(cli: Cli) -> s3util_rs::config::args::CpArgs {
        match cli.command {
            Commands::Cp(cp_args) => cp_args,
            Commands::CreateBucket(_)
            | Commands::DeleteBucket(_)
            | Commands::DeleteObjectTagging(_)
            | Commands::GetBucketVersioning(_)
            | Commands::GetObjectTagging(_)
            | Commands::HeadBucket(_)
            | Commands::HeadObject(_)
            | Commands::Mv(_)
            | Commands::PutBucketVersioning(_)
            | Commands::PutObjectTagging(_)
            | Commands::Rm(_) => {
                panic!("expected Cp variant")
            }
        }
    }

    #[test]
    fn is_progress_indicator_needed_json_tracing() {
        init_dummy_tracing_subscriber();

        let cli = Cli::try_parse_from([
            "s3util",
            "cp",
            "--source-profile",
            "p",
            "--json-tracing",
            "--show-progress",
            "s3://source-bucket",
            "/target-dir",
        ])
        .unwrap();
        let config = Config::try_from(cp_args_from(cli)).unwrap();
        assert!(!is_progress_indicator_needed(&config));
    }

    #[test]
    fn is_progress_indicator_needed_no_json_tracing() {
        init_dummy_tracing_subscriber();

        let cli = Cli::try_parse_from([
            "s3util",
            "cp",
            "--source-profile",
            "p",
            "--show-progress",
            "s3://source-bucket",
            "/target-dir",
        ])
        .unwrap();
        let config = Config::try_from(cp_args_from(cli)).unwrap();
        assert!(is_progress_indicator_needed(&config));
    }

    #[test]
    fn is_progress_indicator_needed_no_tracing_config() {
        init_dummy_tracing_subscriber();

        let cli = Cli::try_parse_from([
            "s3util",
            "cp",
            "--source-profile",
            "p",
            "-qqq",
            "--show-progress",
            "s3://source-bucket",
            "/target-dir",
        ])
        .unwrap();
        let config = Config::try_from(cp_args_from(cli)).unwrap();
        assert!(is_progress_indicator_needed(&config));
    }

    #[test]
    fn is_progress_indicator_needed_default() {
        init_dummy_tracing_subscriber();

        let cli = Cli::try_parse_from([
            "s3util",
            "cp",
            "--source-profile",
            "p",
            "s3://source-bucket",
            "/target-dir",
        ])
        .unwrap();
        let config = Config::try_from(cp_args_from(cli)).unwrap();
        assert!(!is_progress_indicator_needed(&config));
    }

    #[test]
    fn is_progress_indicator_needed_info() {
        init_dummy_tracing_subscriber();

        let cli = Cli::try_parse_from([
            "s3util",
            "cp",
            "-v",
            "--source-profile",
            "p",
            "--show-progress",
            "s3://source-bucket",
            "/target-dir",
        ])
        .unwrap();
        let config = Config::try_from(cp_args_from(cli)).unwrap();
        assert!(!is_progress_indicator_needed(&config));
    }

    #[test]
    fn is_show_result_needed_default() {
        init_dummy_tracing_subscriber();

        let cli = Cli::try_parse_from([
            "s3util",
            "cp",
            "--source-profile",
            "p",
            "s3://source-bucket",
            "/target-dir",
        ])
        .unwrap();
        let config = Config::try_from(cp_args_from(cli)).unwrap();
        assert!(!is_show_result_needed(&config));
    }

    #[test]
    fn is_show_result_needed_silent() {
        init_dummy_tracing_subscriber();

        let cli = Cli::try_parse_from([
            "s3util",
            "cp",
            "-qqq",
            "--source-profile",
            "p",
            "--show-progress",
            "s3://source-bucket",
            "/target-dir",
        ])
        .unwrap();
        let config = Config::try_from(cp_args_from(cli)).unwrap();
        assert!(is_show_result_needed(&config));
    }

    #[test]
    fn is_show_result_needed_json_tracing() {
        init_dummy_tracing_subscriber();

        let cli = Cli::try_parse_from([
            "s3util",
            "cp",
            "-v",
            "--source-profile",
            "p",
            "--json-tracing",
            "--show-progress",
            "s3://source-bucket",
            "/target-dir",
        ])
        .unwrap();
        let config = Config::try_from(cp_args_from(cli)).unwrap();
        assert!(!is_show_result_needed(&config));
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
