#[allow(unused_imports)]
use anyhow::{anyhow, bail, Context, Error, Result};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

macro_rules! declare_fn_get_last_but_one_and_last_extension_file_path(
    (. $extension_last_but_one:ident . $extension_last:ident) => { paste::paste! {
        fn [< get_ $extension_last_but_one _ $extension_last _filepath >](output_filepath: std::path::PathBuf) -> std::path::PathBuf {
            let stem = output_filepath
                .file_stem()
                .map(|stem| std::path::Path::new(stem));
            match (
                stem.and_then(|stem| stem.file_stem()),
                stem.and_then(|stem| stem.extension())
                    .and_then(|os_str| os_str.to_str()),
                output_filepath
                    .extension()
                    .and_then(|os_str| os_str.to_str()),
            ) {
                (_, Some(stringify!($extension_last_but_one)), Some(stringify!($extension_last))) => output_filepath,
                (file_name, extension, Some(stringify!($extension_last_but_one) | stringify!($extension_last))) => {
                    crate::declare_fn_get_last_but_one_and_last_extension_file_path::declare_fn_get_last_but_one_and_last_extension_file_path!(.$extension_last_but_one.$extension_last => output_filepath, file_name, extension)
                }
                (file_name, extension1, extension2) => {
                    crate::declare_fn_get_last_but_one_and_last_extension_file_path::declare_fn_get_last_but_one_and_last_extension_file_path!(.$extension_last_but_one.$extension_last => output_filepath, file_name, extension1, extension2)
                }
            }
        }
    } };
    (
        . $extension_last_but_one:ident . $extension_last:ident
        =>
        $output_filepath:expr,
        $file_name:expr,
        $($extension:expr),+
    ) => {
        $output_filepath
            .parent()
            .unwrap()
            .join(std::path::Path::new(&{
                let mut ret = if let Some(file_name) = $file_name {
                    std::ffi::OsString::from(file_name)
                } else {
                    std::ffi::OsString::new()
                };
                $(
                    if let Some(extension) = $extension {
                        ret.push(".");
                        ret.push(extension);
                    }
                )+
                ret.push(std::ffi::OsString::from(const_format::concatcp!(".", stringify!($extension_last_but_one), ".", stringify!($extension_last))));

                ret
            }))
    }
);

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use log::{debug, error, info, trace, warn};

    use pretty_assertions::assert_eq;
    use std::path::Path;

    declare_fn_get_last_but_one_and_last_extension_file_path!(.xml.gz);

    #[tokio::test]
    async fn declare_fn_get_last_but_one_and_last_extension_file_path() -> Result<()> {
        test_helper::init();

        assert_eq!(
            get_xml_gz_filepath(Path::new("/home/yb/abc/dev/some").to_owned()).as_path(),
            Path::new("/home/yb/abc/dev/some.xml.gz"),
        );

        assert_eq!(
            get_xml_gz_filepath(Path::new("/home/yb/abc/dev/some.xml").to_owned()).as_path(),
            Path::new("/home/yb/abc/dev/some.xml.gz"),
        );

        assert_eq!(
            get_xml_gz_filepath(Path::new("/home/yb/abc/dev/some.gz").to_owned()).as_path(),
            Path::new("/home/yb/abc/dev/some.xml.gz"),
        );

        assert_eq!(
            get_xml_gz_filepath(Path::new("/home/yb/abc/dev/some.xml.gz").to_owned()).as_path(),
            Path::new("/home/yb/abc/dev/some.xml.gz"),
        );

        assert_eq!(
            get_xml_gz_filepath(Path::new("/home/yb/abc/dev/some.thing").to_owned()).as_path(),
            Path::new("/home/yb/abc/dev/some.thing.xml.gz"),
        );

        assert_eq!(
            get_xml_gz_filepath(Path::new("/home/yb/abc/dev/some.thing.xml").to_owned()).as_path(),
            Path::new("/home/yb/abc/dev/some.thing.xml.gz"),
        );

        assert_eq!(
            get_xml_gz_filepath(Path::new("/home/yb/abc/dev/some.thing.gz").to_owned()).as_path(),
            Path::new("/home/yb/abc/dev/some.thing.xml.gz"),
        );

        Ok(())
    }
}

#[allow(unused_imports)]
pub(crate) use declare_fn_get_last_but_one_and_last_extension_file_path; // https://stackoverflow.com/questions/26731243/how-do-i-use-a-macro-across-module-files
