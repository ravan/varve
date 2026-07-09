use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut ok = true;

    for path in std::env::args_os().skip(1).map(PathBuf::from) {
        match std::fs::read_to_string(&path) {
            Ok(source) => {
                let verdict = if varve_gql::parse_program(&source).is_ok() {
                    "ACCEPT"
                } else {
                    "REJECT"
                };
                println!("{}\t{verdict}", path.display());
            }
            Err(err) => {
                eprintln!("{}: {err}", path.display());
                ok = false;
            }
        }
    }

    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
