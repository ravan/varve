#![no_main]

use libfuzzer_sys::fuzz_target;
use varve_gql::{parse_program, to_gql_program};

fuzz_target!(|data: &[u8]| {
    let Ok(src) = std::str::from_utf8(data) else {
        return;
    };

    if let Ok(program) = parse_program(src) {
        let printed = to_gql_program(&program);
        match parse_program(&printed) {
            Ok(reparsed) => assert_eq!(reparsed, program, "printed GQL program: {printed}"),
            Err(err) => panic!("failed to reparse printed GQL program {printed:?}: {err}"),
        }
    }
});
