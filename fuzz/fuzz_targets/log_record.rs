#![no_main]

use libfuzzer_sys::fuzz_target;
use varve_log::decode_frames;

fuzz_target!(|data: &[u8]| {
    if let Ok(records) = decode_frames("fuzz", data) {
        // Anything the strict decoder accepts must round-trip its envelope.
        for record in records {
            let reparsed = varve_log::LogRecord::from_wire(&record.to_wire())
                .expect("decoded record must re-encode/decode");
            assert_eq!(reparsed, record);
        }
    }
});
