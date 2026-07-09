#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

#[test]
fn corpus_files_all_have_expected_verdicts() {
    let root = project_root();
    let expected_path = root.join("resources/gql-corpus/corpus.expected");
    let expected = std::fs::read_to_string(&expected_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", expected_path.display()));

    let mut saw_accept = false;
    let mut saw_reject = false;
    let mut pinned_paths = BTreeSet::new();

    for (line_no, raw_line) in expected.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let (relative_path, expected_verdict) = line.split_once('\t').unwrap_or_else(|| {
            panic!(
                "{}:{}: expected '<path>\\tACCEPT|REJECT'",
                expected_path.display(),
                line_no + 1
            )
        });
        assert!(
            pinned_paths.insert(relative_path.to_string()),
            "{}:{}: duplicate corpus path '{}'",
            expected_path.display(),
            line_no + 1,
            relative_path
        );
        let source_path = root.join(relative_path);
        let source = std::fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("read {}: {err}", source_path.display()));
        let actual_verdict = if varve_gql::parse_program(&source).is_ok() {
            "ACCEPT"
        } else {
            "REJECT"
        };

        assert_eq!(
            actual_verdict,
            expected_verdict,
            "{}",
            source_path.display()
        );

        match expected_verdict {
            "ACCEPT" => saw_accept = true,
            "REJECT" => saw_reject = true,
            other => panic!(
                "{}:{}: unknown verdict '{other}'",
                expected_path.display(),
                line_no + 1
            ),
        }
    }

    assert!(
        saw_accept,
        "{} contains no ACCEPT cases",
        expected_path.display()
    );
    assert!(
        saw_reject,
        "{} contains no REJECT cases",
        expected_path.display()
    );

    let corpus_paths = collect_corpus_paths(&root);
    assert_eq!(
        pinned_paths, corpus_paths,
        "corpus.expected must pin every corpus file"
    );
}

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn collect_corpus_paths(root: &Path) -> BTreeSet<String> {
    let corpus_dir = root.join("resources/gql-corpus");
    let mut paths = BTreeSet::new();

    for entry in std::fs::read_dir(&corpus_dir)
        .unwrap_or_else(|err| panic!("read {}: {err}", corpus_dir.display()))
    {
        let path = entry.expect("read corpus entry").path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("gql") {
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .unwrap_or_else(|err| panic!("strip {}: {err}", path.display()));
        paths.insert(relative.display().to_string());
    }

    paths
}

#[test]
fn parse_corpus_bin_prints_verdicts_for_cli_paths() {
    let bin = env!("CARGO_BIN_EXE_parse_corpus");
    let temp = tempfile::tempdir().expect("create temp dir");
    let accept_path = temp.path().join("accept.gql");
    let reject_path = temp.path().join("reject.gql");
    std::fs::write(&accept_path, "MATCH (n:Person) RETURN n").expect("write accept case");
    std::fs::write(&reject_path, "MATCH (n:Person) RETURN").expect("write reject case");

    let output = Command::new(bin)
        .arg(&accept_path)
        .arg(&reject_path)
        .output()
        .expect("run parse_corpus");

    assert!(
        output.status.success(),
        "parse_corpus failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf-8");
    assert_eq!(
        stdout,
        format!(
            "{}\tACCEPT\n{}\tREJECT\n",
            accept_path.display(),
            reject_path.display()
        )
    );
}
