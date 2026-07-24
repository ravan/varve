from pathlib import Path
import unittest


WORKFLOW = Path(__file__).parents[1] / ".github" / "workflows" / "ci.yml"


class CiWorkflowTest(unittest.TestCase):
    def test_check_job_limits_rust_build_disk_usage(self) -> None:
        workflow = WORKFLOW.read_text()

        for setting in (
            'CARGO_INCREMENTAL: "0"',
            'CARGO_PROFILE_DEV_DEBUG: "0"',
            'CARGO_PROFILE_TEST_DEBUG: "0"',
        ):
            self.assertIn(setting, workflow)

        check_job = workflow.split("\n  check:\n", 1)[1].split("\n  docs:\n", 1)[0]
        self.assertIn("cache-targets: false", check_job)

    def test_check_job_runs_full_isolated_nextest_and_doctest_lanes(self) -> None:
        workflow = WORKFLOW.read_text()
        check_job = workflow.split("\n  check:\n", 1)[1].split("\n  docs:\n", 1)[0]

        self.assertIn("taiki-e/install-action@nextest", check_job)
        self.assertIn("PROPTEST_CASES: \"10000\"", check_job)
        self.assertIn(
            "cargo nextest run --workspace --cargo-profile workspace-test",
            check_job,
        )
        self.assertIn(
            "-E 'not (test(=db_traversal_matches_oracle) "
            "| test(=traversal_invariant_under_flush))'",
            check_job,
        )
        self.assertIn(
            "env -u PROPTEST_CASES cargo nextest run --workspace "
            "--cargo-profile workspace-test",
            check_job,
        )
        self.assertIn(
            "cargo test --workspace --doc --profile workspace-test",
            check_job,
        )


if __name__ == "__main__":
    unittest.main()
