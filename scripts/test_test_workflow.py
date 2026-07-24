from pathlib import Path
import tomllib
import unittest


ROOT = Path(__file__).parents[1]


class TestWorkflowTest(unittest.TestCase):
    def test_cargo_profiles_keep_tests_lean_and_isolated(self) -> None:
        cargo = tomllib.loads((ROOT / "Cargo.toml").read_text())

        self.assertEqual(cargo["profile"]["test"]["debug"], 0)
        self.assertFalse(cargo["profile"]["test"]["incremental"])
        self.assertEqual(cargo["profile"]["test"]["strip"], "symbols")
        self.assertEqual(
            cargo["profile"]["workspace-test"]["inherits"],
            "test",
        )

    def test_just_test_lanes_use_guard_workspace_profile_and_doctests(
        self,
    ) -> None:
        justfile = (ROOT / "justfile").read_text()

        self.assertIn("cache-status:", justfile)
        self.assertIn("clean-test-cache:", justfile)
        self.assertIn("clean-debug-cache:", justfile)
        self.assertGreaterEqual(
            justfile.count("python3 scripts/test_cache.py guard"),
            3,
        )
        self.assertIn(
            "PROPTEST_CASES=256 cargo nextest run --workspace "
            "--cargo-profile workspace-test",
            justfile,
        )
        self.assertIn(
            "PROPTEST_CASES=10000 cargo nextest run --workspace "
            "--cargo-profile workspace-test",
            justfile,
        )
        self.assertGreaterEqual(
            justfile.count(
                "-E 'not (test(=db_traversal_matches_oracle) "
                "| test(=traversal_invariant_under_flush))'"
            ),
            3,
        )
        self.assertGreaterEqual(
            justfile.count(
                "env -u PROPTEST_CASES cargo nextest run --workspace "
                "--cargo-profile workspace-test"
            ),
            3,
        )
        self.assertIn(
            "cargo test --workspace --doc --profile workspace-test",
            justfile,
        )

    def test_nextest_serializes_os_process_tests(self) -> None:
        config = tomllib.loads(
            (ROOT / ".config" / "nextest.toml").read_text()
        )

        self.assertEqual(
            config["test-groups"]["process-tests"]["max-threads"],
            1,
        )
        override = config["profile"]["default"]["overrides"][0]
        self.assertEqual(override["test-group"], "process-tests")
        for binary in (
            "process_consistency",
            "process_scale_out",
            "crash_recovery",
        ):
            self.assertIn(f"binary({binary})", override["filter"])


if __name__ == "__main__":
    unittest.main()
