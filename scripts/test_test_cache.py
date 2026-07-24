import os
from pathlib import Path
import tempfile
import unittest

from scripts import test_cache


class TestCacheTest(unittest.TestCase):
    def test_missing_cache_has_zero_allocated_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing"

            self.assertEqual(test_cache.allocated_size(missing), 0)

    def test_cargo_target_dir_defaults_to_workspace_target(self) -> None:
        workspace = Path("/workspace")

        self.assertEqual(
            test_cache.cargo_target_dir(workspace, {}),
            workspace / "target",
        )

    def test_cargo_target_dir_honors_absolute_and_relative_environment_values(
        self,
    ) -> None:
        workspace = Path("/workspace")

        self.assertEqual(
            test_cache.cargo_target_dir(
                workspace,
                {"CARGO_TARGET_DIR": "/var/cache/varve"},
            ),
            Path("/var/cache/varve"),
        )
        self.assertEqual(
            test_cache.cargo_target_dir(
                workspace,
                {"CARGO_TARGET_DIR": "build/cargo"},
            ),
            workspace / "build/cargo",
        )

    def test_limit_allows_exactly_thirty_gibibytes(self) -> None:
        test_cache.enforce_limit(test_cache.DEFAULT_LIMIT_BYTES)

    def test_limit_rejects_one_byte_over_with_cleanup_diagnostic(self) -> None:
        size = test_cache.DEFAULT_LIMIT_BYTES + 1

        with self.assertRaises(test_cache.CacheLimitExceeded) as raised:
            test_cache.enforce_limit(size)

        diagnostic = str(raised.exception)
        self.assertIn("30.00 GiB", diagnostic)
        self.assertIn("30 GiB limit", diagnostic)
        self.assertIn("just clean-test-cache", diagnostic)

    def test_profile_sizes_use_allocated_bytes_for_expected_directories(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            for profile in ("debug", "workspace-test", "release"):
                profile_dir = target / profile
                profile_dir.mkdir()
                (profile_dir / "artifact").write_bytes(os.urandom(4096))

            sizes = test_cache.profile_sizes(target)

            self.assertEqual(
                tuple(sizes),
                ("debug", "workspace-test", "release"),
            )
            self.assertTrue(all(size > 0 for size in sizes.values()))


if __name__ == "__main__":
    unittest.main()
