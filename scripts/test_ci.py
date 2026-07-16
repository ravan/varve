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


if __name__ == "__main__":
    unittest.main()
