import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("compare.py")


class CompareTests(unittest.TestCase):
    def run_compare(self, varve_lines, antlr_lines):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            varve = tmp_path / "varve.tsv"
            antlr = tmp_path / "antlr.tsv"
            varve.write_text("\n".join(varve_lines) + "\n", encoding="utf-8")
            antlr.write_text("\n".join(antlr_lines) + "\n", encoding="utf-8")
            return subprocess.run(
                [sys.executable, str(SCRIPT), str(varve), str(antlr)],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

    def test_fails_when_varve_accepts_gql_rejected_by_antlr(self):
        result = self.run_compare(
            ["resources/gql-corpus/basic-match.gql\tACCEPT"],
            ["resources/gql-corpus/basic-match.gql\tREJECT"],
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("FAIL", result.stdout)
        self.assertIn("basic-match.gql", result.stdout)

    def test_allows_varve_extension_rejected_by_antlr(self):
        result = self.run_compare(
            ["resources/gql-corpus/temporal-window.ext.gql\tACCEPT"],
            ["resources/gql-corpus/temporal-window.ext.gql\tREJECT"],
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_allows_varve_rejecting_valid_gql(self):
        result = self.run_compare(
            ["resources/gql-corpus/query-pipeline.gql\tREJECT"],
            ["resources/gql-corpus/query-pipeline.gql\tACCEPT"],
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
