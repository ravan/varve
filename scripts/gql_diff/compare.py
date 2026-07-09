#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path


VALID_VERDICTS = {"ACCEPT", "REJECT"}


def read_verdicts(path):
    verdicts = {}
    with Path(path).open(encoding="utf-8") as handle:
        for lineno, line in enumerate(handle, start=1):
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                corpus_path, verdict = line.split("\t", 1)
            except ValueError as exc:
                raise ValueError(f"{path}:{lineno}: expected '<path>\\tACCEPT|REJECT'") from exc
            if verdict not in VALID_VERDICTS:
                raise ValueError(f"{path}:{lineno}: invalid verdict {verdict!r}")
            if corpus_path in verdicts:
                raise ValueError(f"{path}:{lineno}: duplicate path {corpus_path!r}")
            verdicts[corpus_path] = verdict
    return verdicts


def classify(corpus_path, varve, antlr):
    if varve is None or antlr is None:
        return "MISSING"
    if varve == "ACCEPT" and antlr == "REJECT":
        if corpus_path.endswith(".ext.gql"):
            return "EXTENSION"
        return "FAIL"
    if varve == "REJECT" and antlr == "ACCEPT":
        return "VARVE_SUBSET"
    if varve == antlr:
        return "OK"
    return "OK"


def compare(varve, antlr):
    paths = sorted(set(varve) | set(antlr))
    rows = []
    failures = []
    missing = []

    for corpus_path in paths:
        varve_verdict = varve.get(corpus_path)
        antlr_verdict = antlr.get(corpus_path)
        status = classify(corpus_path, varve_verdict, antlr_verdict)
        rows.append((corpus_path, varve_verdict or "-", antlr_verdict or "-", status))
        if status == "FAIL":
            failures.append(corpus_path)
        elif status == "MISSING":
            missing.append(corpus_path)

    return rows, failures, missing


def print_table(rows):
    print("path\tvarve\tantlr\tstatus")
    for corpus_path, varve, antlr, status in rows:
        print(f"{corpus_path}\t{varve}\t{antlr}\t{status}")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Compare Varve corpus verdicts against the ANTLR GQL grammar verdicts."
    )
    parser.add_argument("varve", help="Varve verdict file: '<path>\\tACCEPT|REJECT'")
    parser.add_argument("antlr", help="ANTLR verdict file: '<path>\\tACCEPT|REJECT'")
    args = parser.parse_args(argv)

    try:
        varve = read_verdicts(args.varve)
        antlr = read_verdicts(args.antlr)
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    rows, failures, missing = compare(varve, antlr)
    print_table(rows)

    if missing:
        print()
        print("Missing verdicts:")
        for corpus_path in missing:
            print(f"  {corpus_path}")
        return 2

    if failures:
        print()
        print("ANTLR rejected non-extension GQL that Varve accepted:")
        for corpus_path in failures:
            print(f"  {corpus_path}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
