# GQL Differential Parser Harness

This harness compares Varve's practical-core GQL parser against the generated
ANTLR parser from `resources/gql-grammar/GQL.g4`.

The corpus in `resources/gql-corpus/*.gql` is practical-core GQL. Files ending
in `.ext.gql` are Varve extensions such as temporal clauses, `ERASE`, and
Varve's semicolon-delimited program/catalog shorthand, so the ANTLR parser is
allowed to reject them.

The differential check is intentionally one-way: it only fails when Varve
accepts a non-extension `.gql` file that the official grammar rejects. Varve is
a practical-core subset parser, so Varve rejecting valid GQL is reported in the
table as `VARVE_SUBSET` without failing the check.

## Local Run

```bash
curl -fsSLo /tmp/antlr-4.13.2-complete.jar https://www.antlr.org/download/antlr-4.13.2-complete.jar
mkdir -p target/gql-diff/generated target/gql-diff/classes
(cd resources/gql-grammar && java -jar /tmp/antlr-4.13.2-complete.jar -Dlanguage=Java -o ../../target/gql-diff/generated GQL.g4)
javac -cp /tmp/antlr-4.13.2-complete.jar -d target/gql-diff/classes target/gql-diff/generated/*.java scripts/gql_diff/Main.java
cargo run -p varve-testkit --bin parse_corpus -- resources/gql-corpus/*.gql > target/gql-diff/varve.tsv
java -cp /tmp/antlr-4.13.2-complete.jar:target/gql-diff/classes Main resources/gql-corpus/*.gql > target/gql-diff/antlr.tsv
python3 scripts/gql_diff/compare.py target/gql-diff/varve.tsv target/gql-diff/antlr.tsv
```
