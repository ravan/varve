# GQL ANTLR Grammar (vendored)

Source: https://github.com/opengql/grammar (Apache-2.0, see LICENSE)
Vendored: 2026-07-04 from `main` @ `16ea71bd320ad07fd2c46a3066afbaef7d226922`.

Language-independent ANTLR4 grammar for GQL (ISO/IEC 39075:2024, published April
2024). An initial version was generated with
[gramgen](https://github.com/mburbidg/gramgen) from the XML BNF artifact of the
ISO specification, then hand-tweaked to remove ambiguities present in the BNF
(chiefly mutually left-recursive value-expression productions). ~571 parser
rules covering the full statement surface, path-pattern language (quantifiers,
match modes, SHORTEST), temporal types, sessions, and graph-type DDL.

Upstream ships lexer and parser rules combined in a single `GQL.g4` (lexer rules
at the bottom) rather than the conventional split `GQLLexer.g4` / `GQLParser.g4`,
to work around a JetBrains ANTLR4 plugin bug with separate grammar files.

## Role in Varve

1. **Grammar reference** for the hand-written parser in `varve-gql` — the
   authoritative answer to "what does GQL syntax allow" without ISO spec access.
2. **Differential-test oracle**: CI generates an ANTLR parser from this file
   (Java tooling, test-only) and cross-checks accept/reject verdicts against
   `varve-gql` over the test corpus and fuzz-generated statements.
3. **Coverage checklist**: parser rules map to the practical-core feature list
   in the design doc §8; rules we deliberately don't implement are recorded as
   roadmap items. Each rule carries its ISO section number as a comment
   (e.g. `// 7.1 <session set command>`), so this mapping is direct.

## Caveats

- `options { caseInsensitive = true; }` requires ANTLR ≥ 4.10; the CI
  differential oracle must use a compatible toolchain.
- The grammar contains hand-tweaks relative to the raw ISO BNF (see upstream
  README for details); where a tweak changes accepted syntax, prefer the ISO
  semantics and record the deviation in `varve-gql` docs.
- May diverge from openCypher in places — diff against openCypher TCK
  expectations when in doubt.
