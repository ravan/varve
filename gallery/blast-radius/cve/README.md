# CVE facts, ingested as if it were 2021-12-10

`cve-2021-44228.json` is an in-toto ITE6 vulnerability attestation
(`https://in-toto.io/attestation/vulns/v0.1`) — the document type GUAC's `vuln`
parser turns into `CertifyVuln` statements. Format cribbed from
`refs/guac/internal/testing/testdata/exampledata/certify-vuln.json`.

## Why five subjects and not one

**This file must list every `log4j-core` version the corpus actually contains, or
Q1 silently under-reports and affected images look clean.** Four of the seven
affected images do not carry 2.14.1:

| purl | images |
|---|---|
| `…log4j-core@2.8.2` | druid |
| `…log4j-core@2.9.1` | logstash |
| `…log4j-core@2.11.1` | elasticsearch, sonarqube |
| `…log4j-core@2.14.0` | logstash |
| `…log4j-core@2.14.1` | solr, flink, neo4j |

All five are inside CVE-2021-44228's affected range (2.0-beta9 – 2.14.1). The
authoritative list is the `log4j-core` column of `../corpus/MANIFEST`, and
`../refresh-corpus.sh` prints it at the end of every run — re-check it if the
cohort ever changes.

The purl strings were read **out of the corpus SBOMs**, not hand-written: they
must match byte-for-byte what syft emitted, because GUAC derives the `PkgVersion`
node `_id` from the purl. A mismatched qualifier would attach the `CertifyVuln` to
a *different* node and the traversal would find nothing, with no error. Verified
that syft emits these with no qualifiers.

One statement with five subjects is enough — GUAC's parser applies the predicate
to every subject, yielding one `CertifyVuln` per (package, vulnerability) pair.

## Two timestamps, doing different jobs

- `predicate.metadata.scanStartedOn` / `scanFinishedOn` = **2021-12-10T10:15:00Z**
  is *document content*: GUAC's `ScanMetadata`, i.e. "the scanner claims it looked
  at this on ship day".
- The **valid-time** is set out-of-band by running `guacgql` with
  `--varve-valid-from=2021-12-10T10:15:00Z` (see `../patches/`). That is what
  makes Q2 ("what did we know on ship day") differ from Q3 ("what was true on ship
  day") — the bitemporal point of the whole demo.

They are deliberately the same instant so the narrative is coherent, but they are
independent mechanisms: the first is a property GUAC stores, the second is varve's
valid-time axis. Changing one does not change the other.

**The flag is on `guacgql`, not `guacone`** — the varve backend lives in the
GraphQL server, so valid-time is a property of the running server process. The
corpus must therefore be ingested by a `guacgql` with **no** flag, and this file by
one **with** it.
