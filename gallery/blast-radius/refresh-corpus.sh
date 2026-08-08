#!/bin/sh
# Regenerates the committed blast-radius corpus: one stripped, gzipped SPDX
# document per digest-pinned image, plus corpus/MANIFEST.
#
# This is NOT run by demo.sh. demo.sh is fully offline and reads corpus/*.gz as
# committed. Run this only when the cohort changes or syft is upgraded.
#
# Needs network (~6 GB of layers read straight from the registries -- no docker
# pull, no local images) plus syft and jq. Roughly 9 minutes.
#
# Usage:   sh gallery/blast-radius/refresh-corpus.sh
# In the timedb dev environment, route docker/network through rtk:
#          rtk proxy sh gallery/blast-radius/refresh-corpus.sh
set -eu

cd "$(dirname "$0")"

# Every image is pinned to its multi-arch *index* digest and read at
# linux/amd64, so the corpus is byte-reproducible on any host architecture.
# Tags are recorded for humans only -- the digest is what is fetched.
PLATFORM="linux/amd64"

# Twelve images, all from the Log4Shell window (Nov-Dec 2021). Seven genuinely
# carry a log4j-core in CVE-2021-44228's range; five are clean controls, kept
# fat and deliberately non-JVM-heavy (Node, Python, Java-without-log4j) so the
# corpus is not just one base image repeated. Verdicts are computed below from
# the SBOMs, not from this list -- see corpus/MANIFEST for what was found.
#
# name                     image ref                                                    index digest
COHORT="
solr-8.11.0                solr                                                         sha256:66fe2feeba8c4afdea12c78a4f11218fadd81befc43f223a2f9267bf605a25d1
flink-1.14.0               flink                                                        sha256:e8372e92cd7fa81bba9eda5c4b5eff18754acfca0ec1427a174a113fcfe36909
logstash-7.16.0            docker.elastic.co/logstash/logstash                          sha256:f777d1b84ec679b64717087bdf9617785b05cc3414689e7bdef53d5d46528bc6
elasticsearch-7.16.0       docker.elastic.co/elasticsearch/elasticsearch                sha256:2cdefcb9754028f0b2c860cf9ec52be15c026f3aa23c22ec181e321c427aadc7
sonarqube-9.2.2            sonarqube                                                    sha256:d5128e042b63b96ab3a580e51ae68d8f3c215ecd345f9c1d4cc65651126572fb
neo4j-4.4.0                neo4j                                                        sha256:d0f73347c6093e474580cecf2bf3fef8988cd219d749336d513f2605b3d19d56
druid-0.22.0               apache/druid                                                 sha256:626fd96a997361dce8452c68b28e935a2453153f0d743cf208a0b4355a4fc2c3
kibana-7.16.0              docker.elastic.co/kibana/kibana                              sha256:5817d0a144507fe6957615fc0ccb6b8254addcfdabeff958432f65ac6377d632
odoo-15.0                  odoo                                                         sha256:9843efa8a0c8fba20f81a97fd04339fe0cc49c3162485c3e8bc0447295434ac1
jenkins-2.319.1            jenkins/jenkins                                              sha256:c1d02293a08ba69483992f541935f7639fb10c6c322785bdabaf7fa94cd5e732
tomcat-9.0.55-jdk11        tomcat                                                       sha256:f80d091e4a086fc1253cdc04011b5cd3c6820e8df5ff3047b0175e105435ba68
nginx-1.21.4               nginx                                                        sha256:366e9f1ddebdb844044c2fafd13b75271a9f620819370f8971220c2b330a9254
"

# --- the file-stripping filter -------------------------------------------------
#
# syft emits one SPDX `files` entry per file in the image and a CONTAINS
# relationship for each -- 88% of the solr document. GUAC turns every one of
# those into a `pkg:v:guac/files/sha1:...` pseudo-package plus an IsOccurrence
# and an Artifact node, which cost 364 s of ingest per image and buried the real
# packages 33:1. Stripping them takes an image from 6.4 MB to ~95 KB gzipped and
# ingest from 364 s to 19 s.
#
# What survives is exactly what the demo traverses: the DESCRIBES root, the
# `DocumentRoot CONTAINS <package>` edges that become IsDependency, and the
# package-to-package relationships. Those edges are genuine SPDX, not GUAC's
# topLevelIsHeuristic synthesis -- verified against the raw solr document.
STRIP='
del(.files)
| .packages |= map(del(.hasFiles))
| .relationships |= map(select(
    ((.spdxElementId       | startswith("SPDXRef-File")) | not)
    and ((.relatedSpdxElement | startswith("SPDXRef-File")) | not)))
'

# --- normalisation, so the committed corpus is byte-reproducible ----------------
#
# syft is deterministic in content but stamps two per-run values into every
# document: a random UUID in documentNamespace and wall-clock in
# creationInfo.created. Strip those and two runs over the same digest are
# byte-identical; leave them and MANIFEST's checksums prove nothing, because a
# re-run cannot reproduce them.
#
# documentNamespace keeps one distinct value per image -- GUAC uses it as the
# HasSBOM `uri`, so collapsing it to a constant would merge twelve HasSBOM nodes
# into one and break Q1. It now names the pinned digest instead of a UUID, which
# also makes Q1's `s.uri` column readable. The syft namespace prefix is kept:
# syft really did produce this document, and creators still records the version.
#
# created is pinned to the epoch. It is a normalisation placeholder, not a claim
# about when anything happened -- deliberately a value nobody can misread as
# real. Nothing the demo queries reads it.
NORMALIZE='
.documentNamespace = "https://anchore.com/syft/image/" + $name + "@" + $digest
| .creationInfo.created = "1970-01-01T00:00:00Z"
'

# All log4j-core versions in a document, as a comma-separated list ("-" if none).
LOG4J='
[ .packages[]
  | select(any(.externalRefs[]?.referenceLocator? // "";
               startswith("pkg:maven/org.apache.logging.log4j/log4j-core@")))
  | .versionInfo ]
| unique | if length == 0 then "-" else join(",") end
'

# CVE-2021-44228 affects log4j-core 2.0-beta9 through 2.14.1. 2.3.1, 2.12.2 and
# later patch releases on those branches are fixed, as is everything >= 2.15.0.
# Decided from the version syft actually found, never from the image tag.
vulnerable() {
  echo "$1" | tr ',' '\n' | awk '
    /^2\./ {
      split($0, v, /[.-]/); minor = v[2] + 0; patch = v[3] + 0
      if (minor >  14)                    next          # 2.15+ fixed
      if (minor == 12 && patch >= 2)      next          # 2.12.2+ fixed
      if (minor ==  3 && patch >= 1)      next          # 2.3.1+ fixed
      if (minor ==  0 && $0 ~ /beta[1-8]$/) next        # pre-beta9
      found = 1
    }
    END { exit(found ? 0 : 1) }'
}

for t in syft jq; do
  command -v "$t" >/dev/null || { echo "$t is required but not on PATH" >&2; exit 1; }
done
SYFT_VERSION=$(syft version -o json | jq -r .version)

mkdir -p corpus corpus/raw corpus/expanded
: > corpus/MANIFEST.tmp
started=$(date +%s)

echo "$COHORT" | while read -r name ref digest; do
  [ -z "$name" ] && continue

  printf '=== %s (%s)\n' "$name" "$ref"
  raw="corpus/raw/$name.spdx.json"
  gz="corpus/$name.spdx.json.gz"

  syft -q -o spdx-json --from oci-registry --platform "$PLATFORM" \
    "$ref@$digest" > "$raw"

  log4j=$(jq -r "$LOG4J" "$raw")
  if vulnerable "$log4j"; then verdict=AFFECTED; else verdict=CONTROL; fi

  # gzip -n so the archive carries no mtime either. With NORMALIZE above, the
  # whole path is deterministic: same digest in, same bytes out, same sha256.
  jq -c --arg name "$name" --arg digest "$digest" "$STRIP | $NORMALIZE" "$raw" |
    gzip -n -9 > "$gz"

  pkgs=$(jq '.packages | length' "$raw")
  sha=$(shasum -a 256 "$gz" | cut -d' ' -f1)
  # Exact bytes, not du -- du reports allocated blocks and over-reports by up to
  # 50% on a file this small, which would put a wrong size in the README.
  printf '  %s packages, log4j-core=%s -> %s, %s bytes gz\n' \
    "$pkgs" "$log4j" "$verdict" "$(wc -c < "$gz" | tr -d ' ')"

  printf '%s\t%s\t%s@%s\t%s\t%s\t%s\n' \
    "$(basename "$gz")" "$sha" "$ref" "$digest" "$pkgs" "$log4j" "$verdict" \
    >> corpus/MANIFEST.tmp
done

{
  echo "# varve blast-radius demo corpus"
  echo "#"
  echo "# Regenerate with: sh gallery/blast-radius/refresh-corpus.sh"
  echo "# syft $SYFT_VERSION, platform $PLATFORM, SPDX files/ stripped."
  echo "#"
  echo "# The AFFECTED/CONTROL column is derived from the log4j-core version"
  echo "# syft found inside each image, not from its tag. CVE-2021-44228"
  echo "# affects 2.0-beta9 through 2.14.1."
  echo "#"
  echo "# The sha256 column is reproducible: re-running refresh-corpus.sh with the"
  echo "# same syft version yields byte-identical files. syft's per-run"
  echo "# documentNamespace UUID and creationInfo.created are normalised away to"
  echo "# make that true -- created reads 1970-01-01T00:00:00Z in every document"
  echo "# and is a placeholder, not a timestamp."
  echo "#"
  echo "# file\tsha256\timage@index-digest\tpackages\tlog4j-core\tverdict"
  cat corpus/MANIFEST.tmp
} > corpus/MANIFEST
rm -f corpus/MANIFEST.tmp

# MANIFEST columns: 1 file, 2 sha256, 3 image@digest, 4 packages, 5 log4j, 6 verdict.
total_pkgs=$(awk -F'\t' '!/^#/ {n += $4} END {print n+0}' corpus/MANIFEST)
affected=$(awk -F'\t' '$6 == "AFFECTED"' corpus/MANIFEST | wc -l | tr -d ' ')
controls=$(awk -F'\t' '$6 == "CONTROL"'  corpus/MANIFEST | wc -l | tr -d ' ')

# Distinct purls across the corpus. GUAC keys PkgVersion by purl, so this is
# what S1 (>= 4,000 real PkgVersion) is actually measuring -- one purl became
# exactly one PkgVersion when this was checked against a live ingest.
purls=$(jq -r '.packages[].externalRefs[]? | select(.referenceType == "purl")
               | .referenceLocator' corpus/raw/*.spdx.json |
        sort -u | wc -l | tr -d ' ')

echo
echo "=== corpus refreshed in $(( $(date +%s) - started ))s"
echo "    $((affected + controls)) images: $affected affected, $controls control"
echo "    $total_pkgs SPDX packages, $purls distinct purls"
echo "    $(cat corpus/*.spdx.json.gz | wc -c | tr -d ' ') bytes gzipped total"
echo "    corpus/MANIFEST written; corpus/raw/ (refresh scratch) and corpus/expanded/ (demo.sh) are gitignored"
echo
echo "=== log4j-core found in the corpus (CertifyVuln targets for cve/cve-2021-44228.json)"
jq -r '.packages[].externalRefs[]? | select(.referenceType == "purl")
       | .referenceLocator
       | select(startswith("pkg:maven/org.apache.logging.log4j/log4j-core@"))' \
  corpus/raw/*.spdx.json | sort -u | sed 's/^/    /'
