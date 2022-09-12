#!/usr/bin/bash

set -euo pipefail
set -x

JAR=$(ls ../../varfish-annotator-cli/target/varfish-annotator-cli-*.jar | grep -v sources | tail -n 1)

## prepare

gzip -d -c chr22_part.fa.gz > chr22_part.fa

## step 0: help

java -jar $JAR --help >/tmp/the-output
test -s /tmp/the-output

set +e
java -jar $JAR > /tmp/the-output
retcode=$?
set -e
test 1 -eq $retcode
test -s /tmp/the-output

## step 1: init-db

java -jar $JAR init-db \
  --release GRCh37 \
  --db-release-info varfish-annotator:main \
  --db-release-info varfish-annotator-db:for-testing \
  --db-path /tmp/out \
  \
  --ref-path chr22_part.fa \
  \
  --db-release-info exac:r1.0 \
  --exac-path ExAC.r1.sites.vep.vcf.gz \
  \
  --db-release-info thousand_genomes:v3.20101123 \
  --thousand-genomes-path ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz \
  \
  --db-release-info clinvar:for-testing \
  --clinvar-path Clinvar.tsv.gz \
  \
  --db-release-info gnomad_exomes:r2.1.1 \
  --gnomad-exomes-path gnomad.exomes.r2.1.1.sites.chr22.vcf.bgz \
  \
  --db-release-info gnomad_genomes:r2.1.1 \
  --gnomad-genomes-path gnomad.genomes.r2.1.1.sites.chr22.vcf.bgz \
  \
  --db-release-info hgmd_public:for-testing \
  --hgmd-public HgmdPublicLocus.tsv.gz

## step 2: db-info

java -jar $JAR db-stats --db-path /tmp/out.h2.db --parseable \
> /tmp/db-info.txt
diff /tmp/db-info.txt db-info.txt-expected

## step 3: annotate

java -jar $JAR annotate \
  --release GRCh37 \
  --input-vcf Case_1_index.gatk_hc.vcf.gz \
  --output-gts /tmp/Case_1_index.gatk_hc.gts.tsv \
  --output-db-info /tmp/Case_1_index.gatk_hc.db-info.tsv \
  --ref-path chr22_part.fa \
  --refseq-ser-path hg19_refseq.ser \
  --ensembl-ser-path hg19_ensembl.ser \
  --db-path /tmp/out.h2.db \
  --self-test-chr22-only

diff /tmp/Case_1_index.gatk_hc.gts.tsv Case_1_index.gatk_hc.gts.tsv-expected
diff /tmp/Case_1_index.gatk_hc.db-info.tsv Case_1_index.gatk_hc.db-info.tsv-expected

## step 4: annotate-svs

java -jar $JAR annotate-svs \
  --release GRCh37 \
  --input-vcf Case_1_index.delly2.vcf.gz \
  --output-gts /tmp/Case_1_index.delly2.gts.tsv \
  --output-feature-effects /tmp/Case_1_index.delly2.feature-effects.tsv \
  --output-db-info /tmp/Case_1_index.delly2.db-info.tsv \
  --refseq-ser-path hg19_refseq.ser \
  --ensembl-ser-path hg19_ensembl.ser \
  --db-path /tmp/out.h2.db \
  --self-test-chr22-only

awk -F $'\t' 'BEGIN { OFS=FS } { if (NR > 1) $17 = "UUID"; print $0; }' /tmp/Case_1_index.delly2.gts.tsv \
> /tmp/Case_1_index.delly2.gts.tsv.replaced
diff /tmp/Case_1_index.delly2.gts.tsv.replaced Case_1_index.delly2.gts.tsv-expected
diff /tmp/Case_1_index.delly2.db-info.tsv Case_1_index.delly2.db-info.tsv-expected
awk -F $'\t' 'BEGIN { OFS=FS } { if (NR > 1) $3 = "UUID"; print $0; }' /tmp/Case_1_index.delly2.feature-effects.tsv \
> /tmp/Case_1_index.delly2.feature-effects.tsv.replaced
diff /tmp/Case_1_index.delly2.feature-effects.tsv.replaced Case_1_index.delly2.feature-effects.tsv-expected

## if we reach here, everything is fine

echo "-- ALL TESTS PASSED --"
exit 0
