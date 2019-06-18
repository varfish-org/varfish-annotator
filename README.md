# VarFish Annotator

Annotation of VCF file for import into VarFish (through Web UI).

## Supported Databases

- ExAC r1.0
- gnomAD exomes r2.1
- gnomAD genomes r2.1
- clinvar
- thousand genomes
- hgmd\_public from ENSEMBL r75

## Example

The following will create `varfish-annotator-db-1906.h2.db` and fill it.

```
# DOWNLOAD=path/to/varfish-db-downloader
# ANNOTATOR_VERSION=0.7
# ANNOTATOR_DATA_RELEASE=1906
# java -jar varfish-annotator-cli-$ANNOTATOR_VERSION-SNAPSHOT.jar \
      init-db \
      --db-release-info "varfish-annotator:v$ANNOTATOR_VERSION" \
      --db-release-info "varfish-annotator-db:r1906" \
      \
      --ref-path /fast/projects/cubit/18.12/static_data/reference/GRCh37/hs37d5/hs37d5.fa \
      \
      --db-release-info "clinvar:2019-02-20" \
      --clinvar-path $DOWNLOAD/GRCh37/clinvar/latest/clinvar_tsv_main/output/clinvar_allele_trait_pairs.single.b37.tsv \
      --clinvar-path $DOWNLOAD/GRCh37/clinvar/latest/clinvar_tsv_main/output/clinvar_allele_trait_pairs.multi.b37.tsv \
      \
      --db-path ./varfish-annotator-db-$ANNOTATOR_DATA_RELEASE \
      \
      --db-release-info "exac:r1.0" \
      --exac-path $DOWNLOAD/GRCh37/ExAC/r1/download/ExAC.r1.sites.vep.vcf.gz \
      \
      --db-release-info "gnomad_exomes:r2.1" \
      $(for path in $DOWNLOAD/GRCh37/gnomAD_exomes/r2.1/download/gnomad.exomes.r2.1.sites.chr*.normalized.vcf.bgz; do \
          echo --gnomad-exomes-path $path; \
      done) \
      \
      --db-release-info "gnomad_genomes:r2.1" \
      $(for path in $DOWNLOAD/GRCh37/gnomAD_genomes/r2.1/download/gnomad.genomes.r2.1.sites.chr*.normalized.vcf.bgz; do \
          echo --gnomad-genomes-path $path; \
      done) \
      \
      --db-release-info "thousand_genomes:v3.20101123"
      $(for path in $DOWNLOAD/GRCh37/thousand_genomes/phase3/ALL.chr*.phase3_shapeit2_mvncall_integrated_v5a.20130502.sites.vcf.gz; do \
          echo --thousand-genomes-path $path; \
      done) \
      \
      --db-release-info "hgmd_public:ensembl_r75" \
      --hgmd-public $DOWNLOAD/GRCh37/hgmd_public/ensembl_r75/HgmdPublicLocus.tsv
```

## Formatting Source Code

```
# mvn com.coveo:fmt-maven-plugin:format -Dverbose=true
```
