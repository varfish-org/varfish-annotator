[![Continuous Integration](https://github.com/bihealth/varfish-annotator/actions/workflows/ci.yml/badge.svg)](https://github.com/bihealth/varfish-annotator/actions?query=workflow%3Acontinuous-integration)
[![Coverage Status](https://coveralls.io/repos/github/bihealth/varfish-annotator/badge.svg?branch=main)](https://coveralls.io/github/bihealth/varfish-annotator?branch=main)
[![install with bioconda](https://img.shields.io/badge/install%20with-bioconda-brightgreen.svg?style=flat)](http://bioconda.github.io/recipes/varfish-annotator-cli/README.html)

# VarFish Annotator

Annotation of VCF file for import into VarFish (through Web UI).

## Supported Databases

- ExAC r1.0
- gnomAD exomes r2.1
- gnomAD genomes r2.1
- clinvar
- thousand genomes
- hgmd\_public from ENSEMBL r75

## Supported Variant Callers

VarFish annotator uses [HTSJDK](https://github.com/samtools/htsjdk) for reading [variant call format (VCF) files](https://samtools.github.io/hts-specs/).
HTSJDK supports reading VCF v4.3 so the output of any tool that produces well-formed VCF can be read.
VCF itself only specifies relatively few required fields and callers may use fields in a slightly different way.
We thus document below what fields are used/interpreted by VarFish Annotator to prepare the files for VarFish.

### Small Variants

The following fields are considered:

- `CHROM`
  Will be written out in a normalized way, depending on the genome build.
  That the `chr` prefix will be presenet for GRCh38 and it will be absent for GRCh37.
- `POS`
  The 1-based chromosomal position is written out.
- `REF`
  The reference allele will be written out.
- `ALT`
  For each alternative allele and each gene, a record is written out.
  Asterisk alleles will be ignored ([see Broad's GATK documentation on this](https://gatk.broadinstitute.org/hc/en-us/articles/360035531912-Spanning-or-overlapping-deletions-allele-)).
- `FORMAT` and per `SAMPLE`
  - `GT` The genotype is written out with phasing information.
  - `AD` The allelic depth of the alternative allele of the current record is written out.
  - `DP` The total depth is written out to compute the allelic balance of the alternative allele of the current record is written out.
  - `GQ` The genotype quality is written out.

### Structural Variants / Copy Number Variants

**Supported Callers and Caller Annotation**

The following variant callers are explicitely supported.

- Delly 2 (SVs)
- Dragen CNV caller
- Dragen SV caller
- Manta
- GATK gCNV
- XHMM (deprecated)

In the other cases, VarFish annotator will fall back to a "generic" import where only the per-sample fields `GT`, `FT`, and `GQ` are interpreted.
Your caller should also write out `INFO/END`, `INFO/SVTYPE`, and `INFO/SVLEN` as defined by VCF4.2

VarFish Annotator will look at the field `INFO/SVMETHOD` to annotate calls with the caller where the call originated from.
If this field is empty then you should define `--default-sv-method` so you get appropriately labeled output.
If you have any problem with your data then please tell us by opening a GitHub issue.

**Interpretation of top-level and INFO VCF fields**

The following fields are considered:

- `CHROM`
  Will be written out in a normalized way, depending on the genome build.
  That the `chr` prefix will be presenet for GRCh38 and it will be absent for GRCh37.
- `POS`
  The 1-based chromosomal position is written out.
- `REF`
  The reference allele will be written out.
- `ALT`
  For each alternative allele and each gene, a record is written out.
  Asterisk alleles will be ignored ([see Broad's GATK documentation on this](https://gatk.broadinstitute.org/hc/en-us/articles/360035531912-Spanning-or-overlapping-deletions-allele-)).
- `INFO/CHR2`
  Second chromosome of the SV, if not on the same chromosome.
- `INFO/END`
  End position of the SV.
- `INFO/CT`
  Paired-endsignature induced connection type.
- `INFO/CIPOS`
  Confidence interval around the start point of the SV.
- `INFO/CIEND`
  Confidence interval around the end point of the SV.
- `INFO/SVMETHOD`
  The name of the caller that was used.

**Interpretation of `FORMAT` and per sample fields**

- Common
  - `GT` Genotype, written as `gt`
  - `FT` Per-genotype filter values, written as `ft`
  - `GQ` Phred-scaled genotype quality, written as `gq`
- Delly2
  - `DR` Reference pairs, written as `pec = DR + DV`
  - `DV` Variant pairs, written as `pev`
  - `RR` Reference junction count, written as `src = RR + RV`
  - `RV` Variant junction count, written as `srv`
  - `RDCN` Copy number estimate, written as `cn`
- Dragen CNV
  - `SM` Average normalized overage, written as `anc`
  - `BC` Bucket count, written as point count `pc`
  - `PE` Discordante read count at start/end, written as `pev = PE[0] + PE[1]`
- Dragen SV
  - `PR` Paired read of reference and variant, written as `pec = PR[0] + PR[1]` and `pev = PR[1]`
  - `SR` Paired read of reference and variant, written as `src = SR[0] + SR[1]` and `srv = SR[1]`
- For GATK gCNV
  - `CN` Integer copy number, written as `cn`
  - `NP` Number of points in segment, written as `np`
- Manta (equivalent to Dragen SV)
- For XHMM
    - `RD` Average normalized coveage, written as `an`

## Example

The following will create `varfish-annotator-db-1906.h2.db` and fill it.

```
# DOWNLOAD=path/to/varfish-db-downloader
# ANNOTATOR_VERSION=0.9
# ANNOTATOR_DATA_RELEASE=1907
# java -jar varfish-annotator-cli-$ANNOTATOR_VERSION-SNAPSHOT.jar \
      init-db \
      --db-release-info "varfish-annotator:v$ANNOTATOR_VERSION" \
      --db-release-info "varfish-annotator-db:r$ANNOTATOR_DATA_RELEASE" \
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

## Tests

The folder `/tests` contains some data sets that are appropriate for system (aka "end-to-end") tests of the software.

- `hg19-chr22` --
   This folder contains examples for annotating GATK HC and Delly2 calls on the first 20MB of chr22.
   Only the variants overlapping with `ADA2` and `GAB4` are used.

You can build the data sets with the `build.sh` script that is available in each folder.
This script also serves for documenting the test data's provenance.
The Jannovar software must be available as `jannovar` (e.g., through bioconda) on your `PATH` and you will need `samtools`.

## Using JDK >=18

The **tests** use [junit5-system-exit](https://github.com/tginsberg/junit5-system-exit) for detecting `System.exit()` calls.
In JDK 18 you have to use the `-Djava.security.manager=allow` flag.
Issue [tginsberg/junit5-system-exit#10](https://github.com/tginsberg/junit5-system-exit/issues/10) tracks this issue.

## Developing on Windows

There is an issue with removing temporary directories on Windows.
Apparently, HTSJDK does not properly close files.
Set `-Djunit.jupiter.tempdir.cleanup.mode.default=NEVER` to work around this issue.