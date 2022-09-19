# Changelog

## v0.27 (work in progress)

- Writing out proper SV type for Dragen CNV (#76)
- Adding support for depth of coverage annotation (#73)
- Ensure output files are sorted by chromosomes (#79)

## v0.26

- Explicitely model support for SV callers (#68)
- Removing explicit support for SV2 (#67)
- Adding end-to-end tests `hg19-chr22` (#61)

## v0.25

- Fixing unresolved issue with self-test (#51, #56)

## v0.24

- Fixing problem with self-test on gnomAD genomes with chrY (#51)
- Suppress writing out `*Carriers` information in INFO column for SVs (#53)

## v0.23

**IMPORTANT COMPATIBILITY NOTE:**
For annotating SVs, the output of this version is not compatible with `varfish-server` v1.2 (Anthenea) and early versions of the v2 (Bollonaster) development branch.
You can generate compatible files by adding the `--opt-out=chrom2-columns,dbcounts-columns` as arguments to `annotate-svs`.

- Adding chrom2/count columns to output of `annotate-sv` (#41)
- Writing out BND mates for Delly (#45)

## v0.22

- Adding `db-stats` command (#25)
- Adding issue templates (#28)
- Adding continuous integration with GitHub Actions (#28)
- Adding unit tests for `annotate` command (#31)
- Adding unit tests for `annotate-sv` command (#33)
- Writing out gzip-ed files if output file name ends in `.gz` (#11)
- Do not write out asterisk alleles (#18)
- Adding tests for GRCh38 (#22)
- Adding self-tests for annotation (#27)
- Allow properly counting hemizygous variants when pedigree file is given (#36)

## v0.21

- Bumping `jannovar-core` dependency for fixes in `chrMT` annotation.

## v0.20

- Further bumping of `jannovar-core` against log4shell.
- Fixing problem with missing `SVMETHOD` annotation of SV VCFs.

## v0.19

- Bumping `jannovar-core` dependencies against log4shell vulnerability.

## v0.18

- Fixing problems with querying in GRCh38.

## v0.17

- Adding support for GRCh38 release.

## v0.16

- Fixing import of ExAC r1 VCF.
- Prevent import of REF and ALT alleles with sizes above 1000bp (field lengths in database).

## v0.15

- Properly removing ``"chr"`` prefix for data aligned to `hg19`.
- Checking whether the supported release GRCh37/hg19 was used.
  Will only allow processing of such genomes and block variants from GRCh38/hg38 which would lead to incorrect results.

## v0.14

- Bumping junit and guava dependency.
- Make compatible with new clinvar TSV file.

## v0.13

- Bumping HTSJDK dependency.
- Fixing issue with empty "AD" fields.

## v0.12

- Removing CNVetti::homdel support as it was a dead end.
- Fixing gCNV support.

## v0.11

- Adding support for gCNV output.
- Adding support for CNVetti::homdel output.

## v0.10

- Bumping Jannovar dependency (adds annotation of chrMT).
- Writing out distance to refseq/ensembl exon and adding placeholder for `info` field.
- Reading of updated clinvar and HGMD Public TSV.

## v0.9

- Writing out case and set ID into all output tables.
- Fixing clinvar import into database.
- Initializing case and set ID parameters to `"."`.

## v0.8

- Adding `chromosome_no` output column.

## v0.7

- Bumping Jannovar dependency to v0.32.
- Refactoring columns of small and structural variant call file.
- Not writing out variant effects for small variants any more.
- Writing out overall allele frequency and not `AF_POPMAX` any more.
  **This requires rebuilding the VarFish annotator database.**
- Replacing `pos` and `pos_end` in database by `start` and `end` to make it consistent with created files.
- Properly matching variants by gene without duplicates (#6).

## v0.6

- Adding support for XHMM `GT` attributes.

## v0.5

- Bumping Jannovar dependency (properly load SV type from SV2 output).
- Reading out `FORMAT` fields from SV2 output.

## v0.4

- Fixing bug that gave wrong paired read reference counts.
- BNDs are not annotated as linear variants any more, instead: `start == end`.

## v0.3

- Improved annotation of Delly2 output.

## v0.2

- Fixing bug that prevented intergenic variants from being written out.
  Now, intergenic variatns are written out properly.

## v0.1

- Everything is new.
