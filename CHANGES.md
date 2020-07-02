# Changelog

## HEAD (unreleased)

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
