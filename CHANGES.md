# Changelog

## v0.7 (unreleased)

- Bumping Jannovar dependency to v0.32.
- Refactoring columns of small and structural variant call file.
- Not writing out variant effects for small variants any more.
- Writing out overall allele frequency and not `AF_POPMAX` any more.
  **This requires rebuilding the VarFish annotator database.**
- Replacing `pos` and `pos_end` in database by `start` and `end` to make it consistent with created files.

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
