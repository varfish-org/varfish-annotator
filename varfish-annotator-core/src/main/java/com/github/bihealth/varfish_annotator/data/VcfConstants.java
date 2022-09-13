package com.github.bihealth.varfish_annotator.data;

import com.google.common.collect.ImmutableList;

/** Constants to use for creating/filling VCF. */
public final class VcfConstants {

  /** Name of table with ExAC variants. */
  public static final String EXAC_PREFIX = "exac";

  /** Name of table with gnomAD exomes variants. */
  public static final String GNOMAD_EXOMES_PREFIX = "gnomad_exome";

  /** Name of table with gnomAD genomes variants. */
  public static final String GNOMAD_GENOMES_PREFIX = "gnomad_genome";

  /** Name of table with Thousand Genomes variants. */
  public static final String THOUSAND_GENOMES_PREFIX = "thousand_genomes";

  /** Header fields for the genotypes file. */
  public static final ImmutableList<String> HEADERS_GT =
      ImmutableList.of(
          "release",
          "chromosome",
          "chromosome_no",
          "start",
          "end",
          "bin",
          "reference",
          "alternative",
          "var_type",
          "case_id",
          "set_id",
          "info",
          "genotype",
          "num_hom_alt",
          "num_hom_ref",
          "num_het",
          "num_hemi_alt",
          "num_hemi_ref",
          "in_clinvar",
          "exac_frequency",
          "exac_homozygous",
          "exac_heterozygous",
          "exac_hemizygous",
          "thousand_genomes_frequency",
          "thousand_genomes_homozygous",
          "thousand_genomes_heterozygous",
          "thousand_genomes_hemizygous",
          "gnomad_exomes_frequency",
          "gnomad_exomes_homozygous",
          "gnomad_exomes_heterozygous",
          "gnomad_exomes_hemizygous",
          "gnomad_genomes_frequency",
          "gnomad_genomes_homozygous",
          "gnomad_genomes_heterozygous",
          "gnomad_genomes_hemizygous",
          "refseq_gene_id",
          "refseq_transcript_id",
          "refseq_transcript_coding",
          "refseq_hgvs_c",
          "refseq_hgvs_p",
          "refseq_effect",
          "refseq_exon_dist",
          "ensembl_gene_id",
          "ensembl_transcript_id",
          "ensembl_transcript_coding",
          "ensembl_hgvs_c",
          "ensembl_hgvs_p",
          "ensembl_effect",
          "ensembl_exon_dist");
}
