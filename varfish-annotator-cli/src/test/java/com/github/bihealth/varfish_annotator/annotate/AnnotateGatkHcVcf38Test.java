package com.github.bihealth.varfish_annotator.annotate;

import com.github.bihealth.varfish_annotator.ResourceUtils;
import com.github.bihealth.varfish_annotator.VarfishAnnotatorCli;
import com.github.bihealth.varfish_annotator.utils.GzipUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Test annotation of VCF files generated with GATK HC for GRCh38 */
public class AnnotateGatkHcVcf38Test {

  @TempDir public File tmpFolder;
  File h2DbFile;
  File fastaFile;
  File faiFile;
  File ensemblSerFile;
  File refseqSerFile;

  @BeforeEach
  void initEach() {
    h2DbFile = new File(tmpFolder + "/small-GRCh38.h2.db");
    fastaFile = new File(tmpFolder + "/hs38.chr1.fa");
    faiFile = new File(tmpFolder + "/hs38.chr1.fa.fai");
    ensemblSerFile = new File(tmpFolder + "/hg38_ensembl.ser");
    refseqSerFile = new File(tmpFolder + "/hg38_refseq_curated.ser");

    ResourceUtils.gunzipResourceToFile("/grch38/hs38.chr1.fa.gz", fastaFile);
    ResourceUtils.copyResourceToFile("/grch38/hs38.chr1.fa.fai", faiFile);
    ResourceUtils.copyResourceToFile("/grch38/small-grch38.h2.db", h2DbFile);
    ResourceUtils.copyResourceToFile("/grch38/hg38_ensembl.ser", ensemblSerFile);
    ResourceUtils.copyResourceToFile("/grch38/hg38_refseq_curated.ser", refseqSerFile);
  }

  void runTest(
      String inputFileName,
      String inputPath,
      String expectedDbInfos,
      String expectedGts,
      boolean gzipOutput)
      throws IOException {
    final String gzSuffix = gzipOutput ? ".gz" : "";
    final File vcfPath = new File(tmpFolder + "/" + inputFileName);
    final File tbiPath = new File(vcfPath + ".tbi");
    ResourceUtils.copyResourceToFile("/" + inputPath + "/" + vcfPath.getName(), vcfPath);
    ResourceUtils.copyResourceToFile("/" + inputPath + "/" + tbiPath.getName(), tbiPath);

    final File outputDbInfoPath = new File(tmpFolder + "/output.db-info.tsv" + gzSuffix);
    final File outputGtsPath = new File(tmpFolder + "/output.gts.tsv" + gzSuffix);

    VarfishAnnotatorCli.main(
        new String[] {
          "annotate",
          "--release",
          "GRCh38",
          "--ref-path",
          fastaFile.toString(),
          "--db-path",
          h2DbFile.toString(),
          "--input-vcf",
          vcfPath.toString(),
          "--refseq-ser-path",
          refseqSerFile.toString(),
          "--ensembl-ser-path",
          ensemblSerFile.toString(),
          "--output-db-info",
          outputDbInfoPath.toString(),
          "--output-gts",
          outputGtsPath.toString(),
        });

    if (gzipOutput) {
      final File paths[] = {outputDbInfoPath, outputGtsPath};
      for (File path : paths) {
        try (FileInputStream fin = new FileInputStream(path)) {
          Assertions.assertTrue(GzipUtil.isGZipped(fin));
        }
      }
    } else {
      Assertions.assertEquals(
          expectedDbInfos, FileUtils.readFileToString(outputDbInfoPath, "utf-8"));
      Assertions.assertEquals(expectedGts, FileUtils.readFileToString(outputGtsPath, "utf-8"));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testWithSingleton(boolean gzipOutput) throws IOException {
    final String expectedDbInfo =
        "genomebuild\tdb_name\trelease\n"
            + "GRCh38\tclinvar\ttoday\n"
            + "GRCh38\tgnomad_exomes\tr2.1.1\n"
            + "GRCh38\tgnomad_genomes\tr2.1.1\n"
            + "GRCh38\thgmd_public\tensembl_r104\n";
    final String expectedGts =
        "release\tchromosome\tchromosome_no\tstart\tend\tbin\treference\talternative\tvar_type\tcase_id\tset_id\tinfo\tgenotype\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tin_clinvar\texac_frequency\texac_homozygous\texac_heterozygous\texac_hemizygous\tthousand_genomes_frequency\tthousand_genomes_homozygous\tthousand_genomes_heterozygous\tthousand_genomes_hemizygous\tgnomad_exomes_frequency\tgnomad_exomes_homozygous\tgnomad_exomes_heterozygous\tgnomad_exomes_hemizygous\tgnomad_genomes_frequency\tgnomad_genomes_homozygous\tgnomad_genomes_heterozygous\tgnomad_genomes_hemizygous\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_hgvs_c\trefseq_hgvs_p\trefseq_effect\trefseq_exon_dist\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_hgvs_c\tensembl_hgvs_p\tensembl_effect\tensembl_exon_dist\n"
            + "GRCh38\tchr1\t1\t129285\t129285\t585\tG\tA\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":3,\"\"\"gq\"\"\":9}}\t1\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t185194\t185194\t586\tG\tC\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":12,\"\"\"gq\"\"\":49}}\t0\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t185428\t185428\t586\tA\tG\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":8,\"\"\"gq\"\"\":93}}\t0\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t185451\t185451\t586\tA\tG\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":2,\"\"\"dp\"\"\":5,\"\"\"gq\"\"\":42}}\t0\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t186536\t186536\t586\tC\tT\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":4,\"\"\"dp\"\"\":6,\"\"\"gq\"\"\":37}}\t0\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t187485\t187485\t586\tG\tA\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":7,\"\"\"dp\"\"\":24,\"\"\"gq\"\"\":99}}\t0\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t809990\t809991\t591\tTA\tT\tindel\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":19,\"\"\"gq\"\"\":55}}\t0\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t873251\t873251\t591\tG\tA\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":13,\"\"\"dp\"\"\":13,\"\"\"gq\"\"\":39}}\t1\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t930165\t930165\t592\tG\tA\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":64,\"\"\"dp\"\"\":64,\"\"\"gq\"\"\":99}}\t1\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\tc.83G>A\tp.R28Q\t{\"missense_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\tc.83G>A\tp.R28Q\t{\"missense_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t930317\t930317\t592\tA\tG\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":65,\"\"\"dp\"\"\":65,\"\"\"gq\"\"\":99}}\t1\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\tc.235A>G\tp.I79V\t{\"missense_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\tc.235A>G\tp.I79V\t{\"missense_variant\"}\t.\n";
    runTest("bwa.gatk_hc.HG00102.vcf.gz", "input/grch38", expectedDbInfo, expectedGts, gzipOutput);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testWithTrio(boolean gzipOutput) throws IOException {
    final String expectedDbInfo =
        "genomebuild\tdb_name\trelease\n"
            + "GRCh38\tclinvar\ttoday\n"
            + "GRCh38\tgnomad_exomes\tr2.1.1\n"
            + "GRCh38\tgnomad_genomes\tr2.1.1\n"
            + "GRCh38\thgmd_public\tensembl_r104\n";
    final String expectedGts =
        "release\tchromosome\tchromosome_no\tstart\tend\tbin\treference\talternative\tvar_type\tcase_id\tset_id\tinfo\tgenotype\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tin_clinvar\texac_frequency\texac_homozygous\texac_heterozygous\texac_hemizygous\tthousand_genomes_frequency\tthousand_genomes_homozygous\tthousand_genomes_heterozygous\tthousand_genomes_hemizygous\tgnomad_exomes_frequency\tgnomad_exomes_homozygous\tgnomad_exomes_heterozygous\tgnomad_exomes_hemizygous\tgnomad_genomes_frequency\tgnomad_genomes_homozygous\tgnomad_genomes_heterozygous\tgnomad_genomes_hemizygous\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_hgvs_c\trefseq_hgvs_p\trefseq_effect\trefseq_exon_dist\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_hgvs_c\tensembl_hgvs_p\tensembl_effect\tensembl_exon_dist\n"
            + "GRCh38\tchr1\t1\t55299\t55299\t585\tC\tT\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":3,\"\"\"gq\"\"\":9},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":1,\"\"\"dp\"\"\":1,\"\"\"gq\"\"\":3},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1}}\t2\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t69270\t69270\t585\tA\tG\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":3,\"\"\"gq\"\"\":9}}\t1\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0.837964\t13922\t1530\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t69511\t69511\t585\tA\tG\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"ad\"\"\":0,\"\"\"dp\"\"\":1,\"\"\"gq\"\"\":3},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":20,\"\"\"dp\"\"\":20,\"\"\"gq\"\"\":60}}\t1\t1\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0.949691\t74171\t2921\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t129285\t129285\t585\tG\tA\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":3,\"\"\"gq\"\"\":9},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":4,\"\"\"dp\"\"\":4,\"\"\"gq\"\"\":12},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1}}\t2\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t186536\t186536\t586\tC\tT\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":4,\"\"\"dp\"\"\":8,\"\"\"gq\"\"\":64},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":1,\"\"\"dp\"\"\":7,\"\"\"gq\"\"\":6},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1}}\t0\t0\t2\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t186893\t186893\t586\tT\tA\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":9,\"\"\"gq\"\"\":48},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1}}\t0\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t187019\t187019\t586\tG\tA\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":3,\"\"\"gq\"\"\":9},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":4,\"\"\"dp\"\"\":7,\"\"\"gq\"\"\":56},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1}}\t1\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t187102\t187102\t586\tC\tG\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":6,\"\"\"dp\"\"\":12,\"\"\"gq\"\"\":99},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1}}\t0\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t930165\t930165\t592\tG\tA\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":37,\"\"\"dp\"\"\":38,\"\"\"gq\"\"\":89},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":43,\"\"\"dp\"\"\":64,\"\"\"gq\"\"\":99},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1}}\t1\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\tc.83G>A\tp.R28Q\t{\"missense_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\tc.83G>A\tp.R28Q\t{\"missense_variant\"}\t.\n"
            + "GRCh38\tchr1\t1\t930317\t930317\t592\tA\tG\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":9,\"\"\"dp\"\"\":54,\"\"\"gq\"\"\":95},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"ad\"\"\":1,\"\"\"dp\"\"\":69,\"\"\"gq\"\"\":99},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1}}\t0\t1\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\tc.235A>G\tp.I79V\t{\"missense_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\tc.235A>G\tp.I79V\t{\"missense_variant\"}\t.\n";
    runTest("bwa.gatk_hc.NA12878.vcf.gz", "input/grch38", expectedDbInfo, expectedGts, gzipOutput);
  }

  @Test
  void testWithAsteriskAllele() throws IOException {
    final String expectedDbInfo =
        "genomebuild\tdb_name\trelease\n"
            + "GRCh38\tclinvar\ttoday\n"
            + "GRCh38\tgnomad_exomes\tr2.1.1\n"
            + "GRCh38\tgnomad_genomes\tr2.1.1\n"
            + "GRCh38\thgmd_public\tensembl_r104\n";
    final String expectedGts =
        "release\tchromosome\tchromosome_no\tstart\tend\tbin\treference\talternative\tvar_type\tcase_id\tset_id\tinfo\tgenotype\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tin_clinvar\texac_frequency\texac_homozygous\texac_heterozygous\texac_hemizygous\tthousand_genomes_frequency\tthousand_genomes_homozygous\tthousand_genomes_heterozygous\tthousand_genomes_hemizygous\tgnomad_exomes_frequency\tgnomad_exomes_homozygous\tgnomad_exomes_heterozygous\tgnomad_exomes_hemizygous\tgnomad_genomes_frequency\tgnomad_genomes_homozygous\tgnomad_genomes_heterozygous\tgnomad_genomes_hemizygous\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_hgvs_c\trefseq_hgvs_p\trefseq_effect\trefseq_exon_dist\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_hgvs_c\tensembl_hgvs_p\tensembl_effect\tensembl_exon_dist\n"
            + "GRCh38\tchr1\t1\t185428\t185428\t586\tA\tT\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":6,\"\"\"gq\"\"\":99}}\t0\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\tENSG00000187634\tENST00000437963.5\tTRUE\t.\t.\t{\"intergenic_variant\"}\t.\n";
    runTest(
        "bwa.gatk_hc.HG00102.asterisk_alleles.vcf.gz",
        "input/grch38",
        expectedDbInfo,
        expectedGts,
        false);
  }
}
