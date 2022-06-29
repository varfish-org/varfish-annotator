package com.github.bihealth.varfish_annotator.annotate;

import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import com.ginsberg.junit.exit.FailOnSystemExit;
import com.github.bihealth.varfish_annotator.ResourceUtils;
import com.github.bihealth.varfish_annotator.VarfishAnnotatorCli;
import com.github.bihealth.varfish_annotator.utils.GzipUtil;
import com.github.stefanbirkner.systemlambda.SystemLambda;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Test annotation of VCF files generated with GATK HC for GRCh37 */
public class AnnotateGatkHcVcf37Test {

  @TempDir public File tmpFolder;
  File h2DbFile;
  File fastaFile;
  File faiFile;
  File ensemblSerFile;
  File refseqSerFile;

  @BeforeEach
  void initEach() {
    h2DbFile = new File(tmpFolder + "/small-grch37.h2.db");
    fastaFile = new File(tmpFolder + "/hs37d5.1.fa");
    faiFile = new File(tmpFolder + "/hs37d5.1.fa.fai");
    ensemblSerFile = new File(tmpFolder + "/hg19_ensembl.ser");
    refseqSerFile = new File(tmpFolder + "/hg19_refseq_curated.ser");

    ResourceUtils.gunzipResourceToFile("/grch37/hs37d5.1.fa.gz", fastaFile);
    ResourceUtils.copyResourceToFile("/grch37/hs37d5.1.fa.fai", faiFile);
    ResourceUtils.copyResourceToFile("/grch37/small-grch37.h2.db", h2DbFile);
    ResourceUtils.copyResourceToFile("/grch37/hg19_ensembl.ser", ensemblSerFile);
    ResourceUtils.copyResourceToFile("/grch37/hg19_refseq_curated.ser", refseqSerFile);
  }

  void runTest(
      String inputFileName,
      String inputPath,
      String expectedDbInfos,
      String expectedGts,
      boolean gzipOutput,
      boolean selfTestChr1Only)
      throws IOException {
    final String gzSuffix = gzipOutput ? ".gz" : "";
    final File vcfPath = new File(tmpFolder + "/" + inputFileName);
    final File tbiPath = new File(vcfPath + ".tbi");
    ResourceUtils.copyResourceToFile("/" + inputPath + "/" + vcfPath.getName(), vcfPath);
    ResourceUtils.copyResourceToFile("/" + inputPath + "/" + tbiPath.getName(), tbiPath);

    final File outputDbInfoPath = new File(tmpFolder + "/output.db-info.tsv" + gzSuffix);
    final File outputGtsPath = new File(tmpFolder + "/output.gts.tsv" + gzSuffix);

    final ArrayList<String> args =
        Lists.newArrayList(
            "annotate",
            "--release",
            "GRCh37",
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
            outputGtsPath.toString());
    if (selfTestChr1Only) {
      args.add("--self-test-chr1-only");
    }
    final String[] argsArr = new String[args.size()];
    args.toArray(argsArr);
    VarfishAnnotatorCli.main(argsArr);

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

  @FailOnSystemExit
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testWithSingleton(boolean gzipOutput) throws IOException {
    final String expectedDbInfo =
        "genomebuild\tdb_name\trelease\n"
            + "GRCh37\tclinvar\ttoday\n"
            + "GRCh37\texac\tr1.0\n"
            + "GRCh37\tgnomad_exomes\tr2.1.1\n"
            + "GRCh37\tgnomad_genomes\tr2.1.1\n"
            + "GRCh37\thgmd_public\tensembl_r104\n"
            + "GRCh37\tthousand_genomes\tv5b.20130502\n";
    final String expectedGts =
        "release\tchromosome\tchromosome_no\tstart\tend\tbin\treference\talternative\tvar_type\tcase_id\tset_id\tinfo\tgenotype\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tin_clinvar\texac_frequency\texac_homozygous\texac_heterozygous\texac_hemizygous\tthousand_genomes_frequency\tthousand_genomes_homozygous\tthousand_genomes_heterozygous\tthousand_genomes_hemizygous\tgnomad_exomes_frequency\tgnomad_exomes_homozygous\tgnomad_exomes_heterozygous\tgnomad_exomes_hemizygous\tgnomad_genomes_frequency\tgnomad_genomes_homozygous\tgnomad_genomes_heterozygous\tgnomad_genomes_hemizygous\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_hgvs_c\trefseq_hgvs_p\trefseq_effect\trefseq_exon_dist\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_hgvs_c\tensembl_hgvs_p\tensembl_effect\tensembl_exon_dist\n"
            + "GRCh37\t1\t1\t13656\t13658\t585\tCAG\tC\tindel\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":4,\"\"\"dp\"\"\":4,\"\"\"gq\"\"\":12}}\t1\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0.0295331\t25\t709\t0\t0.489766\t102\t13005\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t847463\tENSG00000187634\tENST00000420190.1\tTRUE\t.\t.\t{\"intergenic_variant\"}\t846602\n"
            + "GRCh37\t1\t1\t14907\t14907\t585\tA\tG\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":6,\"\"\"dp\"\"\":17,\"\"\"gq\"\"\":99}}\t0\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0.495934\t221\t14194\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t846214\tENSG00000187634\tENST00000420190.1\tTRUE\t.\t.\t{\"intergenic_variant\"}\t845353\n"
            + "GRCh37\t1\t1\t14930\t14930\t585\tA\tG\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":5,\"\"\"dp\"\"\":15,\"\"\"gq\"\"\":99}}\t0\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0.481534\t106\t2187\t0\t0\t0\t0\t0\t0.494081\t269\t14070\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t846191\tENSG00000187634\tENST00000420190.1\tTRUE\t.\t.\t{\"intergenic_variant\"}\t845330\n"
            + "GRCh37\t1\t1\t16977\t16977\t585\tG\tA\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":8,\"\"\"dp\"\"\":17,\"\"\"gq\"\"\":99}}\t0\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t844144\tENSG00000187634\tENST00000420190.1\tTRUE\t.\t.\t{\"intergenic_variant\"}\t843283\n"
            + "GRCh37\t1\t1\t129285\t129285\t585\tG\tA\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":3,\"\"\"gq\"\"\":9}}\t1\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t731836\tENSG00000187634\tENST00000420190.1\tTRUE\t.\t.\t{\"intergenic_variant\"}\t730975\n"
            + "GRCh37\t1\t1\t745370\t745371\t590\tTA\tT\tindel\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":19,\"\"\"gq\"\"\":55}}\t0\t0\t1\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t115750\tENSG00000187634\tENST00000420190.1\tTRUE\t.\t.\t{\"intergenic_variant\"}\t114889\n"
            + "GRCh37\t1\t1\t808631\t808631\t591\tG\tA\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":13,\"\"\"dp\"\"\":13,\"\"\"gq\"\"\":39}}\t1\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t52490\tENSG00000187634\tENST00000420190.1\tTRUE\t.\t.\t{\"intergenic_variant\"}\t51629\n"
            + "GRCh37\t1\t1\t808922\t808922\t591\tG\tA\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":64,\"\"\"dp\"\"\":64,\"\"\"gq\"\"\":99}}\t1\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t52199\tENSG00000187634\tENST00000420190.1\tTRUE\t.\t.\t{\"intergenic_variant\"}\t51338\n"
            + "GRCh37\t1\t1\t808928\t808928\t591\tC\tT\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":65,\"\"\"dp\"\"\":65,\"\"\"gq\"\"\":99}}\t1\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t52193\tENSG00000187634\tENST00000420190.1\tTRUE\t.\t.\t{\"intergenic_variant\"}\t51332\n"
            + "GRCh37\t1\t1\t877769\t877769\t591\tC\tT\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":65,\"\"\"dp\"\"\":65,\"\"\"gq\"\"\":99}}\t1\t0\t0\t0\t0\tFALSE\t1.364E-5\t0\t1\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\tc.986-21C>T\tp.=\t{\"coding_transcript_intron_variant\"}\t21\tENSG00000187634\tENST00000341065.4\tTRUE\tc.709-21C>T\tp.=\t{\"coding_transcript_intron_variant\"}\t21\n"
            + "GRCh37\t1\t1\t981952\t981952\t592\tC\tT\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":3,\"\"\"gq\"\"\":9}}\t1\t0\t0\t0\t0\tTRUE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t101991\tENSG00000187634\tENST00000341065.4\tTRUE\t.\t.\t{\"intergenic_variant\"}\t101997\n";
    runTest(
        "bwa.gatk_hc.HG00102.vcf.gz",
        "input/grch37",
        expectedDbInfo,
        expectedGts,
        gzipOutput,
        true);
  }

  @FailOnSystemExit
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testWithTrio(boolean gzipOutput) throws IOException {
    final String expectedDbInfo =
        "genomebuild\tdb_name\trelease\n"
            + "GRCh37\tclinvar\ttoday\n"
            + "GRCh37\texac\tr1.0\n"
            + "GRCh37\tgnomad_exomes\tr2.1.1\n"
            + "GRCh37\tgnomad_genomes\tr2.1.1\n"
            + "GRCh37\thgmd_public\tensembl_r104\n"
            + "GRCh37\tthousand_genomes\tv5b.20130502\n";
    final String expectedGts =
        "release\tchromosome\tchromosome_no\tstart\tend\tbin\treference\talternative\tvar_type\tcase_id\tset_id\tinfo\tgenotype\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tin_clinvar\texac_frequency\texac_homozygous\texac_heterozygous\texac_hemizygous\tthousand_genomes_frequency\tthousand_genomes_homozygous\tthousand_genomes_heterozygous\tthousand_genomes_hemizygous\tgnomad_exomes_frequency\tgnomad_exomes_homozygous\tgnomad_exomes_heterozygous\tgnomad_exomes_hemizygous\tgnomad_genomes_frequency\tgnomad_genomes_homozygous\tgnomad_genomes_heterozygous\tgnomad_genomes_hemizygous\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_hgvs_c\trefseq_hgvs_p\trefseq_effect\trefseq_exon_dist\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_hgvs_c\tensembl_hgvs_p\tensembl_effect\tensembl_exon_dist\n"
            + "GRCh37\t1\t1\t858801\t858801\t591\tA\tG\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":6,\"\"\"dp\"\"\":6,\"\"\"gq\"\"\":18},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":1,\"\"\"dp\"\"\":1,\"\"\"gq\"\"\":3},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":1,\"\"\"dp\"\"\":1,\"\"\"gq\"\"\":3}}\t3\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"upstream_gene_variant\"}\t2320\tENSG00000187634\tENST00000420190.1\tTRUE\t.\t.\t{\"upstream_gene_variant\"}\t1459\n"
            + "GRCh37\t1\t1\t860416\t860416\t591\tG\tA\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":3,\"\"\"gq\"\"\":9},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1}}\t1\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"upstream_gene_variant\"}\t705\tENSG00000187634\tENST00000420190.1\tTRUE\tc.-21+88G>A\tp.=\t{\"5_prime_UTR_intron_variant\"}\t88\n"
            + "GRCh37\t1\t1\t861008\t861008\t591\tG\tC\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":4,\"\"\"dp\"\"\":4,\"\"\"gq\"\"\":12},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1}}\t1\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"upstream_gene_variant\"}\t113\tENSG00000187634\tENST00000420190.1\tTRUE\tc.-20-294G>C\tp.=\t{\"5_prime_UTR_intron_variant\"}\t294\n"
            + "GRCh37\t1\t1\t861630\t861630\t591\tG\tA\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":5,\"\"\"dp\"\"\":5,\"\"\"gq\"\"\":15},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":4,\"\"\"dp\"\"\":4,\"\"\"gq\"\"\":12},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":1,\"\"\"dp\"\"\":1,\"\"\"gq\"\"\":3}}\t3\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\tc.72+237G>A\tp.=\t{\"coding_transcript_intron_variant\"}\t237\tENSG00000187634\tENST00000342066.3\tTRUE\tc.72+237G>A\tp.=\t{\"coding_transcript_intron_variant\"}\t237\n"
            + "GRCh37\t1\t1\t861808\t861808\t591\tA\tG\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":1,\"\"\"dp\"\"\":1,\"\"\"gq\"\"\":3},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":3,\"\"\"gq\"\"\":9},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":1,\"\"\"dp\"\"\":1,\"\"\"gq\"\"\":3}}\t3\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\tc.72+415A>G\tp.=\t{\"coding_transcript_intron_variant\"}\t415\tENSG00000187634\tENST00000342066.3\tTRUE\tc.72+415A>G\tp.=\t{\"coding_transcript_intron_variant\"}\t415\n"
            + "GRCh37\t1\t1\t862093\t862093\t591\tT\tC\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":5,\"\"\"dp\"\"\":5,\"\"\"gq\"\"\":15},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":2,\"\"\"dp\"\"\":2,\"\"\"gq\"\"\":6},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":1,\"\"\"dp\"\"\":1,\"\"\"gq\"\"\":3}}\t3\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\tc.72+700T>C\tp.=\t{\"coding_transcript_intron_variant\"}\t700\tENSG00000187634\tENST00000342066.3\tTRUE\tc.72+700T>C\tp.=\t{\"coding_transcript_intron_variant\"}\t700\n"
            + "GRCh37\t1\t1\t877769\t877769\t591\tC\tT\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":3,\"\"\"gq\"\"\":9},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":6,\"\"\"dp\"\"\":6,\"\"\"gq\"\"\":18},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":6,\"\"\"gq\"\"\":99}}\t2\t0\t1\t0\t0\tFALSE\t1.364E-5\t0\t1\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\tc.986-21C>T\tp.=\t{\"coding_transcript_intron_variant\"}\t21\tENSG00000187634\tENST00000341065.4\tTRUE\tc.709-21C>T\tp.=\t{\"coding_transcript_intron_variant\"}\t21\n"
            + "GRCh37\t1\t1\t901652\t901652\t591\tA\tG\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":3,\"\"\"gq\"\"\":9},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":3,\"\"\"gq\"\"\":9},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":1,\"\"\"dp\"\"\":1,\"\"\"gq\"\"\":3}}\t3\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t21691\tENSG00000187634\tENST00000341065.4\tTRUE\t.\t.\t{\"intergenic_variant\"}\t21697\n"
            + "GRCh37\t1\t1\t903245\t903245\t591\tA\tG\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":4,\"\"\"dp\"\"\":4,\"\"\"gq\"\"\":12},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"ad\"\"\":5,\"\"\"dp\"\"\":5,\"\"\"gq\"\"\":15},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"./.\"\"\",\"\"\"ad\"\"\":-1,\"\"\"dp\"\"\":-1,\"\"\"gq\"\"\":-1}}\t2\t0\t0\t0\t0\tFALSE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t23284\tENSG00000187634\tENST00000341065.4\tTRUE\t.\t.\t{\"intergenic_variant\"}\t23290\n"
            + "GRCh37\t1\t1\t981952\t981952\t592\tC\tT\tsnv\t.\t.\t{}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":4,\"\"\"dp\"\"\":5,\"\"\"gq\"\"\":23},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"ad\"\"\":0,\"\"\"dp\"\"\":1,\"\"\"gq\"\"\":3},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":6,\"\"\"gq\"\"\":99}}\t0\t1\t2\t0\t0\tTRUE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t101991\tENSG00000187634\tENST00000341065.4\tTRUE\t.\t.\t{\"intergenic_variant\"}\t101997\n";
    runTest(
        "bwa.gatk_hc.NA12878.vcf.gz",
        "input/grch37",
        expectedDbInfo,
        expectedGts,
        gzipOutput,
        true);
  }

  @FailOnSystemExit
  @Test
  void testWithAsteriskAllele() throws IOException {
    final String expectedDbInfo =
        "genomebuild\tdb_name\trelease\n"
            + "GRCh37\tclinvar\ttoday\n"
            + "GRCh37\texac\tr1.0\n"
            + "GRCh37\tgnomad_exomes\tr2.1.1\n"
            + "GRCh37\tgnomad_genomes\tr2.1.1\n"
            + "GRCh37\thgmd_public\tensembl_r104\n"
            + "GRCh37\tthousand_genomes\tv5b.20130502\n";
    final String expectedGts =
        "release\tchromosome\tchromosome_no\tstart\tend\tbin\treference\talternative\tvar_type\tcase_id\tset_id\tinfo\tgenotype\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tin_clinvar\texac_frequency\texac_homozygous\texac_heterozygous\texac_hemizygous\tthousand_genomes_frequency\tthousand_genomes_homozygous\tthousand_genomes_heterozygous\tthousand_genomes_hemizygous\tgnomad_exomes_frequency\tgnomad_exomes_homozygous\tgnomad_exomes_heterozygous\tgnomad_exomes_hemizygous\tgnomad_genomes_frequency\tgnomad_genomes_homozygous\tgnomad_genomes_heterozygous\tgnomad_genomes_hemizygous\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_hgvs_c\trefseq_hgvs_p\trefseq_effect\trefseq_exon_dist\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_hgvs_c\tensembl_hgvs_p\tensembl_effect\tensembl_exon_dist\n"
            + "GRCh37\t1\t1\t981952\t981952\t592\tC\tT\tsnv\t.\t.\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ad\"\"\":3,\"\"\"dp\"\"\":6,\"\"\"gq\"\"\":99}}\t0\t0\t1\t0\t0\tTRUE\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t148398\tNM_152486.2\tTRUE\t.\t.\t{\"intergenic_variant\"}\t101991\tENSG00000187634\tENST00000341065.4\tTRUE\t.\t.\t{\"intergenic_variant\"}\t101997\n";
    runTest(
        "bwa.gatk_hc.HG00102.asterisk_alleles.vcf.gz",
        "input/grch37",
        expectedDbInfo,
        expectedGts,
        false,
        true);
  }

  @ExpectSystemExitWithStatus(1)
  @Test
  void testSelfTestFails() throws Exception {
    final String text =
        SystemLambda.tapSystemErr(
            () -> {
              runTest("bwa.gatk_hc.NA12878.vcf.gz", "input/grch37", null, null, false, false);
            });
    Assertions.assertTrue(text.contains("Problem with database self-test:"));
  }
}
