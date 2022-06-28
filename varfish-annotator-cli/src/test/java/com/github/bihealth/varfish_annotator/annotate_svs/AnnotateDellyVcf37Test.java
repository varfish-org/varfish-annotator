package com.github.bihealth.varfish_annotator.annotate_svs;

import com.ginsberg.junit.exit.FailOnSystemExit;
import com.github.bihealth.varfish_annotator.ResourceUtils;
import com.github.bihealth.varfish_annotator.VarfishAnnotatorCli;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Test annotation of VCF files generated with GATK HC for GRCh37 */
public class AnnotateDellyVcf37Test {

  @TempDir public File tmpFolder;
  File h2DbFile;
  File ensemblSerFile;
  File refseqSerFile;

  @BeforeEach
  void initEach() {
    h2DbFile = new File(tmpFolder + "/small-grch37.h2.db");
    ensemblSerFile = new File(tmpFolder + "/hg19_ensembl.ser");
    refseqSerFile = new File(tmpFolder + "/hg19_refseq_curated.ser");

    ResourceUtils.copyResourceToFile("/grch37/small-grch37.h2.db", h2DbFile);
    ResourceUtils.copyResourceToFile("/grch37/hg19_ensembl.ser", ensemblSerFile);
    ResourceUtils.copyResourceToFile("/grch37/hg19_refseq_curated.ser", refseqSerFile);
  }

  void runTest(
      String inputFileName,
      String inputPath,
      String expectedDbInfos,
      String expectedGts,
      String expectedFeatureEffects)
      throws IOException {
    final File vcfPath = new File(tmpFolder + "/" + inputFileName);
    final File tbiPath = new File(vcfPath + ".tbi");
    ResourceUtils.copyResourceToFile("/" + inputPath + "/" + vcfPath.getName(), vcfPath);
    ResourceUtils.copyResourceToFile("/" + inputPath + "/" + tbiPath.getName(), tbiPath);

    final File outputDbInfoPath = new File(tmpFolder + "/output.db-info.tsv");
    final File outputFeatureEffects = new File(tmpFolder + "/output.feature-effects.tsv");
    final File outputGtsPath = new File(tmpFolder + "/output.gts.tsv");

    VarfishAnnotatorCli.main(
        new String[] {
          "annotate-svs",
          "--release",
          "GRCh37",
          "--sequential-uuids",
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
          "--output-feature-effects",
          outputFeatureEffects.toString(),
        });

    Assertions.assertEquals(expectedDbInfos, FileUtils.readFileToString(outputDbInfoPath, "utf-8"));
    Assertions.assertEquals(expectedGts, FileUtils.readFileToString(outputGtsPath, "utf-8"));
    Assertions.assertEquals(
        expectedFeatureEffects, FileUtils.readFileToString(outputFeatureEffects, "utf-8"));
  }

  @Test
  @FailOnSystemExit
  void testWithSingleton() throws IOException {
    final String expectedDbInfo =
        "genomebuild\tdb_name\trelease\n"
            + "GRCh37\tclinvar\ttoday\n"
            + "GRCh37\texac\tr1.0\n"
            + "GRCh37\tgnomad_exomes\tr2.1.1\n"
            + "GRCh37\tgnomad_genomes\tr2.1.1\n"
            + "GRCh37\thgmd_public\tensembl_r104\n"
            + "GRCh37\tthousand_genomes\tv5b.20130502\n";
    final String expectedGts =
        "release\tchromosome\tchromosome_no\tstart\tend\tbin\tstart_ci_left\tstart_ci_right\tend_ci_left\tend_ci_right\tcase_id\tset_id\tsv_uuid\tcaller\tsv_type\tsv_sub_type\tinfo\tgenotype\n"
            + "GRCh37\t1\t1\t1866283\t1867170\t599\t-56\t56\t-56\t56\t.\t.\t00000000-0000-0000-0000-000000000000\tEMBL.DELLYv0.8.5\tDEL\tDEL\t{\"\"\"backgroundCarriers\"\"\":0,\"\"\"affectedCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":10000,\"\"\"pec\"\"\":22,\"\"\"pev\"\"\":6,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\t1\t1\t2583294\t2583895\t604\t-624\t624\t-624\t624\t.\t.\t00000000-0000-0000-0000-000000000001\tEMBL.DELLYv0.8.5\tDUP\tDUP\t{\"\"\"backgroundCarriers\"\"\":0,\"\"\"affectedCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":80,\"\"\"pec\"\"\":13,\"\"\"pev\"\"\":3,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\t1\t1\t25613682\t25614267\t780\t-6\t6\t-6\t6\t.\t.\t00000000-0000-0000-0000-000000000002\tEMBL.DELLYv0.8.5\tINV\tINV\t{\"\"\"backgroundCarriers\"\"\":0,\"\"\"affectedCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":123,\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":46,\"\"\"srv\"\"\":7}}\n"
            + "GRCh37\t14\t14\t93713177\t93713177\t1299\t-3\t3\t-3\t3\t.\t.\t00000000-0000-0000-0000-000000000003\tEMBL.DELLYv0.8.5\tBND\tBND\t{\"\"\"chr2\"\"\":\"\"\"1\"\"\",\"\"\"pos2\"\"\":9121445,\"\"\"backgroundCarriers\"\"\":0,\"\"\"affectedCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t{\"\"\"HG00102\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":10000,\"\"\"pec\"\"\":50,\"\"\"pev\"\"\":17,\"\"\"src\"\"\":54,\"\"\"srv\"\"\":21}}\n";
    final String expectedFeatureEffects =
        "case_id\tset_id\tsv_uuid\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_effect\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_effect\n";
    runTest(
        "bwa.delly2.HG00102.vcf.gz",
        "input/grch37",
        expectedDbInfo,
        expectedGts,
        expectedFeatureEffects);
  }

  @Test
  @FailOnSystemExit
  void testWithTrio() throws IOException {
    final String expectedDbInfo =
        "genomebuild\tdb_name\trelease\n"
            + "GRCh37\tclinvar\ttoday\n"
            + "GRCh37\texac\tr1.0\n"
            + "GRCh37\tgnomad_exomes\tr2.1.1\n"
            + "GRCh37\tgnomad_genomes\tr2.1.1\n"
            + "GRCh37\thgmd_public\tensembl_r104\n"
            + "GRCh37\tthousand_genomes\tv5b.20130502\n";
    final String expectedGts =
        "release\tchromosome\tchromosome_no\tstart\tend\tbin\tstart_ci_left\tstart_ci_right\tend_ci_left\tend_ci_right\tcase_id\tset_id\tsv_uuid\tcaller\tsv_type\tsv_sub_type\tinfo\tgenotype\n"
            + "GRCh37\t1\t1\t1866283\t1867170\t599\t-56\t56\t-56\t56\t.\t.\t00000000-0000-0000-0000-000000000000\tEMBL.DELLYv0.8.5\tDEL\tDEL\t{\"\"\"backgroundCarriers\"\"\":0,\"\"\"affectedCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":10000,\"\"\"pec\"\"\":22,\"\"\"pev\"\"\":6,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":10000,\"\"\"pec\"\"\":22,\"\"\"pev\"\"\":6,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":127,\"\"\"pec\"\"\":11,\"\"\"pev\"\"\":4,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":10000,\"\"\"pec\"\"\":22,\"\"\"pev\"\"\":6,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":127,\"\"\"pec\"\"\":11,\"\"\"pev\"\"\":4,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"gq\"\"\":57,\"\"\"pec\"\"\":19,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\t1\t1\t2583294\t2583895\t604\t-624\t624\t-624\t624\t.\t.\t00000000-0000-0000-0000-000000000001\tEMBL.DELLYv0.8.5\tDUP\tDUP\t{\"\"\"backgroundCarriers\"\"\":0,\"\"\"affectedCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":80,\"\"\"pec\"\"\":13,\"\"\"pev\"\"\":3,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":80,\"\"\"pec\"\"\":13,\"\"\"pev\"\"\":3,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"gq\"\"\":24,\"\"\"pec\"\"\":12,\"\"\"pev\"\"\":1,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":80,\"\"\"pec\"\"\":13,\"\"\"pev\"\"\":3,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"gq\"\"\":24,\"\"\"pec\"\"\":12,\"\"\"pev\"\"\":1,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"gq\"\"\":39,\"\"\"pec\"\"\":13,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\t1\t1\t25613682\t25614267\t780\t-6\t6\t-6\t6\t.\t.\t00000000-0000-0000-0000-000000000002\tEMBL.DELLYv0.8.5\tINV\tINV\t{\"\"\"backgroundCarriers\"\"\":0,\"\"\"affectedCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":123,\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":46,\"\"\"srv\"\"\":7},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":123,\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":46,\"\"\"srv\"\"\":7,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":10000,\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":43,\"\"\"srv\"\"\":8},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":123,\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":46,\"\"\"srv\"\"\":7,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":10000,\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":43,\"\"\"srv\"\"\":8,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":157,\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":56,\"\"\"srv\"\"\":8}}\n"
            + "GRCh37\t14\t14\t93713177\t93713177\t1299\t-3\t3\t-3\t3\t.\t.\t00000000-0000-0000-0000-000000000003\tEMBL.DELLYv0.8.5\tBND\tBND\t{\"\"\"chr2\"\"\":\"\"\"1\"\"\",\"\"\"pos2\"\"\":9121445,\"\"\"backgroundCarriers\"\"\":0,\"\"\"affectedCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t{\"\"\"NA12878\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":10000,\"\"\"pec\"\"\":50,\"\"\"pev\"\"\":17,\"\"\"src\"\"\":54,\"\"\"srv\"\"\":21},\"\"\"NA12891\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":10000,\"\"\"pec\"\"\":50,\"\"\"pev\"\"\":17,\"\"\"src\"\"\":54,\"\"\"srv\"\"\":21,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"gq\"\"\":93,\"\"\"pec\"\"\":31,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":31,\"\"\"srv\"\"\":0},\"\"\"NA12892\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":10000,\"\"\"pec\"\"\":50,\"\"\"pev\"\"\":17,\"\"\"src\"\"\":54,\"\"\"srv\"\"\":21,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"gq\"\"\":93,\"\"\"pec\"\"\":31,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":31,\"\"\"srv\"\"\":0,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"gq\"\"\":10000,\"\"\"pec\"\"\":39,\"\"\"pev\"\"\":15,\"\"\"src\"\"\":37,\"\"\"srv\"\"\":18}}\n";
    final String expectedFeatureEffects =
        "case_id\tset_id\tsv_uuid\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_effect\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_effect\n";
    runTest(
        "bwa.delly2.NA12878.vcf.gz",
        "input/grch37",
        expectedDbInfo,
        expectedGts,
        expectedFeatureEffects);
  }
}
