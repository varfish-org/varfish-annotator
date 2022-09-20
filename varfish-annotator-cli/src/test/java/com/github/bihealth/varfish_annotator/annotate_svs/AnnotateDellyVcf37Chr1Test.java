package com.github.bihealth.varfish_annotator.annotate_svs;

import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import com.ginsberg.junit.exit.FailOnSystemExit;
import com.github.bihealth.varfish_annotator.ResourceUtils;
import com.github.bihealth.varfish_annotator.VarfishAnnotatorCli;
import com.github.bihealth.varfish_annotator.utils.GzipUtil;
import com.github.stefanbirkner.systemlambda.SystemLambda;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Test annotation of VCF files generated with GATK HC for GRCh37 */
public class AnnotateDellyVcf37Chr1Test {

  @TempDir public File tmpFolder;
  File h2DbFile;
  File ensemblSerFile;
  File refseqSerFile;

  @BeforeEach
  void initEach() {
    h2DbFile = new File(tmpFolder + "/small-grch37.h2.db");
    ensemblSerFile = new File(tmpFolder + "/hg19_ensembl.ser");
    refseqSerFile = new File(tmpFolder + "/hg19_refseq_curated.ser");

    ResourceUtils.copyResourceToFile("/grch37-chr1/small-grch37.h2.db", h2DbFile);
    ResourceUtils.copyResourceToFile("/grch37-chr1/hg19_ensembl.ser", ensemblSerFile);
    ResourceUtils.copyResourceToFile("/grch37-chr1/hg19_refseq_curated.ser", refseqSerFile);

    for (String sample : ImmutableList.of("HG00102", "NA12878", "NA12891", "NA12892")) {
      ResourceUtils.copyResourceToFile(
          "/input/grch37-chr1/example." + sample + ".cov.vcf.gz",
          new File(tmpFolder + "/example." + sample + ".cov.vcf.gz"));
      ResourceUtils.copyResourceToFile(
          "/input/grch37-chr1/example." + sample + ".cov.vcf.gz.tbi",
          new File(tmpFolder + "/example." + sample + ".cov.vcf.gz.tbi"));
    }
  }

  void runTest(
      String inputFileName,
      String inputPedFileName,
      String inputPath,
      String expectedDbInfos,
      String expectedGts,
      String expectedFeatureEffects,
      List<String> covVcfs,
      boolean gzipOutput,
      boolean selfTestChr1Only,
      boolean optOutChrom2Columns,
      boolean optOutDbcountsColumns)
      throws IOException {
    final String gzSuffix = gzipOutput ? ".gz" : "";
    final File vcfPath = new File(tmpFolder + "/" + inputFileName);
    final File tbiPath = new File(vcfPath + ".tbi");
    final File pedPath = new File(tmpFolder + "/" + inputPedFileName);
    ResourceUtils.copyResourceToFile("/" + inputPath + "/" + vcfPath.getName(), vcfPath);
    ResourceUtils.copyResourceToFile("/" + inputPath + "/" + tbiPath.getName(), tbiPath);

    final File outputDbInfoPath = new File(tmpFolder + "/output.db-info.tsv" + gzSuffix);
    final File outputFeatureEffects =
        new File(tmpFolder + "/output.feature-effects.tsv" + gzSuffix);
    final File outputGtsPath = new File(tmpFolder + "/output.gts.tsv" + gzSuffix);

    final ArrayList<String> args =
        Lists.newArrayList(
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
            outputFeatureEffects.toString());
    if (selfTestChr1Only) {
      args.add("--self-test-chr1-only");
    }
    if (inputPedFileName != null) {
      args.add("--input-ped");
      args.add(pedPath.toString());
      ResourceUtils.copyResourceToFile("/" + inputPath + "/" + pedPath.getName(), pedPath);
    }
    String optOuts = "";
    if (optOutChrom2Columns) {
      optOuts += "chrom2-columns";
    }
    if (optOutDbcountsColumns) {
      if (!optOuts.isEmpty()) {
        optOuts += ",";
      }
      optOuts += "dbcounts-columns";
    }
    if (!optOuts.isEmpty()) {
      args.add("--opt-out");
      args.add(optOuts);
    }
    for (String covVcf : covVcfs) {
      args.addAll(ImmutableList.of("--coverage-vcf", tmpFolder + "/" + covVcf));
    }

    final String[] argsArr = new String[args.size()];
    args.toArray(argsArr);
    VarfishAnnotatorCli.main(argsArr);

    if (gzipOutput) {
      final File paths[] = {outputDbInfoPath, outputGtsPath, outputFeatureEffects};
      for (File path : paths) {
        try (FileInputStream fin = new FileInputStream(path)) {
          Assertions.assertTrue(GzipUtil.isGZipped(fin));
        }
      }
    } else {
      Assertions.assertEquals(
          expectedDbInfos,
          FileUtils.readFileToString(outputDbInfoPath, "utf-8").replaceAll("\\r\\n?", "\n"));
      Assertions.assertEquals(
          expectedGts,
          FileUtils.readFileToString(outputGtsPath, "utf-8").replaceAll("\\r\\n?", "\n"));
      Assertions.assertEquals(
          expectedFeatureEffects,
          FileUtils.readFileToString(outputFeatureEffects, "utf-8").replaceAll("\\r\\n?", "\n"));
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
        "release\tchromosome\tchromosome_no\tbin\tchromosome2\tchromosome_no2\tbin2\tpe_orientation\tstart\tend\tstart_ci_left\tstart_ci_right\tend_ci_left\tend_ci_right\tcase_id\tset_id\tsv_uuid\tcallers\tsv_type\tsv_sub_type\tinfo\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tgenotype\n"
            + "GRCh37\t1\t1\t599\t1\t1\t599\t3to5\t1866283\t1867170\t-56\t56\t-56\t56\t.\t.\t00000000-0000-0000-0000-000000000000\t{\"EMBL.DELLYv0.8.5\"}\tDEL\tDEL\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t0\t0\t1\t0\t0\t{\"\"\"HG00102\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"ft\"\"\":[\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":22,\"\"\"pev\"\"\":6,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\t1\t1\t604\t1\t1\t604\t5to3\t2583294\t2583895\t-624\t624\t-624\t624\t.\t.\t00000000-0000-0000-0000-000000000001\t{\"EMBL.DELLYv0.8.5\"}\tDUP\tDUP\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t0\t0\t1\t0\t0\t{\"\"\"HG00102\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"ft\"\"\":[\"\"\"MIN_PE_COUNT\"\"\",\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":80,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":13,\"\"\"pev\"\"\":3,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\t1\t1\t654\t14\t14\t654\t3to3\t9121445\t9121445\t-3\t3\t-3\t3\t.\t.\t00000000-0000-0000-0000-000000000004\t{\"EMBL.DELLYv0.8.5\"}\tBND\tBND\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"chr2\"\"\":\"\"\"14\"\"\",\"\"\"pos2\"\"\":93713177,\"\"\"unaffectedCarriers\"\"\":0}\t0\t1\t0\t0\t0\t{\"\"\"HG00102\"\"\":{\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":50,\"\"\"pev\"\"\":17,\"\"\"src\"\"\":54,\"\"\"srv\"\"\":21}}\n"
            + "GRCh37\t1\t1\t780\t1\t1\t780\t3to3\t25613682\t25614267\t-6\t6\t-6\t6\t.\t.\t00000000-0000-0000-0000-000000000002\t{\"EMBL.DELLYv0.8.5\"}\tINV\tINV\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t0\t0\t1\t0\t0\t{\"\"\"HG00102\"\"\":{\"\"\"ft\"\"\":[\"\"\"MIN_PE_AAF\"\"\",\"\"\"MIN_PE_COUNT\"\"\",\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":123,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":46,\"\"\"srv\"\"\":7}}\n"
            + "GRCh37\t14\t14\t1299\t1\t1\t1299\t3to3\t93713177\t93713177\t-3\t3\t-3\t3\t.\t.\t00000000-0000-0000-0000-000000000003\t{\"EMBL.DELLYv0.8.5\"}\tBND\tBND\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"chr2\"\"\":\"\"\"1\"\"\",\"\"\"pos2\"\"\":9121445,\"\"\"unaffectedCarriers\"\"\":0}\t0\t0\t1\t0\t0\t{\"\"\"HG00102\"\"\":{\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":50,\"\"\"pev\"\"\":17,\"\"\"src\"\"\":54,\"\"\"srv\"\"\":21}}\n";
    final String expectedFeatureEffects =
        "case_id\tset_id\tsv_uuid\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_effect\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_effect\n";
    runTest(
        "bwa.delly2.HG00102.vcf.gz",
        null,
        "input/grch37-chr1",
        expectedDbInfo,
        expectedGts,
        expectedFeatureEffects,
        ImmutableList.of("example.HG00102.cov.vcf.gz"),
        gzipOutput,
        true,
        false,
        false);
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
        "release\tchromosome\tchromosome_no\tbin\tchromosome2\tchromosome_no2\tbin2\tpe_orientation\tstart\tend\tstart_ci_left\tstart_ci_right\tend_ci_left\tend_ci_right\tcase_id\tset_id\tsv_uuid\tcallers\tsv_type\tsv_sub_type\tinfo\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tgenotype\n"
            + "GRCh37\t1\t1\t599\t1\t1\t599\t3to5\t1866283\t1867170\t-56\t56\t-56\t56\t.\t.\t00000000-0000-0000-0000-000000000000\t{\"EMBL.DELLYv0.8.5\"}\tDEL\tDEL\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t0\t1\t2\t0\t0\t{\"\"\"NA12878\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"ft\"\"\":[\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":22,\"\"\"pev\"\"\":6,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0},\"\"\"NA12891\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"ft\"\"\":[\"\"\"MAX_PE_AAF\"\"\"],\"\"\"gq\"\"\":127,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":11,\"\"\"pev\"\"\":4,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0},\"\"\"NA12892\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"gq\"\"\":57,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":19,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\t1\t1\t604\t1\t1\t604\t5to3\t2583294\t2583895\t-624\t624\t-624\t624\t.\t.\t00000000-0000-0000-0000-000000000001\t{\"EMBL.DELLYv0.8.5\"}\tDUP\tDUP\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t0\t2\t1\t0\t0\t{\"\"\"NA12878\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"ft\"\"\":[\"\"\"MIN_PE_COUNT\"\"\",\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":80,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":13,\"\"\"pev\"\"\":3,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0},\"\"\"NA12891\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"gq\"\"\":24,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":12,\"\"\"pev\"\"\":1,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0},\"\"\"NA12892\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"gq\"\"\":39,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":13,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\t1\t1\t654\t14\t14\t654\t3to3\t9121445\t9121445\t-3\t3\t-3\t3\t.\t.\t00000000-0000-0000-0000-000000000004\t{\"EMBL.DELLYv0.8.5\"}\tBND\tBND\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"chr2\"\"\":\"\"\"14\"\"\",\"\"\"pos2\"\"\":93713177,\"\"\"unaffectedCarriers\"\"\":0}\t0\t3\t0\t0\t0\t{\"\"\"NA12878\"\"\":{\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":50,\"\"\"pev\"\"\":17,\"\"\"src\"\"\":54,\"\"\"srv\"\"\":21},\"\"\"NA12891\"\"\":{\"\"\"gq\"\"\":93,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":31,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":31,\"\"\"srv\"\"\":0},\"\"\"NA12892\"\"\":{\"\"\"ft\"\"\":[\"\"\"MAX_PE_AAF\"\"\",\"\"\"MAX_PE_COUNT\"\"\",\"\"\"MAX_SR_AAF\"\"\",\"\"\"MAX_SR_COUNT\"\"\"],\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":39,\"\"\"pev\"\"\":15,\"\"\"src\"\"\":37,\"\"\"srv\"\"\":18}}\n"
            + "GRCh37\t1\t1\t780\t1\t1\t780\t3to3\t25613682\t25614267\t-6\t6\t-6\t6\t.\t.\t00000000-0000-0000-0000-000000000002\t{\"EMBL.DELLYv0.8.5\"}\tINV\tINV\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t0\t0\t3\t0\t0\t{\"\"\"NA12878\"\"\":{\"\"\"ft\"\"\":[\"\"\"MIN_PE_AAF\"\"\",\"\"\"MIN_PE_COUNT\"\"\",\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":123,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":46,\"\"\"srv\"\"\":7},\"\"\"NA12891\"\"\":{\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":43,\"\"\"srv\"\"\":8},\"\"\"NA12892\"\"\":{\"\"\"gq\"\"\":157,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":56,\"\"\"srv\"\"\":8}}\n"
            + "GRCh37\t14\t14\t1299\t1\t1\t1299\t3to3\t93713177\t93713177\t-3\t3\t-3\t3\t.\t.\t00000000-0000-0000-0000-000000000003\t{\"EMBL.DELLYv0.8.5\"}\tBND\tBND\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"chr2\"\"\":\"\"\"1\"\"\",\"\"\"pos2\"\"\":9121445,\"\"\"unaffectedCarriers\"\"\":0}\t0\t1\t2\t0\t0\t{\"\"\"NA12878\"\"\":{\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":50,\"\"\"pev\"\"\":17,\"\"\"src\"\"\":54,\"\"\"srv\"\"\":21},\"\"\"NA12891\"\"\":{\"\"\"gq\"\"\":93,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":31,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":31,\"\"\"srv\"\"\":0},\"\"\"NA12892\"\"\":{\"\"\"ft\"\"\":[\"\"\"MAX_PE_AAF\"\"\",\"\"\"MAX_PE_COUNT\"\"\",\"\"\"MAX_SR_AAF\"\"\",\"\"\"MAX_SR_COUNT\"\"\"],\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":39,\"\"\"pev\"\"\":15,\"\"\"src\"\"\":37,\"\"\"srv\"\"\":18}}\n";
    final String expectedFeatureEffects =
        "case_id\tset_id\tsv_uuid\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_effect\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_effect\n";
    runTest(
        "bwa.delly2.NA12878.vcf.gz",
        null,
        "input/grch37-chr1",
        expectedDbInfo,
        expectedGts,
        expectedFeatureEffects,
        ImmutableList.of(
            "example.NA12878.cov.vcf.gz",
            "example.NA12891.cov.vcf.gz",
            "example.NA12892.cov.vcf.gz"),
        gzipOutput,
        true,
        false,
        false);
  }

  @ExpectSystemExitWithStatus(1)
  @Test
  void testSelfTestFails() throws Exception {
    final String text =
        SystemLambda.tapSystemErr(
            () -> {
              runTest(
                  "bwa.delly2.NA12878.vcf.gz",
                  null,
                  "input/grch37-chr1",
                  null,
                  null,
                  null,
                  ImmutableList.of(
                      "example.NA12878.cov.vcf.gz",
                      "example.NA12891.cov.vcf.gz",
                      "example.NA12892.cov.vcf.gz"),
                  false,
                  false,
                  false,
                  false);
            });
    Assertions.assertTrue(text.contains("Problem with database self-test:"));
  }

  @FailOnSystemExit
  @Test
  void testAnnotateHemiMale() throws Exception {
    final String expectedDbInfo =
        "genomebuild\tdb_name\trelease\n"
            + "GRCh37\tclinvar\ttoday\n"
            + "GRCh37\texac\tr1.0\n"
            + "GRCh37\tgnomad_exomes\tr2.1.1\n"
            + "GRCh37\tgnomad_genomes\tr2.1.1\n"
            + "GRCh37\thgmd_public\tensembl_r104\n"
            + "GRCh37\tthousand_genomes\tv5b.20130502\n";
    final String expectedGts =
        "release\tchromosome\tchromosome_no\tbin\tchromosome2\tchromosome_no2\tbin2\tpe_orientation\tstart\tend\tstart_ci_left\tstart_ci_right\tend_ci_left\tend_ci_right\tcase_id\tset_id\tsv_uuid\tcallers\tsv_type\tsv_sub_type\tinfo\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tgenotype\n"
            + "GRCh37\tX\t23\t592\tX\t23\t592\t3to5\t1000000\t1001000\t-56\t56\t-56\t56\t.\t.\t00000000-0000-0000-0000-000000000000\t{\"EMBL.DELLYv0.8.5\"}\tDEL\tDEL\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t1\t0\t0\t0\t0\t{\"\"\"HG00102\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"ft\"\"\":[\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"pec\"\"\":22,\"\"\"pev\"\"\":6,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\tX\t23\t607\tX\t23\t607\t5to3\t3000000\t3001000\t-624\t624\t-624\t624\t.\t.\t00000000-0000-0000-0000-000000000001\t{\"EMBL.DELLYv0.8.5\"}\tDUP\tDUP\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t0\t0\t0\t1\t0\t{\"\"\"HG00102\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"ft\"\"\":[\"\"\"MIN_PE_COUNT\"\"\",\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":80,\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"pec\"\"\":13,\"\"\"pev\"\"\":3,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n";
    final String expectedFeatureEffects =
        "case_id\tset_id\tsv_uuid\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_effect\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_effect\n";
    runTest(
        "bwa.delly2.HG00102.hemi.vcf.gz",
        "FAM_HG00102.ped",
        "input/grch37-chr1",
        expectedDbInfo,
        expectedGts,
        expectedFeatureEffects,
        ImmutableList.of("example.HG00102.cov.vcf.gz"),
        false,
        true,
        false,
        false);
  }

  @FailOnSystemExit
  @Test
  void testAnnotateHemiFamily() throws Exception {
    final String expectedDbInfo =
        "genomebuild\tdb_name\trelease\n"
            + "GRCh37\tclinvar\ttoday\n"
            + "GRCh37\texac\tr1.0\n"
            + "GRCh37\tgnomad_exomes\tr2.1.1\n"
            + "GRCh37\tgnomad_genomes\tr2.1.1\n"
            + "GRCh37\thgmd_public\tensembl_r104\n"
            + "GRCh37\tthousand_genomes\tv5b.20130502\n";
    final String expectedGts =
        "release\tchromosome\tchromosome_no\tbin\tchromosome2\tchromosome_no2\tbin2\tpe_orientation\tstart\tend\tstart_ci_left\tstart_ci_right\tend_ci_left\tend_ci_right\tcase_id\tset_id\tsv_uuid\tcallers\tsv_type\tsv_sub_type\tinfo\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tgenotype\n"
            + "GRCh37\tX\t23\t592\tX\t23\t592\t3to5\t1000000\t1001000\t-56\t56\t-56\t56\t.\t.\t00000000-0000-0000-0000-000000000000\t{\"EMBL.DELLYv0.8.5\"}\tDEL\tDEL\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t1\t1\t1\t0\t0\t{\"\"\"NA12878\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"ft\"\"\":[\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"pec\"\"\":22,\"\"\"pev\"\"\":6,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0},\"\"\"NA12891\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"ft\"\"\":[\"\"\"MAX_PE_AAF\"\"\"],\"\"\"gq\"\"\":127,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":11,\"\"\"pev\"\"\":4,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0},\"\"\"NA12892\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"gq\"\"\":57,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":19,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\tX\t23\t607\tX\t23\t607\t5to3\t3000000\t3001000\t-624\t624\t-624\t624\t.\t.\t00000000-0000-0000-0000-000000000001\t{\"EMBL.DELLYv0.8.5\"}\tDUP\tDUP\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t1\t0\t0\t0\t2\t{\"\"\"NA12878\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"ft\"\"\":[\"\"\"MIN_PE_COUNT\"\"\",\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":80,\"\"\"gt\"\"\":\"\"\"1/1\"\"\",\"\"\"pec\"\"\":13,\"\"\"pev\"\"\":3,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0},\"\"\"NA12891\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"gq\"\"\":24,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":12,\"\"\"pev\"\"\":1,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0},\"\"\"NA12892\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"gq\"\"\":39,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":13,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n";
    final String expectedFeatureEffects =
        "case_id\tset_id\tsv_uuid\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_effect\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_effect\n";
    runTest(
        "bwa.delly2.NA12878.hemi.vcf.gz",
        "FAM_NA12878.ped",
        "input/grch37-chr1",
        expectedDbInfo,
        expectedGts,
        expectedFeatureEffects,
        ImmutableList.of(
            "example.NA12878.cov.vcf.gz",
            "example.NA12891.cov.vcf.gz",
            "example.NA12892.cov.vcf.gz"),
        false,
        true,
        false,
        false);
  }

  @FailOnSystemExit
  @Test
  void testWithSingletonOptOutChrom2() throws IOException {
    final String expectedDbInfo =
        "genomebuild\tdb_name\trelease\n"
            + "GRCh37\tclinvar\ttoday\n"
            + "GRCh37\texac\tr1.0\n"
            + "GRCh37\tgnomad_exomes\tr2.1.1\n"
            + "GRCh37\tgnomad_genomes\tr2.1.1\n"
            + "GRCh37\thgmd_public\tensembl_r104\n"
            + "GRCh37\tthousand_genomes\tv5b.20130502\n";
    final String expectedGts =
        "release\tchromosome\tchromosome_no\tbin\tstart\tend\tstart_ci_left\tstart_ci_right\tend_ci_left\tend_ci_right\tcase_id\tset_id\tsv_uuid\tcallers\tsv_type\tsv_sub_type\tinfo\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tgenotype\n"
            + "GRCh37\t1\t1\t599\t1866283\t1867170\t-56\t56\t-56\t56\t.\t.\t00000000-0000-0000-0000-000000000000\t{\"EMBL.DELLYv0.8.5\"}\tDEL\tDEL\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t0\t0\t1\t0\t0\t{\"\"\"HG00102\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"ft\"\"\":[\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":22,\"\"\"pev\"\"\":6,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\t1\t1\t604\t2583294\t2583895\t-624\t624\t-624\t624\t.\t.\t00000000-0000-0000-0000-000000000001\t{\"EMBL.DELLYv0.8.5\"}\tDUP\tDUP\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t0\t0\t1\t0\t0\t{\"\"\"HG00102\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"ft\"\"\":[\"\"\"MIN_PE_COUNT\"\"\",\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":80,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":13,\"\"\"pev\"\"\":3,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\t1\t1\t654\t9121445\t9121445\t-3\t3\t-3\t3\t.\t.\t00000000-0000-0000-0000-000000000004\t{\"EMBL.DELLYv0.8.5\"}\tBND\tBND\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"chr2\"\"\":\"\"\"14\"\"\",\"\"\"pos2\"\"\":93713177,\"\"\"unaffectedCarriers\"\"\":0}\t0\t1\t0\t0\t0\t{\"\"\"HG00102\"\"\":{\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":50,\"\"\"pev\"\"\":17,\"\"\"src\"\"\":54,\"\"\"srv\"\"\":21}}\n"
            + "GRCh37\t1\t1\t780\t25613682\t25614267\t-6\t6\t-6\t6\t.\t.\t00000000-0000-0000-0000-000000000002\t{\"EMBL.DELLYv0.8.5\"}\tINV\tINV\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t0\t0\t1\t0\t0\t{\"\"\"HG00102\"\"\":{\"\"\"ft\"\"\":[\"\"\"MIN_PE_AAF\"\"\",\"\"\"MIN_PE_COUNT\"\"\",\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":123,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":46,\"\"\"srv\"\"\":7}}\n"
            + "GRCh37\t14\t14\t1299\t93713177\t93713177\t-3\t3\t-3\t3\t.\t.\t00000000-0000-0000-0000-000000000003\t{\"EMBL.DELLYv0.8.5\"}\tBND\tBND\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"chr2\"\"\":\"\"\"1\"\"\",\"\"\"pos2\"\"\":9121445,\"\"\"unaffectedCarriers\"\"\":0}\t0\t0\t1\t0\t0\t{\"\"\"HG00102\"\"\":{\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":50,\"\"\"pev\"\"\":17,\"\"\"src\"\"\":54,\"\"\"srv\"\"\":21}}\n";
    final String expectedFeatureEffects =
        "case_id\tset_id\tsv_uuid\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_effect\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_effect\n";
    runTest(
        "bwa.delly2.HG00102.vcf.gz",
        null,
        "input/grch37-chr1",
        expectedDbInfo,
        expectedGts,
        expectedFeatureEffects,
        ImmutableList.of("example.HG00102.cov.vcf.gz"),
        false,
        true,
        true,
        false);
  }

  @FailOnSystemExit
  @Test
  void testWithSingletonOptOutDbCounts() throws IOException {
    final String expectedDbInfo =
        "genomebuild\tdb_name\trelease\n"
            + "GRCh37\tclinvar\ttoday\n"
            + "GRCh37\texac\tr1.0\n"
            + "GRCh37\tgnomad_exomes\tr2.1.1\n"
            + "GRCh37\tgnomad_genomes\tr2.1.1\n"
            + "GRCh37\thgmd_public\tensembl_r104\n"
            + "GRCh37\tthousand_genomes\tv5b.20130502\n";
    final String expectedGts =
        "release\tchromosome\tchromosome_no\tbin\tchromosome2\tchromosome_no2\tbin2\tpe_orientation\tstart\tend\tstart_ci_left\tstart_ci_right\tend_ci_left\tend_ci_right\tcase_id\tset_id\tsv_uuid\tcallers\tsv_type\tsv_sub_type\tinfo\tgenotype\n"
            + "GRCh37\t1\t1\t599\t1\t1\t599\t3to5\t1866283\t1867170\t-56\t56\t-56\t56\t.\t.\t00000000-0000-0000-0000-000000000000\t{\"EMBL.DELLYv0.8.5\"}\tDEL\tDEL\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"ft\"\"\":[\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":22,\"\"\"pev\"\"\":6,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\t1\t1\t604\t1\t1\t604\t5to3\t2583294\t2583895\t-624\t624\t-624\t624\t.\t.\t00000000-0000-0000-0000-000000000001\t{\"EMBL.DELLYv0.8.5\"}\tDUP\tDUP\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"amq\"\"\":40,\"\"\"anc\"\"\":1.0,\"\"\"ft\"\"\":[\"\"\"MIN_PE_COUNT\"\"\",\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":80,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":13,\"\"\"pev\"\"\":3,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\t1\t1\t654\t14\t14\t654\t3to3\t9121445\t9121445\t-3\t3\t-3\t3\t.\t.\t00000000-0000-0000-0000-000000000004\t{\"EMBL.DELLYv0.8.5\"}\tBND\tBND\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":50,\"\"\"pev\"\"\":17,\"\"\"src\"\"\":54,\"\"\"srv\"\"\":21}}\n"
            + "GRCh37\t1\t1\t780\t1\t1\t780\t3to3\t25613682\t25614267\t-6\t6\t-6\t6\t.\t.\t00000000-0000-0000-0000-000000000002\t{\"EMBL.DELLYv0.8.5\"}\tINV\tINV\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"ft\"\"\":[\"\"\"MIN_PE_AAF\"\"\",\"\"\"MIN_PE_COUNT\"\"\",\"\"\"MIN_SR_AAF\"\"\",\"\"\"MIN_SR_COUNT\"\"\"],\"\"\"gq\"\"\":123,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":46,\"\"\"srv\"\"\":7}}\n"
            + "GRCh37\t14\t14\t1299\t1\t1\t1299\t3to3\t93713177\t93713177\t-3\t3\t-3\t3\t.\t.\t00000000-0000-0000-0000-000000000003\t{\"EMBL.DELLYv0.8.5\"}\tBND\tBND\t{}\t{\"\"\"HG00102\"\"\":{\"\"\"gq\"\"\":10000,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":50,\"\"\"pev\"\"\":17,\"\"\"src\"\"\":54,\"\"\"srv\"\"\":21}}\n";
    final String expectedFeatureEffects =
        "case_id\tset_id\tsv_uuid\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_effect\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_effect\n";
    runTest(
        "bwa.delly2.HG00102.vcf.gz",
        null,
        "input/grch37-chr1",
        expectedDbInfo,
        expectedGts,
        expectedFeatureEffects,
        ImmutableList.of("example.HG00102.cov.vcf.gz"),
        false,
        true,
        false,
        true);
  }

  @FailOnSystemExit
  @Test
  void testWithSingletonBnd() throws IOException {
    final String expectedDbInfo =
        "genomebuild\tdb_name\trelease\n"
            + "GRCh37\tclinvar\ttoday\n"
            + "GRCh37\texac\tr1.0\n"
            + "GRCh37\tgnomad_exomes\tr2.1.1\n"
            + "GRCh37\tgnomad_genomes\tr2.1.1\n"
            + "GRCh37\thgmd_public\tensembl_r104\n"
            + "GRCh37\tthousand_genomes\tv5b.20130502\n";
    final String expectedGts =
        "release\tchromosome\tchromosome_no\tbin\tchromosome2\tchromosome_no2\tbin2\tpe_orientation\tstart\tend\tstart_ci_left\tstart_ci_right\tend_ci_left\tend_ci_right\tcase_id\tset_id\tsv_uuid\tcallers\tsv_type\tsv_sub_type\tinfo\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tgenotype\n"
            + "GRCh37\t1\t1\t2486\t2\t2\t2486\t5to3\t249239891\t249239891\t-248\t248\t-248\t248\t.\t.\t00000000-0000-0000-0000-000000000001\t{\"EMBL.DELLYv0.8.1\"}\tBND\tBND\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"chr2\"\"\":\"\"\"2\"\"\",\"\"\"pos2\"\"\":10265,\"\"\"unaffectedCarriers\"\"\":0}\t0\t1\t0\t0\t0\t{\"\"\"HG00102\"\"\":{\"\"\"gq\"\"\":23,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":10,\"\"\"pev\"\"\":3,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n"
            + "GRCh37\t2\t2\t585\t1\t1\t585\t3to5\t10265\t10265\t-248\t248\t-248\t248\t.\t.\t00000000-0000-0000-0000-000000000000\t{\"EMBL.DELLYv0.8.1\"}\tBND\tBND\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"chr2\"\"\":\"\"\"1\"\"\",\"\"\"pos2\"\"\":249239891,\"\"\"unaffectedCarriers\"\"\":0}\t0\t0\t1\t0\t0\t{\"\"\"HG00102\"\"\":{\"\"\"gq\"\"\":23,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":10,\"\"\"pev\"\"\":3,\"\"\"src\"\"\":0,\"\"\"srv\"\"\":0}}\n";
    final String expectedFeatureEffects =
        "case_id\tset_id\tsv_uuid\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_effect\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_effect\n";
    runTest(
        "bwa.delly2.HG00102.bnd.vcf.gz",
        null,
        "input/grch37-chr1",
        expectedDbInfo,
        expectedGts,
        expectedFeatureEffects,
        ImmutableList.of("example.HG00102.cov.vcf.gz"),
        false,
        true,
        false,
        false);
  }
}
