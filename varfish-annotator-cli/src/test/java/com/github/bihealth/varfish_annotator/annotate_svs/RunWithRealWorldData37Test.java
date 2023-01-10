package com.github.bihealth.varfish_annotator.annotate_svs;

import com.ginsberg.junit.exit.FailOnSystemExit;
import com.github.bihealth.varfish_annotator.ResourceUtils;
import com.github.bihealth.varfish_annotator.VarfishAnnotatorCli;
import com.github.bihealth.varfish_annotator.utils.GzipUtil;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Test running with some real-world data files for GRCh37. */
public class RunWithRealWorldData37Test {

  @TempDir public File tmpFolder;
  File h2DbFile;
  File fastaFile;
  File faiFile;
  File ensemblSerFile;
  File refseqSerFile;

  @BeforeEach
  void initEach() {
    h2DbFile = new File(tmpFolder + "/small-GRCh38.h2.db");
    fastaFile = new File(tmpFolder + "/chr22_part.fa");
    faiFile = new File(tmpFolder + "/chr22_part.fa.fai");
    ensemblSerFile = new File(tmpFolder + "/hg19_ensembl.ser");
    refseqSerFile = new File(tmpFolder + "/hg19_refseq.ser");

    ResourceUtils.gunzipResourceToFile("/grch37-chr22/chr22_part.fa.gz", fastaFile);
    ResourceUtils.copyResourceToFile("/grch37-chr22/chr22_part.fa.fai", faiFile);
    ResourceUtils.copyResourceToFile("/grch37-chr22/small-grch37-chr22.h2.db", h2DbFile);
    ResourceUtils.copyResourceToFile("/grch37-chr22/hg19_ensembl.ser", ensemblSerFile);
    ResourceUtils.copyResourceToFile("/grch37-chr22/hg19_refseq.ser", refseqSerFile);
  }

  void runTest(
      List<String> inputFileNames,
      String inputPath,
      String expectedDbInfosFileName,
      String expectedGtsFileName,
      String expectedFeatureEffectsFileName,
      boolean gzipOutput,
      boolean selfTestChr1Only,
      boolean selfTestChr22Only)
      throws IOException {
    final String gzSuffix = gzipOutput ? ".gz" : "";
    for (String inputFileName : inputFileNames) {
      final File vcfPath = new File(tmpFolder + "/" + inputFileName);
      final File tbiPath = new File(vcfPath + ".tbi");
      ResourceUtils.copyResourceToFile("/" + inputPath + "/" + vcfPath.getName(), vcfPath);
      ResourceUtils.copyResourceToFile("/" + inputPath + "/" + tbiPath.getName(), tbiPath);
    }

    final File expectedDbInfosPath = new File(tmpFolder + "/" + expectedDbInfosFileName);
    ResourceUtils.copyResourceToFile(
        "/input/real-world-37/" + expectedDbInfosFileName, expectedDbInfosPath);
    final File expectedGtsPath = new File(tmpFolder + "/" + expectedGtsFileName);
    ResourceUtils.copyResourceToFile(
        "/input/real-world-37/" + expectedGtsFileName, expectedGtsPath);
    final File expectedFeatureEffectsPath =
        new File(tmpFolder + "/" + expectedFeatureEffectsFileName);
    ResourceUtils.copyResourceToFile(
        "/input/real-world-37/" + expectedFeatureEffectsFileName, expectedFeatureEffectsPath);

    final File outputDbInfoPath = new File(tmpFolder + "/output.db-info.tsv" + gzSuffix);
    final File outputGtsPath = new File(tmpFolder + "/output.gts.tsv" + gzSuffix);
    final File outputFeatureEffectsPath =
        new File(tmpFolder + "/output.feature-effects.tsv" + gzSuffix);

    final ArrayList<String> args =
        Lists.newArrayList(
            "annotate-svs",
            "--release",
            "GRCh37",
            "--sequential-uuids",
            "--db-path",
            h2DbFile.toString(),
            "--refseq-ser-path",
            refseqSerFile.toString(),
            "--ensembl-ser-path",
            ensemblSerFile.toString(),
            "--output-db-info",
            outputDbInfoPath.toString(),
            "--output-gts",
            outputGtsPath.toString(),
            "--output-feature-effects",
            outputFeatureEffectsPath.toString());
    for (String inputFileName : inputFileNames) {
      args.add("--input-vcf");
      args.add(tmpFolder + "/" + inputFileName);
    }
    if (selfTestChr1Only) {
      args.add("--self-test-chr1-only");
    } else if (selfTestChr22Only) {
      args.add("--self-test-chr22-only");
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
          FileUtils.readFileToString(expectedDbInfosPath, "utf-8").replaceAll("\\r\\n?", "\n"),
          FileUtils.readFileToString(outputDbInfoPath, "utf-8").replaceAll("\\r\\n?", "\n"));
      Assertions.assertEquals(
          FileUtils.readFileToString(expectedGtsPath, "utf-8").replaceAll("\\r\\n?", "\n"),
          FileUtils.readFileToString(outputGtsPath, "utf-8").replaceAll("\\r\\n?", "\n"));
      Assertions.assertEquals(
          FileUtils.readFileToString(expectedFeatureEffectsPath, "utf-8")
              .replaceAll("\\r\\n?", "\n"),
          FileUtils.readFileToString(outputFeatureEffectsPath, "utf-8")
              .replaceAll("\\r\\n?", "\n"));
    }
  }

  @FailOnSystemExit
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDelly2(boolean gzipOutput) throws IOException {
    runTest(
        ImmutableList.of("bwa.delly2.Case_1_index-N1-DNA1-WGS1.vcf.gz"),
        "input/real-world-37",
        "bwa.delly2.Case_1_index-N1-DNA1-WGS1.db-infos.tsv",
        "bwa.delly2.Case_1_index-N1-DNA1-WGS1.gts.tsv",
        "bwa.delly2.Case_1_index-N1-DNA1-WGS1.feature-effects.tsv",
        gzipOutput,
        false,
        true);
  }

  @FailOnSystemExit
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGatkGcnv(boolean gzipOutput) throws IOException {
    runTest(
        ImmutableList.of("bwa.gcnv.NA12878-N1-DNA1-WGS1.vcf.gz"),
        "input/real-world-37",
        "bwa.gcnv.NA12878-N1-DNA1-WGS1.db-infos.tsv",
        "bwa.gcnv.NA12878-N1-DNA1-WGS1.gts.tsv",
        "bwa.gcnv.NA12878-N1-DNA1-WGS1.feature-effects.tsv",
        gzipOutput,
        false,
        true);
  }

  @FailOnSystemExit
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDragenCnv(boolean gzipOutput) throws IOException {
    runTest(
        ImmutableList.of("NA-12878WGS_dragen.cnv.vcf.gz"),
        "input/real-world-37",
        "NA-12878WGS_dragen.cnv.db-infos.tsv",
        "NA-12878WGS_dragen.cnv.gts.tsv",
        "NA-12878WGS_dragen.cnv.feature-effects.tsv",
        gzipOutput,
        false,
        true);
  }

  @FailOnSystemExit
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDragenSv(boolean gzipOutput) throws IOException {
    runTest(
        ImmutableList.of("NA-12878WGS_dragen.sv.vcf.gz"),
        "input/real-world-37",
        "NA-12878WGS_dragen.sv.db-infos.tsv",
        "NA-12878WGS_dragen.sv.gts.tsv",
        "NA-12878WGS_dragen.sv.feature-effects.tsv",
        gzipOutput,
        false,
        true);
  }

  @FailOnSystemExit
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDragenSvAndGcnv(boolean gzipOutput) throws IOException {
    runTest(
        ImmutableList.of("NA-12878WGS_dragen.sv.vcf.gz", "bwa.gcnv.NA12878-N1-DNA1-WGS1.vcf.gz"),
        "input/real-world-37",
        "NA-12878WGS_dragensv_gcnv.db-infos.tsv",
        "NA-12878WGS_dragensv_gcnv.gts.tsv",
        "NA-12878WGS_dragensv_gcnv.feature-effects.tsv",
        gzipOutput,
        false,
        true);
  }

  @FailOnSystemExit
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testMelt(boolean gzipOutput) throws IOException {
    runTest(
        ImmutableList.of("bwa.melt.NA12878.vcf.gz"),
        "input/real-world-37",
        "NA12878_melt.db-infos.tsv",
        "NA12878_melt.gts.tsv",
        "NA12878_melt.feature-effects.tsv",
        gzipOutput,
        false,
        true);
  }
}
