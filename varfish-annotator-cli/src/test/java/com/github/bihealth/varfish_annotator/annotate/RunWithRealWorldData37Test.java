package com.github.bihealth.varfish_annotator.annotate;

import com.ginsberg.junit.exit.FailOnSystemExit;
import com.github.bihealth.varfish_annotator.ResourceUtils;
import com.github.bihealth.varfish_annotator.VarfishAnnotatorCli;
import com.github.bihealth.varfish_annotator.utils.GzipUtil;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
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
      String inputFileName,
      String inputPath,
      String expectedDbInfosFileName,
      String expectedGtsFileName,
      boolean gzipOutput,
      boolean selfTestChr1Only,
      boolean selfTestChr22Only)
      throws IOException {
    final String gzSuffix = gzipOutput ? ".gz" : "";
    final File vcfPath = new File(tmpFolder + "/" + inputFileName);
    final File tbiPath = new File(vcfPath + ".tbi");
    ResourceUtils.copyResourceToFile("/" + inputPath + "/" + vcfPath.getName(), vcfPath);
    ResourceUtils.copyResourceToFile("/" + inputPath + "/" + tbiPath.getName(), tbiPath);

    final File expectedDbInfosPath = new File(tmpFolder + "/" + expectedDbInfosFileName);
    ResourceUtils.copyResourceToFile(
        "/input/real-world-37/" + expectedDbInfosFileName, expectedDbInfosPath);
    final File expectedGtsPath = new File(tmpFolder + "/" + expectedGtsFileName);
    ResourceUtils.copyResourceToFile(
        "/input/real-world-37/" + expectedGtsFileName, expectedGtsPath);

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
          FileUtils.readFileToString(expectedDbInfosPath, "utf-8"),
          FileUtils.readFileToString(outputDbInfoPath, "utf-8"));
      Assertions.assertEquals(
          FileUtils.readFileToString(expectedGtsPath, "utf-8"),
          FileUtils.readFileToString(outputGtsPath, "utf-8"));
    }
  }

  @FailOnSystemExit
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGatkHc(boolean gzipOutput) throws IOException {
    runTest(
        "bwa.gatk_hc.Case_1_index-N1-DNA1-WGS1.vcf.gz",
        "input/real-world-37",
        "bwa.gatk_hc.Case_1_index-N1-DNA1-WGS1.db-infos.tsv",
        "bwa.gatk_hc.Case_1_index-N1-DNA1-WGS1.gts.tsv",
        gzipOutput,
        false,
        true);
  }
}
