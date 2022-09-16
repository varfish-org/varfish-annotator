package com.github.bihealth.varfish_annotator.annotate_svs;

import com.github.bihealth.varfish_annotator.ResourceUtils;
import java.io.File;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CoverageFromMaelstromReaderTest {
  @TempDir public File tmpFolder;
  File vcfFile;
  File tbiFile;
  CoverageFromMaelstromReader reader;

  @BeforeEach
  void initEach() {
    vcfFile = new File(tmpFolder + "/coverage.vcf.gz");
    ResourceUtils.copyResourceToFile("/cov/coverage.vcf.gz", vcfFile);
    tbiFile = new File(tmpFolder + "/coverage.vcf.gz.tbi");
    ResourceUtils.copyResourceToFile("/cov/coverage.vcf.gz.tbi", tbiFile);
    reader = new CoverageFromMaelstromReader(vcfFile);
  }

  @Test
  void testGetSample() {
    Assertions.assertEquals("sample", reader.getSample());
  }

  @Test
  void readOneWindow() {
    final CoverageFromMaelstromReader.Result result = reader.read("one", 1, 100);
    Assertions.assertEquals(
        "Result{medianCoverage=1.0, medianMappingQuality=40.0}", result.toString());
    Assertions.assertEquals(1.0, result.getMedianCoverage());
    Assertions.assertEquals(40.0, result.getMedianMappingQuality());
  }

  @Test
  void readTwoWindowsLow() {
    final CoverageFromMaelstromReader.Result result = reader.read("one", 1, 200);
    Assertions.assertEquals(
        "Result{medianCoverage=1.0, medianMappingQuality=40.0}", result.toString());
    Assertions.assertEquals(1.0, result.getMedianCoverage());
    Assertions.assertEquals(40.0, result.getMedianMappingQuality());
  }

  @Test
  void readTwoWindowsHigh() {
    final CoverageFromMaelstromReader.Result result = reader.read("one", 701, 900);
    Assertions.assertEquals(
        "Result{medianCoverage=2.0, medianMappingQuality=50.0}", result.toString());
    Assertions.assertEquals(2.0, result.getMedianCoverage());
    Assertions.assertEquals(50.0, result.getMedianMappingQuality());
  }

  @Test
  void readTwoWindowsSpanning() {
    final CoverageFromMaelstromReader.Result result = reader.read("one", 401, 600);
    Assertions.assertEquals(
        "Result{medianCoverage=1.5, medianMappingQuality=45.0}", result.toString());
    Assertions.assertEquals(1.5, result.getMedianCoverage());
    Assertions.assertEquals(45.0, result.getMedianMappingQuality());
  }

  @Test
  void readTwoWindowsFractional1() {
    final CoverageFromMaelstromReader.Result result = reader.read("one", 451, 600);
    Assertions.assertEquals(
        "Result{medianCoverage=1.6666666666666667, medianMappingQuality=46.666666666666664}",
        result.toString());
    Assertions.assertEquals(2.5 / 1.5, result.getMedianCoverage());
    Assertions.assertEquals(70.0 / 1.5, result.getMedianMappingQuality());
  }

  @Test
  void readTwoWindowsFractional2() {
    final CoverageFromMaelstromReader.Result result = reader.read("one", 401, 550);
    Assertions.assertEquals(
        "Result{medianCoverage=1.3333333333333333, medianMappingQuality=43.333333333333336}",
        result.toString());
    Assertions.assertEquals(2.0 / 1.5, result.getMedianCoverage());
    Assertions.assertEquals(65.0 / 1.5, result.getMedianMappingQuality());
  }
}
