package com.github.bihealth.varfish_annotator.annotate_svs;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SampleGenotypeBuilderTest {
  @Test
  public void testAll() {
    final SampleGenotypeBuilder builder = new SampleGenotypeBuilder();
    builder.setSampleName("SAMPLE");
    builder.setGenotype("0/1");
    builder.setFilters(ImmutableList.of("PASS"));
    builder.setGenotypeQuality(1);
    builder.setPairedEndCoverage(5);
    builder.setPairedEndVariantSupport(2);
    builder.setSplitReadCoverage(7);
    builder.setSplitReadVariantSupport(3);
    builder.setAverageMappingQuality(10);
    builder.setCopyNumber(3);
    builder.setAverageNormalizedCoverage(3.3);
    builder.setPointCount(100);

    Assertions.assertEquals(
        "SampleGenotypeBuilder{sampleName='SAMPLE', genotype='0/1', filters=[PASS], genotypeQuality=1, pairedEndCoverage=5, pairedEndVariantSupport=2, splitReadCoverage=7, splitReadVariantSupport=3, averageMappingQuality=10, copyNumber=3, averageNormalizedCoverage=3.3, pointCount=100}",
        builder.toString());

    Assertions.assertEquals("SAMPLE", builder.getSampleName());
    Assertions.assertEquals("0/1", builder.getGenotype());
    Assertions.assertEquals(ImmutableList.of("PASS"), builder.getFilters());
    Assertions.assertEquals(1, builder.getGenotypeQuality());
    Assertions.assertEquals(5, builder.getPairedEndCoverage());
    Assertions.assertEquals(2, builder.getPairedEndVariantSupport());
    Assertions.assertEquals(7, builder.getSplitReadCoverage());
    Assertions.assertEquals(3, builder.getSplitReadVariantSupport());
    Assertions.assertEquals(10, builder.getAverageMappingQuality());
    Assertions.assertEquals(3, builder.getCopyNumber());
    Assertions.assertEquals(3.3, builder.getAverageNormalizedCoverage());
    Assertions.assertEquals(100, builder.getPointCount());
  }
}
