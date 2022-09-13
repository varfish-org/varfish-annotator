package com.github.bihealth.varfish_annotator.annotate_svs;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SampleGenotypeTest {
  @Test
  public void testConstruction() {
    final SampleGenotype obj =
        new SampleGenotype("SAMPLE", "0/1", ImmutableList.of("PASS"), 1, 2, 3, 4, 5, 6, 7, 8.0, 9);

    Assertions.assertEquals(
        "SampleGenotype{sampleName='SAMPLE', genotype='0/1', filters=[PASS], genotypeQuality=1, pairedEndCoverage=2, pairedEndVariantSupport=3, splitReadCoverage=4, splitReadVariantSupport=5, averageMappingQuality=6, copyNumber=7, averageNormalizedCoverage=8.0, pointCount=9}",
        obj.toString());
    Assertions.assertEquals(obj, obj);
    Assertions.assertEquals(obj.hashCode(), -716760314);

    Assertions.assertEquals("SAMPLE", obj.getSampleName());
    Assertions.assertEquals("0/1", obj.getGenotype());
    Assertions.assertEquals(ImmutableList.of("PASS"), obj.getFilters());
    Assertions.assertEquals(1, obj.getGenotypeQuality());
    Assertions.assertEquals(2, obj.getPairedEndCoverage());
    Assertions.assertEquals(3, obj.getPairedEndVariantSupport());
    Assertions.assertEquals(4, obj.getSplitReadCoverage());
    Assertions.assertEquals(5, obj.getSplitReadVariantSupport());
    Assertions.assertEquals(6, obj.getAverageMappingQuality());
    Assertions.assertEquals(7, obj.getCopyNumber());
    Assertions.assertEquals(8.0, obj.getAverageNormalizedCoverage());
    Assertions.assertEquals(9, obj.getPointCount());
  }

  @Test
  public void testBuildFragmentsPart() {
    final SampleGenotype obj =
        new SampleGenotype(
            "SAMPLE",
            "0/1",
            ImmutableList.of("PASS"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    Assertions.assertEquals(
        "\"\"\"SAMPLE\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ft\"\"\":{\"\"\"PASS\"\"\"}}",
        obj.buildStringFragment());
  }

  @Test
  public void testBuildFragmentsFull() {
    final SampleGenotype obj =
        new SampleGenotype("SAMPLE", "0/1", ImmutableList.of("PASS"), 1, 2, 3, 4, 5, 6, 7, 8.0, 9);
    Assertions.assertEquals(
        "\"\"\"SAMPLE\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"ft\"\"\":{\"\"\"PASS\"\"\"},\"\"\"gq\"\"\":1,\"\"\"pec\"\"\":2,\"\"\"pev\"\"\":3,\"\"\"src\"\"\":4,\"\"\"srv\"\"\":5,\"\"\"amq\"\"\":6,\"\"\"cn\"\"\":7,\"\"\"anc\"\"\":8.0,\"\"\"pc\"\"\":9}",
        obj.buildStringFragment());
  }
}
