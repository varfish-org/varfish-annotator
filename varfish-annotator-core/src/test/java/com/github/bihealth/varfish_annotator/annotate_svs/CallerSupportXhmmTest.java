package com.github.bihealth.varfish_annotator.annotate_svs;

import com.github.bihealth.varfish_annotator.ResourceUtils;
import com.google.common.collect.ImmutableMap;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import java.io.File;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CallerSupportXhmmTest {

  @TempDir public File tmpFolder;
  File vcfFile;
  File otherVcfFile;
  File coverageVcfFile;
  File coverageTbiFile;
  CallerSupportXhmm callerSupport;

  @BeforeEach
  void initEach() {
    vcfFile = new File(tmpFolder + "/vcf-header.vcf");
    ResourceUtils.copyResourceToFile("/callers-sv/xhmm-head.vcf", vcfFile);
    otherVcfFile = new File(tmpFolder + "/incompatible.vcf");
    ResourceUtils.copyResourceToFile("/callers-sv/manta-head.vcf", otherVcfFile);
    coverageVcfFile = new File(tmpFolder + "/example.SAMPLE.cov.vcf.gz");
    ResourceUtils.copyResourceToFile("/callers-sv/example.SAMPLE.cov.vcf.gz", coverageVcfFile);
    coverageTbiFile = new File(tmpFolder + "/example.SAMPLE.cov.vcf.gz.tbi");
    ResourceUtils.copyResourceToFile("/callers-sv/example.SAMPLE.cov.vcf.gz.tbi", coverageTbiFile);
    callerSupport =
        new CallerSupportXhmm(
            ImmutableMap.of("SAMPLE", new CoverageFromMaelstromReader(coverageVcfFile)));
  }

  @Test
  void testGetSvCaller() {
    Assertions.assertEquals(SvCaller.XHMM, callerSupport.getSvCaller());
  }

  @Test
  void testIsCompatiblePositive() {
    final VCFFileReader vcfReader = new VCFFileReader(vcfFile, false);
    final VCFHeader vcfHeader = vcfReader.getHeader();

    Assertions.assertTrue(callerSupport.isCompatible(vcfHeader));
    Assertions.assertEquals(callerSupport.getVersion(vcfReader), "2016_01_04.cc14e52");
  }

  @Test
  void testIsCompatibleNegative() {
    final VCFFileReader vcfReader = new VCFFileReader(vcfFile, false);
    final VCFHeader vcfHeader = vcfReader.getHeader();

    Assertions.assertTrue(callerSupport.isCompatible(vcfHeader));
  }

  @Test
  void testBuildSampleGenotype() {
    final VCFFileReader vcfReader = new VCFFileReader(vcfFile, false);
    final VariantContext vc = vcfReader.iterator().next();
    final SampleGenotype sampleGenotype = callerSupport.buildSampleGenotype(vc, 1, "SAMPLE");
    final String expected =
        "SampleGenotype{sampleName='SAMPLE', genotype='0', filters=[], genotypeQuality=null, pairedEndCoverage=null, pairedEndVariantSupport=null, splitReadCoverage=null, splitReadVariantSupport=null, averageMappingQuality=40, copyNumber=null, averageNormalizedCoverage=3.47, pointCount=null}";
    Assertions.assertEquals(expected, sampleGenotype.toString());
  }
}
