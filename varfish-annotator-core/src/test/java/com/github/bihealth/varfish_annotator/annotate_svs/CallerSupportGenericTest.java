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

public class CallerSupportGenericTest {

  @TempDir public File tmpFolder;
  File vcfFile;
  File coverageVcfFile;
  File coverageTbiFile;
  CallerSupportGeneric callerSupport;

  @BeforeEach
  void initEach() {
    vcfFile = new File(tmpFolder + "/generic-head.vcf");
    ResourceUtils.copyResourceToFile("/callers-sv/generic-head.vcf", vcfFile);
    coverageVcfFile = new File(tmpFolder + "/example.SAMPLE.cov.vcf.gz");
    ResourceUtils.copyResourceToFile("/callers-sv/example.SAMPLE.cov.vcf.gz", coverageVcfFile);
    coverageTbiFile = new File(tmpFolder + "/example.SAMPLE.cov.vcf.gz.tbi");
    ResourceUtils.copyResourceToFile("/callers-sv/example.SAMPLE.cov.vcf.gz.tbi", coverageTbiFile);
    callerSupport =
        new CallerSupportGeneric(
            ImmutableMap.of("SAMPLE", new CoverageFromMaelstromReader(coverageVcfFile)));
  }

  @Test
  void testGetSvCaller() {
    Assertions.assertEquals(SvCaller.GENERIC, callerSupport.getSvCaller());
  }

  @Test
  void testIsCompatiblePositive() {
    final VCFFileReader vcfReader = new VCFFileReader(vcfFile, false);
    final VCFHeader vcfHeader = vcfReader.getHeader();

    Assertions.assertTrue(callerSupport.isCompatible(vcfHeader));
    Assertions.assertEquals(callerSupport.getVersion(vcfReader), "UNKNOWN");
  }

  @Test
  void testBuildSampleGenotype() {
    final VCFFileReader vcfReader = new VCFFileReader(vcfFile, false);
    final VariantContext vc = vcfReader.iterator().next();
    final SampleGenotype sampleGenotype = callerSupport.buildSampleGenotype(vc, 1, "SAMPLE");
    final String expected =
        "SampleGenotype{sampleName='SAMPLE', genotype='0/1', filters=[\"\"\"LowQual\"\"\"], genotypeQuality=59, pairedEndCoverage=null, pairedEndVariantSupport=null, splitReadCoverage=null, splitReadVariantSupport=null, averageMappingQuality=null, copyNumber=null, averageNormalizedCoverage=null, pointCount=null}";
    Assertions.assertEquals(expected, sampleGenotype.toString());
  }
}
