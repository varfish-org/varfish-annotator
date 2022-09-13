package com.github.bihealth.varfish_annotator.annotate_svs;

import com.github.bihealth.varfish_annotator.ResourceUtils;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import java.io.File;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CallerSupportDellySvTest {

  @TempDir public File tmpFolder;
  File vcfFile;
  File otherVcfFile;
  CallerSupportDelly2 callerSupport;

  @BeforeEach
  void initEach() {
    vcfFile = new File(tmpFolder + "/vcf-header.vcf");
    ResourceUtils.copyResourceToFile("/callers-sv/delly2-head.vcf", vcfFile);
    otherVcfFile = new File(tmpFolder + "/incompatible.vcf");
    ResourceUtils.copyResourceToFile("/callers-sv/manta-head.vcf", otherVcfFile);
    callerSupport = new CallerSupportDelly2();
  }

  @Test
  void testGetSvCaller() {
    Assertions.assertEquals(SvCaller.DELLY2_SV, callerSupport.getSvCaller());
  }

  @Test
  void testIsCompatiblePositive() {
    final VCFFileReader vcfReader = new VCFFileReader(vcfFile, false);
    final VCFHeader vcfHeader = vcfReader.getHeader();

    Assertions.assertTrue(callerSupport.isCompatible(vcfHeader));
    Assertions.assertEquals(callerSupport.getVersion(vcfReader), "1.1.3");
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
        "SampleGenotype{sampleName='SAMPLE', genotype='0/1', filters=[], genotypeQuality=59, pairedEndCoverage=0, pairedEndVariantSupport=0, splitReadCoverage=11, splitReadVariantSupport=4, averageMappingQuality=null, copyNumber=2, averageNormalizedCoverage=null, pointCount=null}";
    Assertions.assertEquals(expected, sampleGenotype.toString());
  }
}
