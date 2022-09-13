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

public class CallerSupportDragenSvTest {

  @TempDir public File tmpFolder;
  File vcfFile;
  File otherVcfFile;
  CallerSupportDragenSv callerSupport;

  @BeforeEach
  void initEach() {
    vcfFile = new File(tmpFolder + "/vcf-header.vcf");
    ResourceUtils.copyResourceToFile("/callers-sv/dragen-sv-head.vcf", vcfFile);
    otherVcfFile = new File(tmpFolder + "/incompatible.vcf");
    ResourceUtils.copyResourceToFile("/callers-sv/delly2-head.vcf", otherVcfFile);
    callerSupport = new CallerSupportDragenSv();
  }

  @Test
  void testGetSvCaller() {
    Assertions.assertEquals(SvCaller.DRAGEN_SV, callerSupport.getSvCaller());
  }

  @Test
  void testIsCompatiblePositive() {
    final VCFFileReader vcfReader = new VCFFileReader(vcfFile, false);
    final VCFHeader vcfHeader = vcfReader.getHeader();

    Assertions.assertTrue(callerSupport.isCompatible(vcfHeader));
    Assertions.assertEquals(callerSupport.getVersion(vcfReader), "07.021.624.3.10.4");
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
        "SampleGenotype{sampleName='SAMPLE', genotype='1/1', filters=[], genotypeQuality=53, pairedEndCoverage=2, pairedEndVariantSupport=2, splitReadCoverage=20, splitReadVariantSupport=20, averageMappingQuality=null, copyNumber=null, averageNormalizedCoverage=null, pointCount=null}";
    Assertions.assertEquals(expected, sampleGenotype.toString());
  }
}
