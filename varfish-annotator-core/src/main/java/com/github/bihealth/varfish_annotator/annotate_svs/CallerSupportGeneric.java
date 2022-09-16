package com.github.bihealth.varfish_annotator.annotate_svs;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import java.util.Map;

/** Import SV caller support for generic SV callers. */
public class CallerSupportGeneric extends CallerSupport {

  public CallerSupportGeneric(Map<String, CoverageFromMaelstromReader> coverageReaders) {
    super(coverageReaders);
  }

  public SvCaller getSvCaller() {
    return SvCaller.GENERIC;
  }

  @Override
  public String getVersion(VCFFileReader vcfReader) {
    return "UNKNOWN";
  }

  public boolean isCompatible(VCFHeader vcfHeader) {
    final VCFInfoHeaderLine svTypeInfo = vcfHeader.getInfoHeaderLine("SVTYPE");
    boolean seenSvTypeInfo = (svTypeInfo != null);
    final VCFInfoHeaderLine svEndInfo = vcfHeader.getInfoHeaderLine("END");
    boolean seenSvTypeEnd = (svEndInfo != null);
    final VCFInfoHeaderLine svLenInfo = vcfHeader.getInfoHeaderLine("SVLEN");
    boolean seenSvLenEnd = (svLenInfo != null);

    return seenSvTypeInfo && seenSvTypeEnd && seenSvLenEnd;
  }

  @Override
  protected void buildSampleGenotypeImpl(
      SampleGenotypeBuilder builder, VariantContext ctx, int alleleNo, String sample) {
    // no generic implementation possible
  }
}
