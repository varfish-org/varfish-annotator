package com.github.bihealth.varfish_annotator.annotate_svs;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.*;
import java.util.Map;

/** Import SV caller support for XHMM. */
public class CallerSupportXhmm extends CallerSupport {

  public CallerSupportXhmm(Map<String, CoverageFromMaelstromReader> coverageReaders) {
    super(coverageReaders);
  }

  public SvCaller getSvCaller() {
    return SvCaller.XHMM;
  }

  @Override
  public String getVersion(VCFFileReader vcfReader) {
    return "2016_01_04.cc14e52"; // latest release
  }

  public boolean isCompatible(VCFHeader vcfHeader) {
    final VCFFormatHeaderLine ndqFormat = vcfHeader.getFormatHeaderLine("NDQ");
    boolean seenNdqFormat = (ndqFormat != null);
    final VCFFormatHeaderLine dscvrFormat = vcfHeader.getFormatHeaderLine("DSCVR");
    boolean seenDscvrFormat = (dscvrFormat != null);

    return seenNdqFormat && seenDscvrFormat;
  }

  @Override
  protected void buildSampleGenotypeImpl(
      SampleGenotypeBuilder builder, VariantContext ctx, int alleleNo, String sample) {
    final Genotype genotype = ctx.getGenotype(sample);
    builder.setAverageNormalizedCoverage(
        Double.parseDouble(genotype.getExtendedAttribute("RD", "0").toString()));
  }
}
