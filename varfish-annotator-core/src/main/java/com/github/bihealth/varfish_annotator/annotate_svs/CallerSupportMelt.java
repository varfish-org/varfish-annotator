package com.github.bihealth.varfish_annotator.annotate_svs;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.*;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Import SV caller support for MELT. */
public class CallerSupportMelt extends CallerSupport {

  public CallerSupportMelt(Map<String, CoverageFromMaelstromReader> coverageReaders) {
    super(coverageReaders);
  }

  public SvCaller getSvCaller() {
    return SvCaller.MELT;
  }

  private static final Pattern p = Pattern.compile("MELTv(.*)");

  @Override
  public String getVersion(VCFFileReader vcfReader) {
    for (VCFHeaderLine headerLine : vcfReader.getHeader().getOtherHeaderLines()) {
      if (headerLine.getKey().equals("source")) {
        final Matcher m = p.matcher(headerLine.getValue());
        if (m.find()) {
          return m.group(1);
        }
      }
    }
    return null;
  }

  public boolean isCompatible(VCFHeader vcfHeader) {
    boolean seenSourceMeltLine = false;
    for (VCFHeaderLine headerLine : vcfHeader.getOtherHeaderLines()) {
      if (headerLine.getKey().equals("source") && headerLine.getValue().startsWith("MELTv")) {
        seenSourceMeltLine = true;
      }
    }

    final VCFFilterHeaderLine hdpFilterLine = vcfHeader.getFilterHeaderLine("hDP");
    boolean seenHdpFilterLine = (hdpFilterLine != null);

    return seenSourceMeltLine && seenHdpFilterLine;
  }

  @Override
  protected void buildSampleGenotypeImpl(
      SampleGenotypeBuilder builder, VariantContext ctx, int alleleNo, String sample) {
    final Genotype genotype = ctx.getGenotype(sample);
    final int dp = genotype.getDP();
    final int ad =
        (genotype.getAD() != null && genotype.getAD().length > 0) ? genotype.getAD()[0] : 0;
    builder.setPairedEndCoverage(dp);
    builder.setPairedEndVariantSupport(ad);
    builder.setSplitReadCoverage(0);
    builder.setSplitReadVariantSupport(0);
  }
}
