package com.github.bihealth.varfish_annotator.annotate_svs;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFFilterHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import java.util.Map;

/** Import SV caller support for Delly2. */
public class CallerSupportDelly2 extends CallerSupport {

  public CallerSupportDelly2(Map<String, CoverageFromMaelstromReader> coverageReaders) {
    super(coverageReaders);
  }

  public SvCaller getSvCaller() {
    return SvCaller.DELLY2_SV;
  }

  @Override
  public String getVersion(VCFFileReader vcfReader) {
    final VariantContext vc = vcfReader.iterator().next();
    if (!vc.hasAttribute("SVMETHOD")) {
      return null;
    } else {
      final String svMethod = vc.getAttributeAsString("SVMETHOD", "");
      if (!svMethod.startsWith("EMBL.DELLYv")) {
        return null;
      } else {
        return svMethod.substring("EMBL.DELLYv".length());
      }
    }
  }

  public boolean isCompatible(VCFHeader vcfHeader) {
    final VCFFilterHeaderLine lowQualFilter = vcfHeader.getFilterHeaderLine("LowQual");
    boolean seenLowQualFilter = (lowQualFilter != null);
    final VCFInfoHeaderLine ctInfo = vcfHeader.getInfoHeaderLine("CT");
    boolean seenCtInfo = (ctInfo != null);
    final VCFInfoHeaderLine impreciseInfo = vcfHeader.getInfoHeaderLine("IMPRECISE");
    boolean seenImpreciseInfo = (impreciseInfo != null);

    return seenLowQualFilter && seenCtInfo && seenImpreciseInfo;
  }

  @Override
  protected void buildSampleGenotypeImpl(
      SampleGenotypeBuilder builder, VariantContext ctx, int alleleNo, String sample) {
    final Genotype genotype = ctx.getGenotype(sample);
    final int dr = Integer.parseInt(genotype.getExtendedAttribute("DR", "0").toString());
    final int dv = Integer.parseInt(genotype.getExtendedAttribute("DV", "0").toString());
    builder.setPairedEndCoverage(dr + dv);
    builder.setPairedEndVariantSupport(dv);
    final int rr = Integer.parseInt(genotype.getExtendedAttribute("RR", "0").toString());
    final int rv = Integer.parseInt(genotype.getExtendedAttribute("RV", "0").toString());
    builder.setSplitReadCoverage(rr + rv);
    builder.setSplitReadVariantSupport(rv);
    if (genotype.hasExtendedAttribute("RDCN")) {
      final int cn = Integer.parseInt(genotype.getExtendedAttribute("RDCN", "0").toString());
      builder.setCopyNumber(cn);
    }
  }
}
