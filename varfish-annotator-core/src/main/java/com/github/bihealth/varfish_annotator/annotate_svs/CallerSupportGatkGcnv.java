package com.github.bihealth.varfish_annotator.annotate_svs;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.*;
import java.util.Map;

/** Import SV caller support for Delly2. */
public class CallerSupportGatkGcnv extends CallerSupport {

  public CallerSupportGatkGcnv(Map<String, CoverageFromMaelstromReader> coverageReaders) {
    super(coverageReaders);
  }

  public SvCaller getSvCaller() {
    return SvCaller.GATK_GCNV;
  }

  @Override
  public String getVersion(VCFFileReader vcfReader) {
    return "UNKNOWN";
  }

  public boolean isCompatible(VCFHeader vcfHeader) {
    final VCFFormatHeaderLine qseFormat = vcfHeader.getFormatHeaderLine("QSE");
    boolean seenQseFormat = (qseFormat != null);
    final VCFFormatHeaderLine qssFormat = vcfHeader.getFormatHeaderLine("QSS");
    boolean seenQssFormat = (qssFormat != null);

    return seenQseFormat && seenQssFormat;
  }

  @Override
  protected void buildSampleGenotypeImpl(
      SampleGenotypeBuilder builder, VariantContext ctx, int alleleNo, String sample) {
    final Genotype genotype = ctx.getGenotype(sample);
    builder.setCopyNumber(Integer.parseInt(genotype.getExtendedAttribute("CN", "0").toString()));
    builder.setPointCount(Integer.parseInt(genotype.getExtendedAttribute("NP", "0").toString()));
  }
}
