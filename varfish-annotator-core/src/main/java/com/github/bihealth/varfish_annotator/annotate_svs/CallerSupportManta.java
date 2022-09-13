package com.github.bihealth.varfish_annotator.annotate_svs;

import com.github.bihealth.varfish_annotator.utils.HtsjdkUtils;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;

/** Import SV caller support for Manta. */
public class CallerSupportManta extends CallerSupport {

  public SvCaller getSvCaller() {
    return SvCaller.MANTA;
  }

  @Override
  public String getVersion(VCFFileReader vcfReader) {
    for (VCFHeaderLine headerLine : HtsjdkUtils.getSourceHeaderLines(vcfReader.getHeader())) {
      final String value = headerLine.getValue();
      if (value.startsWith("GenerateSVCandidates") && value.contains(" ")) {
        return value.split(" ", 2)[1];
      }
    }
    return null;
  }

  public boolean isCompatible(VCFHeader vcfHeader) {
    for (VCFHeaderLine headerLine : HtsjdkUtils.getSourceHeaderLines(vcfHeader)) {
      final String value = headerLine.getValue();
      if (value.startsWith("GenerateSVCandidates")) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected void buildSampleGenotypeImpl(
      SampleGenotypeBuilder builder, VariantContext ctx, int alleleNo, String sample) {
    final Genotype genotype = ctx.getGenotype(sample);
    final String[] prs = genotype.getExtendedAttribute("PR", "0,0").toString().split(",");
    final int pr0 = Integer.parseInt(prs[0]);
    final int pr1 = Integer.parseInt(prs[1]);
    final String[] srs = genotype.getExtendedAttribute("SR", "0,0").toString().split(",");
    final int sr0 = Integer.parseInt(srs[0]);
    final int sr1 = Integer.parseInt(srs[1]);
    builder.setPairedEndCoverage(pr0 + pr1);
    builder.setPairedEndVariantSupport(pr1);
    builder.setSplitReadCoverage(sr0 + sr1);
    builder.setSplitReadVariantSupport(sr1);
  }
}
