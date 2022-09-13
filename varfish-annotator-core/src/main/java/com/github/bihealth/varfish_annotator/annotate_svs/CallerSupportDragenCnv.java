package com.github.bihealth.varfish_annotator.annotate_svs;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFFilterHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Import SV caller support for Dragen CNV. */
public class CallerSupportDragenCnv extends CallerSupport {

  public SvCaller getSvCaller() {
    return SvCaller.DRAGEN_CNV;
  }

  private static final Pattern p = Pattern.compile("Version=\"(.*?)\"");

  @Override
  public String getVersion(VCFFileReader vcfReader) {
    for (VCFHeaderLine headerLine : vcfReader.getHeader().getOtherHeaderLines()) {
      if (headerLine.getKey().equals("DRAGENVersion")) {
        final Matcher m = p.matcher(headerLine.getValue());
        if (m.find()) {
          return m.group(1);
        }
      }
    }
    return null;
  }

  public boolean isCompatible(VCFHeader vcfHeader) {
    boolean seenDragenVersionHeaderLine = false;
    boolean seenDragenCommandLineHeaderLine = false;
    for (VCFHeaderLine headerLine : vcfHeader.getOtherHeaderLines()) {
      if (headerLine.getKey().equals("DRAGENVersion")) {
        seenDragenVersionHeaderLine = true;
      } else if (headerLine.getKey().equals("DRAGENCommandLine")) {
        seenDragenCommandLineHeaderLine = true;
      }
    }

    final VCFFilterHeaderLine cnvBinSupportRatio =
        vcfHeader.getFilterHeaderLine("cnvBinSupportRatio");
    boolean seenCnvBinSupportRatioFilter = (cnvBinSupportRatio != null);

    return seenDragenVersionHeaderLine
        && seenDragenCommandLineHeaderLine
        && seenCnvBinSupportRatioFilter;
  }

  @Override
  protected void buildSampleGenotypeImpl(
      SampleGenotypeBuilder builder, VariantContext ctx, int alleleNo, String sample) {
    final Genotype genotype = ctx.getGenotype(sample);
    builder.setAverageNormalizedCoverage(
        Double.parseDouble(genotype.getExtendedAttribute("SM", "0.0").toString()));
    builder.setPointCount(Integer.parseInt(genotype.getExtendedAttribute("BC", "0").toString()));
    final String pe = genotype.getExtendedAttribute("PE", "0,0").toString();
    final String[] pes = pe.split(",");
    final int pe0 = Integer.parseInt(pes[0]);
    final int pe1 = Integer.parseInt(pes[1]);
    builder.setPairedEndVariantSupport(pe0 + pe1);
  }
}
