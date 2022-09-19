package com.github.bihealth.varfish_annotator.annotate_svs;

import static com.github.bihealth.varfish_annotator.utils.StringUtils.tripleQuote;

import com.google.common.base.Joiner;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import java.util.*;
import java.util.stream.Collectors;

public abstract class CallerSupport {
  /** Mapping from sample name to {@code CoverageFromMaelstromReader}. */
  protected final Map<String, CoverageFromMaelstromReader> coverageReaders;

  public CallerSupport(Map<String, CoverageFromMaelstromReader> coverageReaders) {
    this.coverageReaders = coverageReaders;
  }

  /** @return String to put into output as "SVMETHOD". */
  public String getSvMethod(VCFFileReader vcfReader) {
    return getSvCaller() + "v" + getVersion(vcfReader);
  }

  /** @return Name of the used SV caller. */
  public abstract SvCaller getSvCaller();

  /**
   * Extract version from the given VCF file.
   *
   * <p>This may read a record from the VCF file, so make sure to reset the reader if necessary. It
   * assumes that all records in the VCF file are from the same caller.
   *
   * @param vcfReader VCF header to extract the version from
   * @return Version string of the used SV caller.
   */
  public abstract String getVersion(VCFFileReader vcfReader);

  /**
   * @param vcfHeader VCF header to consider
   * @return Whether the SV caller is compatible with the given VCF header.
   */
  public abstract boolean isCompatible(VCFHeader vcfHeader);

  public SampleGenotype buildSampleGenotype(VariantContext ctx, int alleleNo, String sample) {
    SampleGenotypeBuilder builder = new SampleGenotypeBuilder();
    builder.setSampleName(sample);
    builder.setGenotype(buildGenotype(ctx, alleleNo, sample));
    builder.setFilters(buildFilters(ctx, sample));
    builder.setGenotypeQuality(getGenotypeQuality(ctx, alleleNo, sample));
    annotateCovMq(builder, ctx, sample);
    buildSampleGenotypeImpl(builder, ctx, alleleNo, sample);
    return builder.build();
  }

  /** Annotate coverage and mapping quality for sample. */
  private void annotateCovMq(SampleGenotypeBuilder builder, VariantContext ctx, String sample) {
    final CoverageFromMaelstromReader reader = coverageReaders.get(sample);
    if (reader == null) {
      return;
    }
    if (ctx.getAttributeAsString("SVTYPE", "").startsWith("CNV")
        || ctx.getAttributeAsString("SVTYPE", "").startsWith("DEL")
        || ctx.getAttributeAsString("SVTYPE", "").startsWith("DUP")) {
      final CoverageFromMaelstromReader.Result result =
          reader.read(
              ctx.getContig(), ctx.getStart(), ctx.getAttributeAsInt("END", ctx.getStart()));
      builder.setAverageNormalizedCoverage(result.getMedianCoverage());
      builder.setAverageMappingQuality(
          Math.toIntExact(Math.round(result.getMedianMappingQuality())));
    }
  }

  protected abstract void buildSampleGenotypeImpl(
      SampleGenotypeBuilder builder, VariantContext ctx, int alleleNo, String sample);

  private String buildGenotype(VariantContext ctx, int alleleNo, String sample) {
    final Genotype genotype = ctx.getGenotype(sample);
    final List<String> gtList = new ArrayList<>();
    for (Allele allele : genotype.getAlleles()) {
      if (allele.isNoCall()) {
        gtList.add(".");
      } else if (ctx.getAlleleIndex(allele) == alleleNo) {
        gtList.add("1");
      } else {
        gtList.add("0");
      }
    }
    if (genotype.isPhased()) {
      return Joiner.on("|").join(gtList);
    } else {
      gtList.sort(Comparator.naturalOrder());
      return Joiner.on("/").join(gtList);
    }
  }

  private List<String> buildFilters(VariantContext ctx, String sample) {
    final Genotype genotype = ctx.getGenotype(sample);
    if (genotype.getFilters() != null && !genotype.getFilters().equals("")) {
      final List<String> fts =
          Arrays.stream(genotype.getFilters().split(";"))
              .map(s -> tripleQuote(s))
              .collect(Collectors.toList());
      return fts;
    } else {
      return new ArrayList<>();
    }
  }

  private Integer getGenotypeQuality(VariantContext ctx, int alleleNo, String sample) {
    final Genotype genotype = ctx.getGenotype(sample);
    if (genotype.hasGQ()) {
      return genotype.getGQ();
    } else {
      return null;
    }
  }
}
