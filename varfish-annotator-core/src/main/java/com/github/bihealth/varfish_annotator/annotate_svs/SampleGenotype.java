package com.github.bihealth.varfish_annotator.annotate_svs;

import static com.github.bihealth.varfish_annotator.utils.StringUtils.tripleQuote;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class SampleGenotype {

  private final String sampleName;
  private final String genotype;
  private final ImmutableList<String> filters;
  private final Integer genotypeQuality;
  private final Integer pairedEndCoverage;
  private final Integer pairedEndVariantSupport;
  private final Integer splitReadCoverage;
  private final Integer splitReadVariantSupport;
  private final Integer averageMappingQuality;
  private final Integer copyNumber;
  private final Double averageNormalizedCoverage;
  private final Integer pointCount;

  public SampleGenotype(
      String sampleName,
      String genotype,
      List<String> filters,
      Integer genotypeQuality,
      Integer pairedEndCoverage,
      Integer pairedEndVariantSupport,
      Integer splitReadCoverage,
      Integer splitReadVariantSupport,
      Integer averageMappingQuality,
      Integer copyNumber,
      Double averageNormalizedCoverage,
      Integer pointCount) {
    this.sampleName = sampleName;
    this.genotype = genotype;
    this.filters = ImmutableList.copyOf(filters);
    this.genotypeQuality = genotypeQuality;
    this.pairedEndCoverage = pairedEndCoverage;
    this.pairedEndVariantSupport = pairedEndVariantSupport;
    this.splitReadCoverage = splitReadCoverage;
    this.splitReadVariantSupport = splitReadVariantSupport;
    this.averageMappingQuality = averageMappingQuality;
    this.copyNumber = copyNumber;
    this.averageNormalizedCoverage = averageNormalizedCoverage;
    this.pointCount = pointCount;
  }

  public String buildStringFragment() {
    final ArrayList<String> attrs = new ArrayList<>();
    attrs.add(Joiner.on("").join(tripleQuote("gt"), ":", tripleQuote(genotype)));
    if (filters != null && !filters.isEmpty()) {
      final String fts =
          filters.stream().map(s -> tripleQuote(s)).collect(Collectors.joining(", "));
      attrs.add(Joiner.on("").join(tripleQuote("ft"), ":{", fts, "}"));
    }
    if (genotypeQuality != null) {
      attrs.add(Joiner.on("").join(tripleQuote("gq"), ":", genotypeQuality));
    }
    if (pairedEndCoverage != null) {
      attrs.add(Joiner.on("").join(tripleQuote("pec"), ":", pairedEndCoverage));
    }
    if (pairedEndVariantSupport != null) {
      attrs.add(Joiner.on("").join(tripleQuote("pev"), ":", pairedEndVariantSupport));
    }
    if (splitReadCoverage != null) {
      attrs.add(Joiner.on("").join(tripleQuote("src"), ":", splitReadCoverage));
    }
    if (splitReadVariantSupport != null) {
      attrs.add(Joiner.on("").join(tripleQuote("srv"), ":", splitReadVariantSupport));
    }
    if (averageMappingQuality != null) {
      attrs.add(Joiner.on("").join(tripleQuote("amq"), ":", averageMappingQuality));
    }
    if (copyNumber != null) {
      attrs.add(Joiner.on("").join(tripleQuote("cn"), ":", copyNumber));
    }
    if (averageNormalizedCoverage != null) {
      attrs.add(Joiner.on("").join(tripleQuote("anc"), ":", averageNormalizedCoverage));
    }
    if (pointCount != null) {
      attrs.add(Joiner.on("").join(tripleQuote("pc"), ":", pointCount));
    }
    return Joiner.on("").join(tripleQuote(sampleName), ":{", Joiner.on(",").join(attrs), "}");
  }

  public String getSampleName() {
    return sampleName;
  }

  public String getGenotype() {
    return genotype;
  }

  public List<String> getFilters() {
    return filters;
  }

  public Integer getGenotypeQuality() {
    return genotypeQuality;
  }

  public Integer getPairedEndCoverage() {
    return pairedEndCoverage;
  }

  public Integer getPairedEndVariantSupport() {
    return pairedEndVariantSupport;
  }

  public Integer getSplitReadCoverage() {
    return splitReadCoverage;
  }

  public Integer getSplitReadVariantSupport() {
    return splitReadVariantSupport;
  }

  public Integer getAverageMappingQuality() {
    return averageMappingQuality;
  }

  public Integer getCopyNumber() {
    return copyNumber;
  }

  public Double getAverageNormalizedCoverage() {
    return averageNormalizedCoverage;
  }

  public Integer getPointCount() {
    return pointCount;
  }

  @Override
  public String toString() {
    return "SampleGenotype{"
        + "sampleName='"
        + sampleName
        + '\''
        + ", genotype='"
        + genotype
        + '\''
        + ", filters="
        + filters
        + ", genotypeQuality="
        + genotypeQuality
        + ", pairedEndCoverage="
        + pairedEndCoverage
        + ", pairedEndVariantSupport="
        + pairedEndVariantSupport
        + ", splitReadCoverage="
        + splitReadCoverage
        + ", splitReadVariantSupport="
        + splitReadVariantSupport
        + ", averageMappingQuality="
        + averageMappingQuality
        + ", copyNumber="
        + copyNumber
        + ", averageNormalizedCoverage="
        + averageNormalizedCoverage
        + ", pointCount="
        + pointCount
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SampleGenotype that = (SampleGenotype) o;
    return Objects.equal(getSampleName(), that.getSampleName())
        && Objects.equal(getGenotype(), that.getGenotype())
        && Objects.equal(getFilters(), that.getFilters())
        && Objects.equal(getGenotypeQuality(), that.getGenotypeQuality())
        && Objects.equal(getPairedEndCoverage(), that.getPairedEndCoverage())
        && Objects.equal(getPairedEndVariantSupport(), that.getPairedEndVariantSupport())
        && Objects.equal(getSplitReadCoverage(), that.getSplitReadCoverage())
        && Objects.equal(getSplitReadVariantSupport(), that.getSplitReadVariantSupport())
        && Objects.equal(getAverageMappingQuality(), that.getAverageMappingQuality())
        && Objects.equal(getCopyNumber(), that.getCopyNumber())
        && Objects.equal(getAverageNormalizedCoverage(), that.getAverageNormalizedCoverage())
        && Objects.equal(getPointCount(), that.getPointCount());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getSampleName(),
        getGenotype(),
        getFilters(),
        getGenotypeQuality(),
        getPairedEndCoverage(),
        getPairedEndVariantSupport(),
        getSplitReadCoverage(),
        getSplitReadVariantSupport(),
        getAverageMappingQuality(),
        getCopyNumber(),
        getAverageNormalizedCoverage(),
        getPointCount());
  }
}
