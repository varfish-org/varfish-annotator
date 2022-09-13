package com.github.bihealth.varfish_annotator.annotate_svs;

import java.util.ArrayList;
import java.util.List;

public final class SampleGenotypeBuilder {
  private String sampleName;
  private String genotype;
  private List<String> filters = new ArrayList<>();
  private Integer genotypeQuality;
  private Integer pairedEndCoverage;
  private Integer pairedEndVariantSupport;
  private Integer splitReadCoverage;
  private Integer splitReadVariantSupport;
  private Integer averageMappingQuality;
  private Integer copyNumber;
  private Double averageNormalizedCoverage;
  private Integer pointCount;

  public SampleGenotype build() {
    return new SampleGenotype(
        sampleName,
        genotype,
        filters,
        genotypeQuality,
        pairedEndCoverage,
        pairedEndVariantSupport,
        splitReadCoverage,
        splitReadVariantSupport,
        averageMappingQuality,
        copyNumber,
        averageNormalizedCoverage,
        pointCount);
  }

  public String getSampleName() {
    return sampleName;
  }

  public void setSampleName(String sampleName) {
    this.sampleName = sampleName;
  }

  public String getGenotype() {
    return genotype;
  }

  public void setGenotype(String genotype) {
    this.genotype = genotype;
  }

  public List<String> getFilters() {
    return filters;
  }

  public void setFilters(List<String> filters) {
    this.filters = new ArrayList<>();
    this.filters.addAll(filters);
  }

  public Integer getGenotypeQuality() {
    return genotypeQuality;
  }

  public void setGenotypeQuality(Integer genotypeQuality) {
    this.genotypeQuality = genotypeQuality;
  }

  public Integer getPairedEndCoverage() {
    return pairedEndCoverage;
  }

  public void setPairedEndCoverage(Integer pairedEndCoverage) {
    this.pairedEndCoverage = pairedEndCoverage;
  }

  public Integer getPairedEndVariantSupport() {
    return pairedEndVariantSupport;
  }

  public void setPairedEndVariantSupport(Integer pairedEndVariantSupport) {
    this.pairedEndVariantSupport = pairedEndVariantSupport;
  }

  public Integer getSplitReadCoverage() {
    return splitReadCoverage;
  }

  public void setSplitReadCoverage(Integer splitReadCoverage) {
    this.splitReadCoverage = splitReadCoverage;
  }

  public Integer getSplitReadVariantSupport() {
    return splitReadVariantSupport;
  }

  public void setSplitReadVariantSupport(Integer splitReadVariantSupport) {
    this.splitReadVariantSupport = splitReadVariantSupport;
  }

  public Integer getAverageMappingQuality() {
    return averageMappingQuality;
  }

  public void setAverageMappingQuality(Integer averageMappingQuality) {
    this.averageMappingQuality = averageMappingQuality;
  }

  public Integer getCopyNumber() {
    return copyNumber;
  }

  public void setCopyNumber(Integer copyNumber) {
    this.copyNumber = copyNumber;
  }

  public Double getAverageNormalizedCoverage() {
    return averageNormalizedCoverage;
  }

  public void setAverageNormalizedCoverage(Double averageNormalizedCoverage) {
    this.averageNormalizedCoverage = averageNormalizedCoverage;
  }

  public Integer getPointCount() {
    return pointCount;
  }

  public void setPointCount(Integer pointCount) {
    this.pointCount = pointCount;
  }

  @Override
  public String toString() {
    return "SampleGenotypeBuilder{"
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
}
