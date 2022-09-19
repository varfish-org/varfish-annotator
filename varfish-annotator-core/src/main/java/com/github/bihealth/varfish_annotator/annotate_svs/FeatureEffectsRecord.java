package com.github.bihealth.varfish_annotator.annotate_svs;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import java.util.Collection;

public class FeatureEffectsRecord {
  final String caseId;
  final String setId;
  final String svUuid;
  final String refseqGeneId;
  final String refseqTranscriptId;
  final Boolean refseqTranscriptCoding;
  final ImmutableList<String> refseqEffect;
  final String ensemblGeneId;
  final String ensemblTranscriptId;
  final Boolean ensemblTranscriptCoding;
  final ImmutableList<String> ensemblEffect;

  public FeatureEffectsRecord(
      String caseId,
      String setId,
      String svUuid,
      String refseqGeneId,
      String refseqTranscriptId,
      Boolean refseqTranscriptCoding,
      Collection<String> refseqEffect,
      String ensemblGeneId,
      String ensemblTranscriptId,
      Boolean ensemblTranscriptCoding,
      Collection<String> ensemblEffect) {
    this.caseId = caseId;
    this.setId = setId;
    this.svUuid = svUuid;
    this.refseqGeneId = refseqGeneId;
    this.refseqTranscriptId = refseqTranscriptId;
    this.refseqTranscriptCoding = refseqTranscriptCoding;
    this.refseqEffect = ImmutableList.copyOf(refseqEffect);
    this.ensemblGeneId = ensemblGeneId;
    this.ensemblTranscriptId = ensemblTranscriptId;
    this.ensemblTranscriptCoding = ensemblTranscriptCoding;
    this.ensemblEffect = ImmutableList.copyOf(ensemblEffect);
  }

  public String getCaseId() {
    return caseId;
  }

  public String getSetId() {
    return setId;
  }

  public String getSvUuid() {
    return svUuid;
  }

  public String getRefseqGeneId() {
    return refseqGeneId;
  }

  public String getRefseqTranscriptId() {
    return refseqTranscriptId;
  }

  public Boolean getRefseqTranscriptCoding() {
    return refseqTranscriptCoding;
  }

  public ImmutableList<String> getRefseqEffect() {
    return refseqEffect;
  }

  public String getEnsemblGeneId() {
    return ensemblGeneId;
  }

  public String getEnsemblTranscriptId() {
    return ensemblTranscriptId;
  }

  public Boolean getEnsemblTranscriptCoding() {
    return ensemblTranscriptCoding;
  }

  public ImmutableList<String> getEnsemblEffect() {
    return ensemblEffect;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FeatureEffectsRecord that = (FeatureEffectsRecord) o;
    return Objects.equal(getCaseId(), that.getCaseId())
        && Objects.equal(getSetId(), that.getSetId())
        && Objects.equal(getSvUuid(), that.getSvUuid())
        && Objects.equal(getRefseqGeneId(), that.getRefseqGeneId())
        && Objects.equal(getRefseqTranscriptId(), that.getRefseqTranscriptId())
        && Objects.equal(getRefseqTranscriptCoding(), that.getRefseqTranscriptCoding())
        && Objects.equal(getRefseqEffect(), that.getRefseqEffect())
        && Objects.equal(getEnsemblGeneId(), that.getEnsemblGeneId())
        && Objects.equal(getEnsemblTranscriptId(), that.getEnsemblTranscriptId())
        && Objects.equal(getEnsemblTranscriptCoding(), that.getEnsemblTranscriptCoding())
        && Objects.equal(getEnsemblEffect(), that.getEnsemblEffect());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getCaseId(),
        getSetId(),
        getSvUuid(),
        getRefseqGeneId(),
        getRefseqTranscriptId(),
        getRefseqTranscriptCoding(),
        getRefseqEffect(),
        getEnsemblGeneId(),
        getEnsemblTranscriptId(),
        getEnsemblTranscriptCoding(),
        getEnsemblEffect());
  }

  @Override
  public String toString() {
    return "FeatureEffectsRecord{"
        + "caseId='"
        + caseId
        + '\''
        + ", setId='"
        + setId
        + '\''
        + ", svUuid='"
        + svUuid
        + '\''
        + ", refseqGeneId='"
        + refseqGeneId
        + '\''
        + ", refseqTranscriptId='"
        + refseqTranscriptId
        + '\''
        + ", refseqTranscriptCoding="
        + refseqTranscriptCoding
        + ", refseqEffect="
        + refseqEffect
        + ", ensemblGeneId='"
        + ensemblGeneId
        + '\''
        + ", ensemblTranscriptId='"
        + ensemblTranscriptId
        + '\''
        + ", ensemblTranscriptCoding="
        + ensemblTranscriptCoding
        + ", ensemblEffect="
        + ensemblEffect
        + '}';
  }
}
