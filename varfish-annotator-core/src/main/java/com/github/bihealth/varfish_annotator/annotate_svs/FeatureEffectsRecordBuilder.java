package com.github.bihealth.varfish_annotator.annotate_svs;

import java.util.ArrayList;
import java.util.List;

public class FeatureEffectsRecordBuilder {
  String caseId;
  String setId;
  String svUuid;
  String refseqGeneId;
  String refseqTranscriptId;
  Boolean refseqTranscriptCoding;
  List<String> refseqEffect = new ArrayList<>();
  String ensemblGeneId;
  String ensemblTranscriptId;
  Boolean ensemblTranscriptCoding;
  List<String> ensemblEffect = new ArrayList<>();

  public FeatureEffectsRecord build() {
    return new FeatureEffectsRecord(
        caseId,
        setId,
        svUuid,
        refseqGeneId,
        refseqTranscriptId,
        refseqTranscriptCoding,
        refseqEffect,
        ensemblGeneId,
        ensemblTranscriptId,
        ensemblTranscriptCoding,
        ensemblEffect);
  }

  public String getCaseId() {
    return caseId;
  }

  public void setCaseId(String caseId) {
    this.caseId = caseId;
  }

  public String getSetId() {
    return setId;
  }

  public void setSetId(String setId) {
    this.setId = setId;
  }

  public String getSvUuid() {
    return svUuid;
  }

  public void setSvUuid(String svUuid) {
    this.svUuid = svUuid;
  }

  public String getRefseqGeneId() {
    return refseqGeneId;
  }

  public void setRefseqGeneId(String refseqGeneId) {
    this.refseqGeneId = refseqGeneId;
  }

  public String getRefseqTranscriptId() {
    return refseqTranscriptId;
  }

  public void setRefseqTranscriptId(String refseqTranscriptId) {
    this.refseqTranscriptId = refseqTranscriptId;
  }

  public Boolean getRefseqTranscriptCoding() {
    return refseqTranscriptCoding;
  }

  public void setRefseqTranscriptCoding(Boolean refseqTranscriptCoding) {
    this.refseqTranscriptCoding = refseqTranscriptCoding;
  }

  public List<String> getRefseqEffect() {
    return refseqEffect;
  }

  public void setRefseqEffect(List<String> refseqEffect) {
    this.refseqEffect = refseqEffect;
  }

  public String getEnsemblGeneId() {
    return ensemblGeneId;
  }

  public void setEnsemblGeneId(String ensemblGeneId) {
    this.ensemblGeneId = ensemblGeneId;
  }

  public String getEnsemblTranscriptId() {
    return ensemblTranscriptId;
  }

  public void setEnsemblTranscriptId(String ensemblTranscriptId) {
    this.ensemblTranscriptId = ensemblTranscriptId;
  }

  public Boolean getEnsemblTranscriptCoding() {
    return ensemblTranscriptCoding;
  }

  public void setEnsemblTranscriptCoding(Boolean ensemblTranscriptCoding) {
    this.ensemblTranscriptCoding = ensemblTranscriptCoding;
  }

  public List<String> getEnsemblEffect() {
    return ensemblEffect;
  }

  public void setEnsemblEffect(List<String> ensemblEffect) {
    this.ensemblEffect = ensemblEffect;
  }

  @Override
  public String toString() {
    return "FeatureEffectsRecordBuilder{"
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
