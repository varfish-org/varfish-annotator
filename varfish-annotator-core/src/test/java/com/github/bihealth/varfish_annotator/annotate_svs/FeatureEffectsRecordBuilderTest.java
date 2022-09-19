package com.github.bihealth.varfish_annotator.annotate_svs;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class FeatureEffectsRecordBuilderTest {

  @Test
  void testClass() {
    final FeatureEffectsRecordBuilder builder = new FeatureEffectsRecordBuilder();
    builder.setCaseId("caseId");
    builder.setSetId("setId");
    builder.setSvUuid("svUuid");
    builder.setRefseqGeneId("refseqGeneId");
    builder.setRefseqTranscriptId("refseqTranscriptId");
    builder.setRefseqTranscriptCoding(true);
    builder.setRefseqEffect(ImmutableList.of("eff1", "eff2"));
    builder.setEnsemblGeneId("ensemblGeneId");
    builder.setEnsemblTranscriptId("ensemblTranscriptId");
    builder.setEnsemblTranscriptCoding(true);
    builder.setEnsemblEffect(ImmutableList.of("eff3", "eff4"));

    Assertions.assertEquals(
        "FeatureEffectsRecordBuilder{caseId='caseId', setId='setId', svUuid='svUuid', refseqGeneId='refseqGeneId', refseqTranscriptId='refseqTranscriptId', refseqTranscriptCoding=true, refseqEffect=[eff1, eff2], ensemblGeneId='ensemblGeneId', ensemblTranscriptId='ensemblTranscriptId', ensemblTranscriptCoding=true, ensemblEffect=[eff3, eff4]}",
        builder.toString());

    Assertions.assertEquals("caseId", builder.getCaseId());
    Assertions.assertEquals("setId", builder.getSetId());
    Assertions.assertEquals("svUuid", builder.getSvUuid());
    Assertions.assertEquals("refseqGeneId", builder.getRefseqGeneId());
    Assertions.assertEquals("refseqTranscriptId", builder.getRefseqTranscriptId());
    Assertions.assertEquals(true, builder.getRefseqTranscriptCoding());
    Assertions.assertEquals("[eff1, eff2]", builder.getRefseqEffect().toString());
    Assertions.assertEquals("ensemblGeneId", builder.getEnsemblGeneId());
    Assertions.assertEquals("ensemblTranscriptId", builder.getEnsemblTranscriptId());
    Assertions.assertEquals(true, builder.getEnsemblTranscriptCoding());
    Assertions.assertEquals("[eff3, eff4]", builder.getEnsemblEffect().toString());

    final FeatureEffectsRecord record = builder.build();
  }
}
