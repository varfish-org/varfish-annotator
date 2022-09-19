package com.github.bihealth.varfish_annotator.annotate_svs;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class FeatureEffectsRecordTest {

  @Test
  void testClass() {
    final FeatureEffectsRecord record =
        new FeatureEffectsRecord(
            "caseId",
            "setId",
            "svUuid",
            "refseqGeneId",
            "refseqTranscriptId",
            true,
            ImmutableList.of("eff1", "eff2"),
            "ensemblGeneId",
            "ensemblTranscriptId",
            true,
            ImmutableList.of("eff3", "eff4"));

    Assertions.assertEquals("caseId", record.getCaseId());
    Assertions.assertEquals("setId", record.getSetId());
    Assertions.assertEquals("svUuid", record.getSvUuid());
    Assertions.assertEquals("refseqGeneId", record.getRefseqGeneId());
    Assertions.assertEquals("refseqTranscriptId", record.getRefseqTranscriptId());
    Assertions.assertEquals(true, record.getRefseqTranscriptCoding());
    Assertions.assertEquals("[eff1, eff2]", record.getRefseqEffect().toString());
    Assertions.assertEquals("ensemblGeneId", record.getEnsemblGeneId());
    Assertions.assertEquals("ensemblTranscriptId", record.getEnsemblTranscriptId());
    Assertions.assertEquals(true, record.getEnsemblTranscriptCoding());
    Assertions.assertEquals("[eff3, eff4]", record.getEnsemblEffect().toString());

    Assertions.assertTrue(record.equals(record));
    Assertions.assertEquals(
        "FeatureEffectsRecord{caseId='caseId', setId='setId', svUuid='svUuid', refseqGeneId='refseqGeneId', refseqTranscriptId='refseqTranscriptId', refseqTranscriptCoding=true, refseqEffect=[eff1, eff2], ensemblGeneId='ensemblGeneId', ensemblTranscriptId='ensemblTranscriptId', ensemblTranscriptCoding=true, ensemblEffect=[eff3, eff4]}",
        record.toString());
  }
}
