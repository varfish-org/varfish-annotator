package com.github.bihealth.varfish_annotator.annotate_svs;

import com.google.common.collect.ImmutableList;
import java.util.TreeMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GenotypeRecordBuilderTest {

  @Test
  void testClass() {
    GenotypeRecordBuilder builder = new GenotypeRecordBuilder();

    builder.setRelease("GRCh37");
    builder.setChromosome("chr1");
    builder.setChromosomeNo(1);
    builder.setBin(2);
    builder.setChromosome2("chr2");
    builder.setChromosomeNo2(3);
    builder.setBin2(4);
    builder.setPeOrientation("3to5");
    builder.setStart(123);
    builder.setEnd(456);
    builder.setStartCiLeft(5);
    builder.setStartCiRight(6);
    builder.setEndCiLeft(7);
    builder.setEndCiRight(8);
    builder.setCaseId("caseid");
    builder.setSvUuid("svuuid");
    builder.setSetId("setid");
    builder.setCaller("caller");
    builder.setCallers(ImmutableList.of("caller"));
    builder.setSvType("svtype");
    builder.setSvSubType("svtype:subtype");
    final TreeMap<String, Object> info = new TreeMap();
    info.put("xxx", "yyy");
    builder.setInfo(info);
    builder.setNumHomAlt(10);
    builder.setNumHomRef(11);
    builder.setNumHet(12);
    builder.setNumHemiAlt(13);
    builder.setNumHemiRef(14);
    final TreeMap<String, String> gt = new TreeMap();
    gt.put("gt", "0/1");
    final TreeMap<String, Object> genotype = new TreeMap();
    genotype.put("sample", gt);
    builder.setGenotype(genotype);

    final String expected =
        "GenotypeRecordBuilder{release='GRCh37', chromosome='chr1', chromosomeNo=1, bin=2, chromosome2='chr2', chromosomeNo2=3, bin2=4, peOrientation='3to5', start=123, end=456, startCiLeft=5, startCiRight=6, endCiLeft=7, endCiRight=8, caseId='caseid', setId='setid', svUuid='svuuid', caller='caller', callers='[caller]', svType='svtype', svSubType='svtype:subtype', info={xxx=yyy}, numHomAlt=10, numHomRef=11, numHet=12, numHemiAlt=13, numHemiRef=14, genotype={sample={gt=0/1}}}";
    Assertions.assertEquals(expected, builder.toString());

    Assertions.assertEquals("GRCh37", builder.getRelease());
    Assertions.assertEquals("chr1", builder.getChromosome());
    Assertions.assertEquals(1, builder.getChromosomeNo());
    Assertions.assertEquals(2, builder.getBin());
    Assertions.assertEquals("chr2", builder.getChromosome2());
    Assertions.assertEquals(3, builder.getChromosomeNo2());
    Assertions.assertEquals(4, builder.getBin2());
    Assertions.assertEquals("3to5", builder.getPeOrientation());
    Assertions.assertEquals(123, builder.getStart());
    Assertions.assertEquals(456, builder.getEnd());
    Assertions.assertEquals(5, builder.getStartCiLeft());
    Assertions.assertEquals(6, builder.getStartCiRight());
    Assertions.assertEquals(7, builder.getEndCiLeft());
    Assertions.assertEquals(8, builder.getEndCiRight());
    Assertions.assertEquals("caseid", builder.getCaseId());
    Assertions.assertEquals("svuuid", builder.getSvUuid());
    Assertions.assertEquals("caller", builder.getCaller());
    Assertions.assertEquals("[caller]", builder.getCallers().toString());
    Assertions.assertEquals("svtype", builder.getSvType());
    Assertions.assertEquals("svtype:subtype", builder.getSvSubType());
    Assertions.assertEquals("{xxx=yyy}", builder.getInfo().toString());
    Assertions.assertEquals(10, builder.getNumHomAlt());
    Assertions.assertEquals(11, builder.getNumHomRef());
    Assertions.assertEquals(12, builder.getNumHet());
    Assertions.assertEquals(13, builder.getNumHemiAlt());
    Assertions.assertEquals(14, builder.getNumHemiRef());
    Assertions.assertEquals("{sample={gt=0/1}}", builder.getGenotype().toString());

    final GenotypeRecord record = builder.build();
  }
}
