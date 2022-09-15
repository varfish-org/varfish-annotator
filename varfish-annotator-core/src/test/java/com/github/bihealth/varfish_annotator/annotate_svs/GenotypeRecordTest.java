package com.github.bihealth.varfish_annotator.annotate_svs;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GenotypeRecordTest {

  @Test
  public void testClass() {
    final Map<String, Object> info = ImmutableMap.of("xxx", "yyy");
    final Map<String, Object> genotypes = ImmutableMap.of("sample", ImmutableMap.of("gt", "0/1"));
    final GenotypeRecord record =
        new GenotypeRecord(
            "GRCh37",
            "chr1",
            1,
            2,
            "chr2",
            3,
            4,
            "3to5",
            123,
            456,
            5,
            6,
            7,
            8,
            "caseid",
            "setid",
            "svuuid",
            "caller",
            "svtype",
            "svtype:subtype",
            info,
            10,
            11,
            12,
            13,
            14,
            genotypes);

    Assertions.assertEquals("GRCh37", record.getRelease());
    Assertions.assertEquals("chr1", record.getChromosome());
    Assertions.assertEquals(1, record.getChromosomeNo());
    Assertions.assertEquals(2, record.getBin());
    Assertions.assertEquals("chr2", record.getChromosome2());
    Assertions.assertEquals(3, record.getChromosomeNo2());
    Assertions.assertEquals(4, record.getBin2());
    Assertions.assertEquals("3to5", record.getPeOrientation());
    Assertions.assertEquals(123, record.getStart());
    Assertions.assertEquals(456, record.getEnd());
    Assertions.assertEquals(5, record.getStartCiLeft());
    Assertions.assertEquals(6, record.getStartCiRight());
    Assertions.assertEquals(7, record.getEndCiLeft());
    Assertions.assertEquals(8, record.getEndCiRight());
    Assertions.assertEquals("caseid", record.getCaseId());
    Assertions.assertEquals("svuuid", record.getSvUuid());
    Assertions.assertEquals("caller", record.getCaller());
    Assertions.assertEquals("svtype", record.getSvType());
    Assertions.assertEquals("svtype:subtype", record.getSvSubType());
    Assertions.assertEquals("{xxx=yyy}", record.getInfo().toString());
    Assertions.assertEquals(10, record.getNumHomAlt());
    Assertions.assertEquals(11, record.getNumHomRef());
    Assertions.assertEquals(12, record.getNumHet());
    Assertions.assertEquals(13, record.getNumHemiAlt());
    Assertions.assertEquals(14, record.getNumHemiRef());
    Assertions.assertEquals("{sample={gt=0/1}}", record.getGenotype().toString());

    Assertions.assertEquals(
        "GRCh37\tchr1\t1\t2\t123\t456\t5\t6\t7\t8\tcaseid\tsetid\tsvuuid\tcaller\tsvtype\tsvtype:subtype\t{\"\"\"xxx\"\"\":\"\"\"yyy\"\"\"}\t{\"\"\"sample\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\"}}",
        record.toTsv(false, false));
    Assertions.assertEquals(
        "GRCh37\tchr1\t1\t2\t123\t456\t5\t6\t7\t8\tcaseid\tsetid\tsvuuid\tcaller\tsvtype\tsvtype:subtype\t{\"\"\"xxx\"\"\":\"\"\"yyy\"\"\"}\t10\t11\t12\t13\t14\t{\"\"\"sample\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\"}}",
        record.toTsv(false, true));
    Assertions.assertEquals(
        "GRCh37\tchr1\t1\t2\tchr2\t3\t4\t3to5\t123\t456\t5\t6\t7\t8\tcaseid\tsetid\tsvuuid\tcaller\tsvtype\tsvtype:subtype\t{\"\"\"xxx\"\"\":\"\"\"yyy\"\"\"}\t{\"\"\"sample\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\"}}",
        record.toTsv(true, false));
    Assertions.assertEquals(
        "GRCh37\tchr1\t1\t2\tchr2\t3\t4\t3to5\t123\t456\t5\t6\t7\t8\tcaseid\tsetid\tsvuuid\tcaller\tsvtype\tsvtype:subtype\t{\"\"\"xxx\"\"\":\"\"\"yyy\"\"\"}\t10\t11\t12\t13\t14\t{\"\"\"sample\"\"\":{\"\"\"gt\"\"\":\"\"\"0/1\"\"\"}}",
        record.toTsv(true, true));

    Assertions.assertTrue(record.equals(record));
    Assertions.assertEquals(record.hashCode(), -1471834423);
  }

  @Test
  void testGetHeaders() {
    Assertions.assertEquals(
        "release\tchromosome\tchromosome_no\tbin\tstart\tend\tstart_ci_left\tstart_ci_right\tend_ci_left\tend_ci_right\tcase_id\tset_id\tsv_uuid\tcaller\tsv_type\tsv_sub_type\tinfo\tgenotype",
        GenotypeRecord.tsvHeader(false, false));
    Assertions.assertEquals(
        "release\tchromosome\tchromosome_no\tbin\tstart\tend\tstart_ci_left\tstart_ci_right\tend_ci_left\tend_ci_right\tcase_id\tset_id\tsv_uuid\tcaller\tsv_type\tsv_sub_type\tinfo\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tgenotype",
        GenotypeRecord.tsvHeader(false, true));
    Assertions.assertEquals(
        "release\tchromosome\tchromosome_no\tbin\tchromosome2\tchromosome_no2\tbin2\tpe_orientation\tstart\tend\tstart_ci_left\tstart_ci_right\tend_ci_left\tend_ci_right\tcase_id\tset_id\tsv_uuid\tcaller\tsv_type\tsv_sub_type\tinfo\tgenotype",
        GenotypeRecord.tsvHeader(true, false));
    Assertions.assertEquals(
        "release\tchromosome\tchromosome_no\tbin\tchromosome2\tchromosome_no2\tbin2\tpe_orientation\tstart\tend\tstart_ci_left\tstart_ci_right\tend_ci_left\tend_ci_right\tcase_id\tset_id\tsv_uuid\tcaller\tsv_type\tsv_sub_type\tinfo\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tgenotype",
        GenotypeRecord.tsvHeader(true, true));
  }
}
