package com.github.bihealth.varfish_annotator.dbstats;

import com.github.bihealth.varfish_annotator.ResourceUtils;
import com.github.bihealth.varfish_annotator.VarfishAnnotatorCli;
import com.github.stefanbirkner.systemlambda.SystemLambda;
import java.io.File;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Smoke tests for the <code>db-stats</code> sub command.
 *
 * @author <a href="mailto:manuel.holtgrewe@bih-charite.de">Manuel Holtgrewe</a>
 */
public class DbStats37Chr1Test {

  @TempDir public File tmpFolder;
  File h2Db;
  File fastaFile;
  File faiFile;

  @BeforeEach
  void initEach() {
    h2Db = new File(tmpFolder + "/small-GRCh37.h2.db");
    fastaFile = new File(tmpFolder + "/hs37d5.1.fa");
    faiFile = new File(tmpFolder + "/hs37d5.1.fa.fai");

    ResourceUtils.gunzipResourceToFile("/grch37-chr1/hs37d5.1.fa.gz", fastaFile);
    ResourceUtils.copyResourceToFile("/grch37-chr1/hs37d5.1.fa.fai", faiFile);
    ResourceUtils.copyResourceToFile("/grch37-chr1/small-grch37.h2.db", h2Db);
  }

  @Test
  void testParseable() throws Exception {
    String text =
        SystemLambda.tapSystemOutNormalized(
            () -> {
              VarfishAnnotatorCli.main(
                  new String[] {
                    "db-stats", "--parseable", "--db-path", h2Db.toString(),
                  });
            });

    final String expected =
        "Table clinvar_var\n"
            + "TABLE\tRELEASE\tCHROM\tCOUNT\n"
            + "clinvar_var\tGRCh37\t1\t1000\n"
            + "Table exac_var\n"
            + "TABLE\tRELEASE\tCHROM\tCOUNT\n"
            + "exac_var\tGRCh37\t1\t1000\n"
            + "Table hgmd_locus\n"
            + "TABLE\tRELEASE\tCHROM\tCOUNT\n"
            + "hgmd_locus\tGRCh37\t1\t954\n"
            + "Table thousand_genomes_var\n"
            + "TABLE\tRELEASE\tCHROM\tCOUNT\n"
            + "thousand_genomes_var\tGRCh37\t1\t1000\n"
            + "Table gnomad_exome_var\n"
            + "TABLE\tRELEASE\tCHROM\tCOUNT\n"
            + "gnomad_exome_var\tGRCh37\t1\t1000\n"
            + "Table gnomad_genome_var\n"
            + "TABLE\tRELEASE\tCHROM\tCOUNT\n"
            + "gnomad_genome_var\tGRCh37\t1\t1000\n";

    Assertions.assertEquals(expected, text);
  }

  @Test
  void testNonParseable() throws Exception {
    String text =
        SystemLambda.tapSystemOutNormalized(
            () -> {
              VarfishAnnotatorCli.main(
                  new String[] {
                    "db-stats", "--parseable", "--db-path", h2Db.toString(),
                  });
            });
    final String expected =
        "Table clinvar_var\n"
            + "TABLE\tRELEASE\tCHROM\tCOUNT\n"
            + "clinvar_var\tGRCh37\t1\t1000\n"
            + "Table exac_var\n"
            + "TABLE\tRELEASE\tCHROM\tCOUNT\n"
            + "exac_var\tGRCh37\t1\t1000\n"
            + "Table hgmd_locus\n"
            + "TABLE\tRELEASE\tCHROM\tCOUNT\n"
            + "hgmd_locus\tGRCh37\t1\t954\n"
            + "Table thousand_genomes_var\n"
            + "TABLE\tRELEASE\tCHROM\tCOUNT\n"
            + "thousand_genomes_var\tGRCh37\t1\t1000\n"
            + "Table gnomad_exome_var\n"
            + "TABLE\tRELEASE\tCHROM\tCOUNT\n"
            + "gnomad_exome_var\tGRCh37\t1\t1000\n"
            + "Table gnomad_genome_var\n"
            + "TABLE\tRELEASE\tCHROM\tCOUNT\n"
            + "gnomad_genome_var\tGRCh37\t1\t1000\n";

    Assertions.assertEquals(expected, text);
  }
}
