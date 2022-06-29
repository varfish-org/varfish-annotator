package com.github.bihealth.varfish_annotator.init_db;

import com.ginsberg.junit.exit.FailOnSystemExit;
import com.github.bihealth.varfish_annotator.ResourceUtils;
import com.github.bihealth.varfish_annotator.VarfishAnnotatorCli;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Smoke tests for the <code>init-db</code> sub command with GRCh38.
 *
 * @author <a href="mailto:manuel.holtgrewe@bih-charite.de">Manuel Holtgrewe</a>
 */
public class InitDb38Test {
  @TempDir public File tmpFolder;
  File outputH2Db;
  File fastaFile;
  File faiFile;

  @BeforeEach
  void initEach() {
    outputH2Db = new File(tmpFolder + "/output.h2.db");
    fastaFile = new File(tmpFolder + "/hs38.chr1.fa");
    faiFile = new File(tmpFolder + "/hs38.chr1.fa.fai");

    ResourceUtils.gunzipResourceToFile("/grch38/hs38.chr1.fa.gz", fastaFile);
    ResourceUtils.copyResourceToFile("/grch38/hs38.chr1.fa.fai", faiFile);
  }

  void checkTable(String tableName, int expectedCount) throws SQLException {
    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:h2:"
                + outputH2Db.toString()
                + ";TRACE_LEVEL_FILE=0;MV_STORE=FALSE;MVCC=FALSE"
                + ";DB_CLOSE_ON_EXIT=FALSE",
            "sa",
            "")) {
      final String query = "SELECT COUNT(*) from " + tableName;
      final PreparedStatement stmt = conn.prepareStatement(query);
      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          Assertions.fail("SELECT COUNT(*) query failed!");
        }
        Assertions.assertEquals(expectedCount, rs.getInt(1));
      }
    }
  }

  void testTsvImportImpl(
      String fileName, String argName, String tableName, String dbReleaseInfo, int expectedCount)
      throws SQLException {
    final File pathTsv = new File(tmpFolder + "/" + fileName);

    ResourceUtils.copyResourceToFile("/grch38/" + fileName, pathTsv);

    VarfishAnnotatorCli.main(
        new String[] {
          "init-db",
          "--release",
          "GRCh38",
          "--db-release-info",
          dbReleaseInfo,
          "--ref-path",
          fastaFile.toString(),
          "--db-path",
          outputH2Db.toString(),
          argName,
          pathTsv.toString()
        });

    checkTable(tableName, expectedCount);
  }

  void testVcfImpl(
      String fileName, String argName, String tableName, String dbReleaseInfo, int expectedCount)
      throws SQLException {
    final File vcfPath = new File(tmpFolder + "/" + fileName);
    final File tbiPath = new File(vcfPath + ".tbi");

    ResourceUtils.copyResourceToFile("/grch38/" + vcfPath.getName(), vcfPath);
    ResourceUtils.copyResourceToFile("/grch38/" + tbiPath.getName(), tbiPath);

    VarfishAnnotatorCli.main(
        new String[] {
          "init-db",
          "--release",
          "GRCh38",
          "--db-release-info",
          dbReleaseInfo,
          "--ref-path",
          fastaFile.toString(),
          "--db-path",
          outputH2Db.toString(),
          argName,
          vcfPath.toString()
        });

    checkTable(tableName, expectedCount);
  }

  @FailOnSystemExit
  @Test
  void testClinvarImport() throws SQLException {
    testTsvImportImpl(
        "Clinvar.tsv", "--clinvar-path", ClinvarImporter.TABLE_NAME, "clinvar:today", 1000);
  }

  @FailOnSystemExit
  @Test
  void testHgmdImport() throws SQLException {
    testTsvImportImpl(
        "HgmdPublicLocus.tsv",
        "--hgmd-public",
        HgmdPublicImporter.TABLE_NAME,
        "hgmd_public:ensembl_r104",
        954);
  }

  @FailOnSystemExit
  @Test
  void testGnomadExomesImport() throws IOException, SQLException {
    testVcfImpl(
        "gnomad.exomes.r2.1.1.sites.chr1.stripped.vcf.bgz.gz",
        "--gnomad-exomes-path",
        GnomadExomesImporter.TABLE_NAME,
        "gnomad_exomes:r2.1.1",
        1000);
  }

  @FailOnSystemExit
  @Test
  void testGnomadGenomesImport() throws IOException, SQLException {
    testVcfImpl(
        "gnomad.genomes.r3.1.1.sites.chr1.stripped.vcf.bgz.gz",
        "--gnomad-genomes-path",
        GnomadGenomesImporter.TABLE_NAME,
        "gnomad_genomes:r3.1.1",
        1000);
  }

  /** The output of this test can be used to rebuild the <code>.h2.db</code> file. */
  @FailOnSystemExit
  @Test
  void testBuildFull() throws SQLException {
    testTsvImportImpl(
        "Clinvar.tsv", "--clinvar-path", ClinvarImporter.TABLE_NAME, "clinvar:today", 1000);
    testTsvImportImpl(
        "HgmdPublicLocus.tsv",
        "--hgmd-public",
        HgmdPublicImporter.TABLE_NAME,
        "hgmd_public:ensembl_r104",
        954);
    testVcfImpl(
        "gnomad.exomes.r2.1.1.sites.chr1.stripped.vcf.bgz.gz",
        "--gnomad-exomes-path",
        GnomadExomesImporter.TABLE_NAME,
        "gnomad_exomes:r2.1.1",
        1000);
    testVcfImpl(
        "gnomad.genomes.r3.1.1.sites.chr1.stripped.vcf.bgz.gz",
        "--gnomad-genomes-path",
        GnomadGenomesImporter.TABLE_NAME,
        "gnomad_genomes:r2.1.1",
        1000);
    //    Assertions.fail();
  }
}
