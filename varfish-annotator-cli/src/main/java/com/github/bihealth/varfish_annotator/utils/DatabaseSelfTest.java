package com.github.bihealth.varfish_annotator.utils;

import com.github.bihealth.varfish_annotator.annotate.AnnotateVcf;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Helper class that checks that the database has at least one annotation for each chromosome. */
public class DatabaseSelfTest {
  /* List of chromosome names */
  private static ImmutableList<String> CHROMS =
      ImmutableList.of(
          "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16",
          "17", "18", "19", "20", "21", "22", "X", "Y");

  private Connection conn;

  public DatabaseSelfTest(Connection conn) {
    this.conn = conn;
  }

  public void selfTest(String release, boolean checkChr1Only, boolean checkChr22Only)
      throws SelfTestFailedException {
    final List<String> chromNames;
    if (checkChr1Only) {
      chromNames = CHROMS.subList(0, 1);
    } else if (checkChr22Only) {
      chromNames = CHROMS.subList(21, 22);
    } else {
      chromNames = CHROMS;
    }
    if ("grch37".equals(release.toLowerCase())) {
      selfTestDb(AnnotateVcf.EXAC_PREFIX, "GRCh37", "", chromNames);
      selfTestDb(AnnotateVcf.GNOMAD_EXOMES_PREFIX, "GRCh37", "", chromNames);
      selfTestDb(AnnotateVcf.GNOMAD_GENOMES_PREFIX, "GRCh37", "", chromNames);
      selfTestDb(AnnotateVcf.THOUSAND_GENOMES_PREFIX, "GRCh37", "", chromNames);
    } else if ("grch38".equals(release.toLowerCase())) {
      selfTestDb(AnnotateVcf.GNOMAD_EXOMES_PREFIX, "GRCh38", "chr", chromNames);
      selfTestDb(AnnotateVcf.GNOMAD_GENOMES_PREFIX, "GRCh38", "chr", chromNames);
    } else {
      throw new RuntimeException("Invalid release: " + release);
    }
  }

  private void selfTestDb(
      String tablePrefix, String release, String chrPrefix, List<String> chromNames)
      throws SelfTestFailedException {
    final List<String> missingChroms = new ArrayList<>();
    final String query =
        "SELECT * FROM " + tablePrefix + "_var WHERE (release = ?) AND (chrom = ?) LIMIT 1";
    try {
      final PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, release);
      for (String chrom : chromNames) {
        final String chromName = chrPrefix + chrom;
        if ("grch37".equalsIgnoreCase(release)
            && tablePrefix.equals(AnnotateVcf.GNOMAD_GENOMES_PREFIX)
            && chromName.equals("Y")) {
          continue; // skip chrY for gnomAD genomes on GRCh37
        }
        stmt.setString(2, chromName);

        try (ResultSet rs = stmt.executeQuery()) {
          if (!rs.next()) {
            missingChroms.add(chromName);
          }
        }
      }
    } catch (SQLException e) {
      throw new SelfTestFailedException(
          "Problem with SQL query in self-test for table " + tablePrefix + "_var", e);
    }

    if (!missingChroms.isEmpty()) {
      throw new SelfTestFailedException(
          "Could not found the following chromosomes for table "
              + tablePrefix
              + "_var: "
              + missingChroms);
    }
  }
}
