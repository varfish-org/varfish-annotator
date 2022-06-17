package com.github.bihealth.varfish_annotator.dbstats;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.sql.*;

public class DbStats {

  /** The names of the tables to expect. */
  private static final ImmutableList<String> TABLE_NAMES =
      ImmutableList.of(
          "clinvar_var",
          "exac_var",
          "hgmd_locus",
          "thousand_genomes_var",
          "gnomad_exome_var",
          "gnomad_genome_var");

  /** Tables that only work for GRCh37. */
  private static final ImmutableList<String> GRCH37_ONLY_TABLES =
      ImmutableList.of("exac_var", "thousand_genomes_var");

  /** Configuration for the command. */
  private final DbStatsArgs args;

  /** Construct with the given configuration. */
  public DbStats(DbStatsArgs args) {
    this.args = args;
  }

  /** Execute the command. */
  public void run() {
    System.err.println("Running db-stats; args: " + args);

    String dbPath = args.getDbPath();
    if (dbPath.endsWith(".h2.db")) {
      dbPath = dbPath.substring(0, dbPath.length() - ".h2.db".length());
    }

    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:h2:"
                + dbPath
                + ";TRACE_LEVEL_FILE=0;MV_STORE=FALSE;MVCC=FALSE;ACCESS_MODE_DATA=r"
                + ";DB_CLOSE_ON_EXIT=FALSE",
            "sa",
            ""); ) {
      for (String tableName : TABLE_NAMES) {
        if (!GRCH37_ONLY_TABLES.contains(tableName) || dbPath.endsWith("grch37")) {
          runForTable(conn, tableName);
        }
      }
      System.err.println("Foo bar!");
    } catch (SQLException e) {
      System.err.println("Problem opening H2 database");
      e.printStackTrace();
      System.exit(1);
    }

    System.err.println("All done. Have a nice day!");
  }

  private void runForTable(Connection conn, String tableName) {
    System.out.println("Table " + tableName);

    final String chromCol = tableName.equals("hgmd_locus") ? "chromosome" : "chrom";

    final String query =
        "SELECT release, "
            + chromCol
            + ", COUNT(*) as count FROM "
            + tableName
            + " GROUP BY release, "
            + chromCol
            + " ORDER BY release ASC, "
            + chromCol
            + " ASC";

    if (args.isParseable()) {
      System.out.println("TABLE\tRELEASE\tCHROM\tCOUNT");
    }

    try {
      final PreparedStatement stmt = conn.prepareStatement(query);
      try (ResultSet rs = stmt.executeQuery()) {
        if (!args.isParseable()) {
          System.out.println("Table: " + tableName);
        }
        while (rs.next()) {
          System.out.println(
              Joiner.on('\t')
                  .join(
                      ImmutableList.of(
                          tableName,
                          rs.getString(1),
                          rs.getString(2),
                          String.valueOf(rs.getInt(3)))));
        }
        if (!args.isParseable()) {
          System.out.println("\n\n");
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Problem with querying table " + tableName, e);
    }
  }
}
