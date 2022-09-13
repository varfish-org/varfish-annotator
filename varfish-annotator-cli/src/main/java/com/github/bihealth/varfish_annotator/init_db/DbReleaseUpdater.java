package com.github.bihealth.varfish_annotator.init_db;

import com.github.bihealth.varfish_annotator.VarfishAnnotatorException;
import com.github.bihealth.varfish_annotator.db.DbConstants;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/** Store database release infos. */
public class DbReleaseUpdater {

  /** The JDBC connection. */
  private final Connection conn;

  /** The release info strings */
  private final List<String> dbReleaseInfos;

  /**
   * Construct the <tt>DbReleaseUpdater</tt> object.
   *
   * @param conn Connection to database
   * @param dbReleaseInfos Database release information.
   */
  public DbReleaseUpdater(Connection conn, List<String> dbReleaseInfos) {
    this.conn = conn;
    this.dbReleaseInfos = dbReleaseInfos;
  }

  /** Save database release infos. */
  public void run() throws VarfishAnnotatorException {
    System.err.println("Re-creating table in database...");
    recreateTable();

    System.err.println("Updating release information table...");

    updateReleaseInfos();

    System.err.println("Done with updating release information table...");
  }

  /** Create the release information table if necessary. */
  private void recreateTable() throws VarfishAnnotatorException {
    final String createQuery =
        "CREATE TABLE IF NOT EXISTS "
            + DbConstants.TABLE_NAME
            + "("
            + "db_name VARCHAR(100) NOT NULL PRIMARY KEY, "
            + "release VARCHAR(100) NOT NULL, "
            + ")";
    try (PreparedStatement stmt = conn.prepareStatement(createQuery)) {
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem with CREATE TABLE statement", e);
    }
  }

  /** Update the release information */
  private void updateReleaseInfos() throws VarfishAnnotatorException {
    final String insertQuery =
        "MERGE INTO " + DbConstants.TABLE_NAME + " (db_name, release)" + " VALUES (?, ?)";

    for (String line : dbReleaseInfos) {
      String[] arr = line.split(":");
      try {
        System.err.println("Updating " + arr[0] + " -> " + arr[1]);
        final PreparedStatement stmt = conn.prepareStatement(insertQuery);
        stmt.setString(1, arr[0]);
        stmt.setString(2, arr[1]);
        stmt.executeUpdate();
        stmt.close();
      } catch (SQLException e) {
        throw new VarfishAnnotatorException("Problem updating database", e);
      }
    }
  }
}
