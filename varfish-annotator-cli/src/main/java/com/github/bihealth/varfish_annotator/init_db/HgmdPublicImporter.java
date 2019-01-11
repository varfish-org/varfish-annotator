package com.github.bihealth.varfish_annotator.init_db;

import com.github.bihealth.varfish_annotator.VarfishAnnotatorException;
import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.zip.GZIPInputStream;

/**
 * Implementation of importing HGMD Public TSV file to database.
 *
 * <p>HGMD sites are imported from the TSV file generated from the ENSEMBL track.
 */
public class HgmdPublicImporter {

  /** The name of the table in the database. */
  public static final String TABLE_NAME = "hgmd_locus";

  /** The expected TSV header. */
  public static ImmutableList<String> EXPECTED_HEADER =
      ImmutableList.of("release", "chromosome", "start", "end", "variation_name");

  /** The JDBC connection. */
  private final Connection conn;

  /** Path to HGMD Public TSV file */
  private final String hgmdPublicFile;

  /**
   * Construct the <tt>HgmdPublicImporter</tt> object.
   *
   * @param conn Connection to database
   * @param hgmdPublicFile Paths to HGMD Public TSV files generated from ENSEMBL tracks.
   */
  public HgmdPublicImporter(Connection conn, String hgmdPublicFile) {
    this.conn = conn;
    this.hgmdPublicFile = hgmdPublicFile;
  }

  /** Execute HGMD Public import. */
  public void run() throws VarfishAnnotatorException {
    System.err.println("Re-creating table in database...");
    recreateTable();

    System.err.println("Importing HGMD Public...");

    importTsvFile(hgmdPublicFile);

    System.err.println("Done with importing HGMD Public...");
  }

  /**
   * Re-create the HGMD Public table in the database.
   *
   * <p>After calling this method, the table has been created and is empty.
   */
  private void recreateTable() throws VarfishAnnotatorException {
    final String dropQuery = "DROP TABLE IF EXISTS " + TABLE_NAME;
    try (PreparedStatement stmt = conn.prepareStatement(dropQuery)) {
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem with DROP TABLE statement", e);
    }

    final String createQuery =
        "CREATE TABLE "
            + TABLE_NAME
            + "("
            + "release VARCHAR(10) NOT NULL, "
            + "chromosome VARCHAR(20) NOT NULL, "
            + "start INTEGER NOT NULL, "
            + "end INTEGER NOT NULL, "
            + "variation_name VARCHAR("
            + InitDb.VARCHAR_LEN
            + ") NOT NULL"
            + ")";
    try (PreparedStatement stmt = conn.prepareStatement(createQuery)) {
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem with CREATE TABLE statement", e);
    }

    final ImmutableList<String> indexQueries =
        ImmutableList.of(
            "CREATE PRIMARY KEY ON " + TABLE_NAME + " (release, chromosome, start, end)",
            "CREATE INDEX ON " + TABLE_NAME + " (release, chromosome, start, end)");
    for (String query : indexQueries) {
      try (PreparedStatement stmt = conn.prepareStatement(query)) {
        stmt.executeUpdate();
      } catch (SQLException e) {
        throw new VarfishAnnotatorException("Problem with CREATE INDEX statement", e);
      }
    }
  }

  /** Import the HGMD Public TSV file generated from ENSEMBL */
  private void importTsvFile(String pathTsvFile) throws VarfishAnnotatorException {
    System.err.println("Importing TSV: " + pathTsvFile);

    final String insertQuery =
        "MERGE INTO "
            + TABLE_NAME
            + " (release, chromosome, start, end, variation_name)"
            + " VALUES (?, ?, ?, ?, ?)";

    String line = null;
    String headerLine = null;
    try (InputStream fileStream = new FileInputStream(pathTsvFile);
        InputStream gzipStream =
            pathTsvFile.endsWith(".gz") ? new GZIPInputStream(fileStream) : fileStream;
        Reader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
        BufferedReader buffered = new BufferedReader(decoder)) {
      while ((line = buffered.readLine()) != null) {
        final ImmutableList<String> arr = ImmutableList.copyOf(line.split("\t"));
        if (headerLine == null) {
          headerLine = line;
          if (!arr.equals(EXPECTED_HEADER)) {
            throw new VarfishAnnotatorException(
                "Unexpected header records: " + arr + ", expected: " + EXPECTED_HEADER);
          }
        } else {
          final PreparedStatement stmt = conn.prepareStatement(insertQuery);
          stmt.setString(1, arr.get(0));
          stmt.setString(2, arr.get(1));
          stmt.setInt(3, Integer.parseInt(arr.get(2)));
          stmt.setInt(4, Integer.parseInt(arr.get(3)));
          stmt.setString(5, arr.get(4));
          stmt.executeUpdate();
          stmt.close();
        }
      }
    } catch (IOException e) {
      throw new VarfishAnnotatorException("Problem reading gziped TSV file", e);
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem updating database", e);
    }
  }
}
