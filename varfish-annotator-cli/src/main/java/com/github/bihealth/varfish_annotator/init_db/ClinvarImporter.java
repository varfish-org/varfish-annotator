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
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Implementation of importing ClinVar to database.
 *
 * <p>ClinVar is imported from the MacArthur TSV files which we assume to be already properly
 * normalized.
 */
public class ClinvarImporter {

  /** The name of the table in the database. */
  public static final String TABLE_NAME = "clinvar_var";

  /** The expected TSV header. */
  public static ImmutableList<String> EXPECTED_HEADER =
      ImmutableList.of(
          "release",
          "chromosome",
          "position",
          "reference",
          "alternative",
          "start",
          "stop",
          "strand",
          "variation_type",
          "variation_id",
          "rcv",
          "scv",
          "allele_id",
          "symbol",
          "hgvs_c",
          "hgvs_p",
          "molecular_consequence",
          "clinical_significance",
          "clinical_significance_ordered",
          "pathogenic",
          "likely_pathogenic",
          "uncertain_significance",
          "likely_benign",
          "benign",
          "review_status",
          "review_status_ordered",
          "last_evaluated",
          "all_submitters",
          "submitters_ordered",
          "all_traits",
          "all_pmids",
          "inheritance_modes",
          "age_of_onset",
          "prevalence",
          "disease_mechanism",
          "origin",
          "xrefs",
          "dates_ordered",
          "multi");

  /** The JDBC connection. */
  private final Connection conn;

  /** Path to ClinVar TSV files */
  private final ImmutableList<String> clinvarTsvFiles;

  /**
   * Construct the <tt>ClinvarImporter</tt> object.
   *
   * @param conn Connection to database
   * @param clinvarTsvFiles Paths to ClinVar TSV files from MacArthur repository
   */
  public ClinvarImporter(Connection conn, List<String> clinvarTsvFiles) {
    this.conn = conn;
    this.clinvarTsvFiles = ImmutableList.copyOf(clinvarTsvFiles);
  }

  /** Execute Clinvar import. */
  public void run() throws VarfishAnnotatorException {
    System.err.println("Re-creating table in database...");
    recreateTable();

    System.err.println("Importing ClinVar...");

    for (String path : clinvarTsvFiles) {
      importTsvFile(path);
    }

    System.err.println("Done with importing ClinVar...");
  }

  /**
   * Re-create the ClinVar table in the database.
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
            + "chrom VARCHAR(20) NOT NULL, "
            + "start INTEGER NOT NULL, "
            + "end INTEGER NOT NULL, "
            + "ref VARCHAR("
            + InitDb.VARCHAR_LEN
            + ") NOT NULL, "
            + "alt VARCHAR("
            + InitDb.VARCHAR_LEN
            + ") NOT NULL, "
            + ")";
    try (PreparedStatement stmt = conn.prepareStatement(createQuery)) {
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem with CREATE TABLE statement", e);
    }

    final ImmutableList<String> indexQueries =
        ImmutableList.of(
            "CREATE PRIMARY KEY ON " + TABLE_NAME + " (release, chrom, start, ref, alt)",
            "CREATE INDEX ON " + TABLE_NAME + " (release, chrom, start, end)");
    for (String query : indexQueries) {
      try (PreparedStatement stmt = conn.prepareStatement(query)) {
        stmt.executeUpdate();
      } catch (SQLException e) {
        throw new VarfishAnnotatorException("Problem with CREATE INDEX statement", e);
      }
    }
  }

  /** Import a ClinVar TSV file from the MacArthur repository. */
  private void importTsvFile(String pathTsvFile) throws VarfishAnnotatorException {
    System.err.println("Importing TSV: " + pathTsvFile);

    final String insertQuery =
        "MERGE INTO "
            + TABLE_NAME
            + " (release, chrom, start, end, ref, alt)"
            + " VALUES (?, ?, ?, ?, ?, ?)";

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
          stmt.setInt(3, Integer.parseInt(arr.get(5)));
          stmt.setInt(4, Integer.parseInt(arr.get(6)));
          stmt.setString(5, arr.get(2));
          stmt.setString(6, arr.get(3));
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
