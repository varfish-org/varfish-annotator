package com.github.bihealth.varhab.init_db;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.util.List;

/** Implementation of importing clinvar to database. */
public class ClinvarImporter {

  /** The JDBC connection. */
  private final Connection conn;
  /** Path to ClinVar TSV files */
  private final ImmutableList<String> clinvarTsvFiles;

  /**
   * Construct the <tt>ExacImporter</tt> object.
   *
   * @param conn Connection to database
   * @param clinvarTsvFiles Path to ExAC VCF path.
   */
  public ClinvarImporter(Connection conn, List<String> clinvarTsvFiles) {
    this.conn = conn;
    this.clinvarTsvFiles = ImmutableList.copyOf(clinvarTsvFiles);
  }

  /** Execute Clinvar import. */
  public void run() {
    System.err.println("Importing ClinVar...");
    System.err.println("Done with importing ClinVar...");
  }
}
