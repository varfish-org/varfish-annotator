package com.github.bihealth.varhab.init_db;

import java.sql.Connection;

/**
 * Implementation of gnomAD exomes import.
 *
 * <p>The code will also normalize the gnomAD data per-variant.
 */
public final class GnomadGenomesImporter extends GnomadImporter {

  public GnomadGenomesImporter(Connection conn, String gnomadVcfPath, String refFastaPath) {
    super(conn, gnomadVcfPath, refFastaPath);
  }

  /** The name of the table in the database. */
  protected String getTableName() {
    return "gnomad_genome_var";
  }

  /** The field prefix. */
  protected String getFieldPrefix() {
    return "gnomad_genome";
  }
}
