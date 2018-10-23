package com.github.bihealth.varfish_annotator.init_db;

import java.sql.Connection;

/**
 * Implementation of gnomAD genomes import.
 *
 * <p>The code will also normalize the gnomAD data per-variant.
 */
public final class GnomadExomesImporter extends GnomadImporter {

  public GnomadExomesImporter(Connection conn, String gnomadVcfPath, String refFastaPath) {
    super(conn, gnomadVcfPath, refFastaPath);
  }

  /** The name of the table in the database. */
  protected String getTableName() {
    return "gnomad_exome_var";
  }

  /** The field prefix. */
  protected String getFieldPrefix() {
    return "gnomad_exome";
  }
}
