package com.github.bihealth.varfish_annotator.init_db;

import java.sql.Connection;
import java.util.List;

/**
 * Implementation of gnomAD genomes import.
 *
 * <p>The code will also normalize the gnomAD data per-variant.
 */
public final class GnomadExomesImporter extends GnomadImporter {

  public GnomadExomesImporter(
      Connection conn,
      String genomeRelease,
      List<String> gnomadVcfPaths,
      String refFastaPath,
      String genomicRegion) {
    super(conn, genomeRelease, gnomadVcfPaths, refFastaPath, genomicRegion);
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
