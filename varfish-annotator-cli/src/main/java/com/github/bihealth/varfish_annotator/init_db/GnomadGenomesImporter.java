package com.github.bihealth.varfish_annotator.init_db;

import java.sql.Connection;
import java.util.List;

/**
 * Implementation of gnomAD exomes import.
 *
 * <p>The code will also normalize the gnomAD data per-variant.
 */
public final class GnomadGenomesImporter extends GnomadImporter {

  public GnomadGenomesImporter(
      Connection conn, List<String> gnomadVcfPaths, String refFastaPath, String genomicRegion) {
    super(conn, gnomadVcfPaths, refFastaPath, genomicRegion);
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
