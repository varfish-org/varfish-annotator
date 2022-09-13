package com.github.bihealth.varfish_annotator.db;

import com.github.bihealth.varfish_annotator.VarfishAnnotatorException;
import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Helper class for writing out information about database to output TSV file. */
public class DbInfoWriterHelper {

  /**
   * Write information about used databases to TSV file.
   *
   * @param conn Database connection to get the information from.
   * @param dbInfoWriter Writer for database information.
   * @param genomeRelease The genome release to write out.
   * @param classForVersion The Java class to use for extracting the version.
   * @throws VarfishAnnotatorException in case of problems
   */
  public void writeDbInfos(
      Connection conn, BufferedWriter dbInfoWriter, String genomeRelease, Class classForVersion)
      throws VarfishAnnotatorException {
    try {
      dbInfoWriter.write("genomebuild\tdb_name\trelease\n");
    } catch (IOException e) {
      throw new VarfishAnnotatorException("Could not write out headers", e);
    }

    final String query =
        "SELECT db_name, release FROM " + DbConstants.TABLE_NAME + " ORDER BY db_name";
    try {
      final PreparedStatement stmt = conn.prepareStatement(query);

      try (ResultSet rs = stmt.executeQuery()) {
        while (true) {
          if (!rs.next()) {
            return;
          }
          final String versionString;
          if (rs.getString(1).equals("varfish-annotator")) {
            versionString = classForVersion.getPackage().getSpecificationVersion();
          } else {
            versionString = rs.getString(2);
          }
          dbInfoWriter.write(genomeRelease + "\t" + rs.getString(1) + "\t" + versionString + "\n");
        }
      }
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem with querying database", e);
    } catch (IOException e) {
      throw new VarfishAnnotatorException("Could not write TSV info", e);
    }
  }
}
