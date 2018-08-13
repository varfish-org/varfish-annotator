package com.github.bihealth.varhab;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.List;

/**
 * JCommander command for <tt>varhab init-db</tt>.
 *
 * @author <a href="mailto:manuel.holtgrewe@bihealth.de">Manuel Holtgrewe</a>
 */
@Parameters(commandDescription = "Initialize or update DB")
public final class CommandInitDb {
  @Parameter(names = "--help", help = true)
  private boolean help = false;

  @Parameter(
      names = "--db-path",
      description = "Path to H2 file to initialize/update",
      required = true)
  private String dbPath;

  @Parameter(
      names = "--exac-path",
      description =
          "Path to ExAC TSV file to use for import, " + "see documentation for more information")
  private String exacPath;

  @Parameter(
      names = "--clinvar-path",
      description =
          "Path to Clinvar TSV file(s) to use for "
              + "import, see documentation for more information")
  private List<String> clinvarPaths;

  public boolean isHelp() {
    return help;
  }

  public String getDbPath() {
    return dbPath;
  }

  public String getExacPath() {
    return exacPath;
  }

  public List<String> getClinvarPaths() {
    return clinvarPaths;
  }

  @Override
  public String toString() {
    return "CommandInitDb [help="
        + help
        + ", dbPath="
        + dbPath
        + ", exacPath="
        + exacPath
        + ", clinvarPaths="
        + clinvarPaths
        + "]";
  }
}
