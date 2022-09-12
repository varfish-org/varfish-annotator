package com.github.bihealth.varfish_annotator.dbstats;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * JCommander command for <tt>varfish_annotator dbstats</tt>.
 *
 * @author <a href="mailto:manuel.holtgrewe@bihealth.de">Manuel Holtgrewe</a>
 */
@Parameters(commandDescription = "Display statistics of db contents")
public class DbStatsArgs {

  @Parameter(names = "--help", help = true)
  private boolean help = false;

  @Parameter(names = "--parseable", help = true)
  private boolean parseable = false;

  @Parameter(
      names = "--db-path",
      description = "Path to H2 file to initialize/update",
      required = true)
  private String dbPath;

  public boolean isHelp() {
    return help;
  }

  public String getDbPath() {
    return dbPath;
  }

  public boolean isParseable() {
    return parseable;
  }

  @Override
  public String toString() {
    return "DbStatsArgs{"
        + "help="
        + help
        + ", parseable="
        + parseable
        + ", dbPath='"
        + dbPath
        + '\''
        + '}';
  }
}
