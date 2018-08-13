package com.github.bihealth.varhab;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * JCommander command for <tt>varhab annotate</tt>.
 *
 * @author <a href="mailto:manuel.holtgrewe@bihealth.de">Manuel Holtgrewe</a>
 */
@Parameters(commandDescription = "Annotate VCF to TSV files")
public final class CommandAnnotate {
  @Parameter(names = "--help", help = true)
  private boolean help = false;

  @Parameter(
      names = "--db-path",
      description = "Path to H2 file to initialize/update",
      required = true)
  private String dbPath;

  @Parameter(
      names = "--input-vcf",
      description = "Path to input VCF file to annotate",
      required = true)
  private String inputVcf;

  @Parameter(
      names = "--output-gts",
      description = "Path to output TSV file with annotated genotypes",
      required = true)
  private String outputGts;

  @Parameter(
      names = "--output-vars",
      description = "Path to output TSV file with annotated variants",
      required = true)
  private String outputVars;

  public boolean isHelp() {
    return help;
  }

  public String getDbPath() {
    return dbPath;
  }

  public String getInputVcf() {
    return inputVcf;
  }

  public String getOutputGts() {
    return outputGts;
  }

  public String getOutputVars() {
    return outputVars;
  }

  @Override
  public String toString() {
    return "CommandAnnotate [help="
        + help
        + ", dbPath="
        + dbPath
        + ", inputVcf="
        + inputVcf
        + ", outputGts="
        + outputGts
        + ", outputVars="
        + outputVars
        + "]";
  }
}
