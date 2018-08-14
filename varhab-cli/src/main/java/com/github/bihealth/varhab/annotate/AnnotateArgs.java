package com.github.bihealth.varhab.annotate;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * JCommander command for <tt>varhab annotate</tt>.
 *
 * @author <a href="mailto:manuel.holtgrewe@bihealth.de">Manuel Holtgrewe</a>
 */
@Parameters(commandDescription = "Annotate VCF to TSV files")
public final class AnnotateArgs {
  @Parameter(names = "--help", help = true)
  private boolean help = false;

  @Parameter(names = "--ser-path", description = "Path to Jannovar .ser file", required = true)
  private String serPath;

  @Parameter(
      names = "--db-path",
      description = "Path to H2 file to initialize/update",
      required = true)
  private String dbPath;

  @Parameter(names = "--release", description = "The genome release used", required = true)
  private String release;

  @Parameter(names = "--case-id", description = "The value to use for case ID", required = true)
  private String caseId;

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

  public String getSerPath() {
    return serPath;
  }

  public String getDbPath() {
    return dbPath;
  }

  public String getRelease() {
    return release;
  }

  public String getCaseId() {
    return caseId;
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
    return "AnnotateArgs{"
        + "help="
        + help
        + ", serPath='"
        + serPath
        + '\''
        + ", dbPath='"
        + dbPath
        + '\''
        + ", release='"
        + release
        + '\''
        + ", caseId='"
        + caseId
        + '\''
        + ", inputVcf='"
        + inputVcf
        + '\''
        + ", outputGts='"
        + outputGts
        + '\''
        + ", outputVars='"
        + outputVars
        + '\''
        + '}';
  }
}
