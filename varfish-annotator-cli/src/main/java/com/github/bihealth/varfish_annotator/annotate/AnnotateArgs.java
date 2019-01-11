package com.github.bihealth.varfish_annotator.annotate;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * JCommander command for <tt>varfish_annotator annotate</tt>.
 *
 * @author <a href="mailto:manuel.holtgrewe@bihealth.de">Manuel Holtgrewe</a>
 */
@Parameters(commandDescription = "Annotate VCF to TSV files")
public final class AnnotateArgs {

  @Parameter(names = "--help", help = true)
  private boolean help = false;

  @Parameter(
      names = "--refseq-ser-path",
      description = "Path to Jannovar .ser file for RefSeq",
      required = true)
  private String refseqSerPath;

  @Parameter(
      names = "--ensembl-ser-path",
      description = "Path to Jannovar .ser file for ENSEMBL",
      required = true)
  private String ensemblSerPath;

  @Parameter(
      names = "--ref-path",
      description = "Path to reference FASTA file, used for variant normalization",
      required = true)
  private String refPath;

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

  @Parameter(
      names = "--output-db-info",
      description = "Path to output TSV file with annotation DB versions",
      required = true)
  private String outputDbInfos;

  @Parameter(
      names = "--contig-regex",
      description = "Regular expression to use for selection of contigs")
  private String contigRegex = "^(chr)?(\\d+|X|Y|M|MT)$";

  public boolean isHelp() {
    return help;
  }

  public String getRefseqSerPath() {
    return refseqSerPath;
  }

  public String getEnsemblSerPath() {
    return ensemblSerPath;
  }

  public String getRefPath() {
    return refPath;
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

  public String getContigRegex() {
    return contigRegex;
  }

  public String getOutputDbInfos() {
    return outputDbInfos;
  }

  @Override
  public String toString() {
    return "AnnotateArgs{"
        + "help="
        + help
        + ", refseqSerPath='"
        + refseqSerPath
        + '\''
        + ", ensemblSerPath='"
        + ensemblSerPath
        + '\''
        + ", refPath='"
        + refPath
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
        + ", outputDbInfos='"
        + outputDbInfos
        + '\''
        + ", contigRegex='"
        + contigRegex
        + '\''
        + '}';
  }
}
