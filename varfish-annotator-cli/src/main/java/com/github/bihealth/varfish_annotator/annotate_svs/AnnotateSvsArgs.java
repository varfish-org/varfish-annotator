package com.github.bihealth.varfish_annotator.annotate_svs;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.ArrayList;
import java.util.List;

/**
 * JCommander command for <tt>varfish_annotator annotate-sv</tt>.
 *
 * @author <a href="mailto:manuel.holtgrewe@bihealth.de">Manuel Holtgrewe</a>
 */
@Parameters(commandDescription = "Annotate VCF to TSV files")
public final class AnnotateSvsArgs {

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
      names = "--db-path",
      description = "Path to H2 file to initialize/update",
      required = true)
  private String dbPath;

  @Parameter(names = "--release", description = "The genome release used", required = true)
  private String release;

  @Parameter(
      names = "--self-test-chr1-only",
      description = "Self-test with chr1 only (for tests)",
      hidden = true)
  private boolean selfTestChr1Only = false;

  @Parameter(
      names = "--self-test-chr22-only",
      description = "Self-test with chr22 only (for tests)",
      hidden = true)
  private boolean selfTestChr22Only = false;

  @Parameter(names = "--case-id", description = "The value to use for case ID")
  private String caseId = ".";

  @Parameter(names = "--set-id", description = "The value to use for set ID")
  private String setId = ".";

  @Parameter(
      names = "--input-vcf",
      description =
          "Path to input VCF file to annotate.  You can specify this multiple times to read in files from "
              + "different callers, for example.  In this case, the variants will be merged based on the "
              + "--merge-* arguments.",
      required = true)
  private List<String> inputVcf = new ArrayList<>();

  @Parameter(
      names = "--input-ped",
      description = "Path to PED file to use for getting biological sex information",
      required = false)
  private String inputPed;

  @Parameter(
      names = "--output-gts",
      description = "Path to output TSV file with SVs and genotype calls",
      required = true)
  private String outputGts;

  @Parameter(
      names = "--output-feature-effects",
      description = "Path to output TSV file with annotation of effects on features",
      required = true)
  private String outputFeatureEffects;

  @Parameter(
      names = "--output-db-info",
      description = "Path to output TSV file with annotation DB versions",
      required = true)
  private String outputDbInfos;

  @Parameter(
      names = "--contig-regex",
      description = "Regular expression to use for selection of contigs")
  private String contigRegex = "^(chr)?(\\d+|X|Y|M|MT)$";

  @Parameter(
      names = "--default-sv-method",
      description = "String to use for INFO/SVMETHOD if missing")
  private String defaultSvMethod = ".";

  @Parameter(
      names = "--sequential-uuids",
      description = "Generate UUIDs sequentially (for testing)")
  private Boolean sequentialUuids = false;

  @Parameter(names = "--opt-out", description = "Features to opt out for")
  private String optOutFeatures = "";

  @Parameter(names = "--write-bnd-mate", description = "Write out BND mates (true, false, or auto)")
  private String writeBndMates = "auto";

  @Parameter(
      names = "skip-filters",
      description =
          "Skip records with this filter value (comma-separated list), default is 'LowQual'")
  private String skipFilters = "LowQual";

  @Parameter(
      names = "--coverage-vcf",
      description =
          "Annotate CNV with coverage and mapping quality from maelstrom-core coverage VCF file")
  private List<String> coverageVcfs = new ArrayList<>();

  @Parameter(
      names = "--merge-overlap",
      description = "Reciprocal overlap to require for merging (default: 0.75)")
  private double mergeOverlap = 0.75;

  @Parameter(
      names = "--merge-bnd-radius",
      description = "Merge BNDs within the given radius (default: 50)")
  private int mergeBndRadius = 50;

  public String getRefseqSerPath() {
    return refseqSerPath;
  }

  public String getEnsemblSerPath() {
    return ensemblSerPath;
  }

  public String getDbPath() {
    return dbPath;
  }

  public String getRelease() {
    return release;
  }

  public boolean isSelfTestChr1Only() {
    return selfTestChr1Only;
  }

  public boolean isSelfTestChr22Only() {
    return selfTestChr22Only;
  }

  public String getCaseId() {
    return caseId;
  }

  public String getSetId() {
    return setId;
  }

  public List<String> getInputVcf() {
    return inputVcf;
  }

  public String getInputPed() {
    return inputPed;
  }

  public String getOutputGts() {
    return outputGts;
  }

  public String getOutputFeatureEffects() {
    return outputFeatureEffects;
  }

  public String getContigRegex() {
    return contigRegex;
  }

  public String getOutputDbInfos() {
    return outputDbInfos;
  }

  public String getDefaultSvMethod() {
    return defaultSvMethod;
  }

  public Boolean getSequentialUuids() {
    return sequentialUuids;
  }

  public String getOptOutFeatures() {
    return optOutFeatures;
  }

  public String getWriteBndMates() {
    return writeBndMates;
  }

  public String getSkipFilters() {
    return skipFilters;
  }

  public List<String> getCoverageVcfs() {
    return coverageVcfs;
  }

  public double getMergeOverlap() {
    return mergeOverlap;
  }

  public int getMergeBndRadius() {
    return mergeBndRadius;
  }

  @Override
  public String toString() {
    return "AnnotateSvsArgs{"
        + "help="
        + help
        + ", refseqSerPath='"
        + refseqSerPath
        + '\''
        + ", ensemblSerPath='"
        + ensemblSerPath
        + '\''
        + ", dbPath='"
        + dbPath
        + '\''
        + ", release='"
        + release
        + '\''
        + ", selfTestChr1Only="
        + selfTestChr1Only
        + ", selfTestChr22Only="
        + selfTestChr22Only
        + ", caseId='"
        + caseId
        + '\''
        + ", setId='"
        + setId
        + '\''
        + ", inputVcf='"
        + inputVcf
        + '\''
        + ", inputPed='"
        + inputPed
        + '\''
        + ", outputGts='"
        + outputGts
        + '\''
        + ", outputFeatureEffects='"
        + outputFeatureEffects
        + '\''
        + ", outputDbInfos='"
        + outputDbInfos
        + '\''
        + ", contigRegex='"
        + contigRegex
        + '\''
        + ", defaultSvMethod='"
        + defaultSvMethod
        + '\''
        + ", sequentialUuids="
        + sequentialUuids
        + ", optOutFeatures='"
        + optOutFeatures
        + '\''
        + ", writeBndMates='"
        + writeBndMates
        + '\''
        + ", skipFilters='"
        + skipFilters
        + '\''
        + ", coverageVcfs="
        + coverageVcfs
        + ", mergeOverlap="
        + mergeOverlap
        + ", mergeBndRadius="
        + mergeBndRadius
        + '}';
  }
}
