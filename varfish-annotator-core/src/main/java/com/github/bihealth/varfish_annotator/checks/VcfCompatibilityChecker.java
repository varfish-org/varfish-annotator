package com.github.bihealth.varfish_annotator.checks;

import com.github.bihealth.varfish_annotator.data.GenomeVersion;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.variant.vcf.VCFContigHeaderLine;
import htsjdk.variant.vcf.VCFFileReader;
import java.util.List;

/**
 * Check VCF file for compatibility with annotation.
 *
 * <p>At the moment, only a simple check is implemented that tests whether the dataset looks like
 * GRCh37/hg19 as this is the Genome build that VarFish supports.
 */
public class VcfCompatibilityChecker {

  /** Length of chr1 in hg19. */
  private static final int CHR1_HG19_LENGTH = 249250621;
  /** Length of chr1 in hg38. */
  private static final int CHR1_HG38_LENGTH = 248956422;

  /** The {@link VCFFileReader} that is to be used for checking. */
  private VCFFileReader reader;

  /**
   * Construct a new {@link VcfCompatibilityChecker}.
   *
   * @param reader The {@link VCFFileReader} to use for checking headers etc.
   */
  public VcfCompatibilityChecker(VCFFileReader reader) {
    this.reader = reader;
  }

  /**
   * Check whether the VCF file given to the construtor as <tt>reader</tt> looks to be compatible.
   *
   * <p>Throws an exception in case of problems and otherwise just returns. Will print a warning to
   * stderr if no reliable decision could be made.
   *
   * @param requiredRelease The required release.
   * @throws IncompatibleVcfException If the VCF file looks to be incompatible.
   */
  public void check(String requiredRelease) throws IncompatibleVcfException {
    // Check whether this looks like GRCh37/h19.
    GenomeVersion genomeVersion = this.guessGenomeVersion();
    if (genomeVersion == GenomeVersion.GRCH37 || genomeVersion == GenomeVersion.HG19) {
      System.err.println(
          "INFO: Genome looks like GRCh" + 37 + " (sequence only; regardless of 'chr' prefix).");
      if (!"GRCh37".equals(requiredRelease)) {
        throw new IncompatibleVcfException(
            "VCF file looks like hg37 by chr1 length you required " + requiredRelease);
      }
    } else if (genomeVersion == GenomeVersion.GRCH38 || genomeVersion == GenomeVersion.HG38) {
      System.err.println(
          "INFO: Genome looks like GRCh" + 38 + " (sequence only; regardless of 'chr' prefix).");
      if (!"GRCh38".equals(requiredRelease)) {
        throw new IncompatibleVcfException(
            "VCF file looks like hg38 by chr1 length you required " + requiredRelease);
      }
    } else {
      System.err.println("WARNING: VCF file did not contain contig line for '1' or 'chr1'");
      System.err.println("WARNING: Will proceed as if it is hg19/GRCh37.");
    }
  }

  public GenomeVersion guessGenomeVersion() {
    final List<VCFContigHeaderLine> contigLines = this.reader.getHeader().getContigLines();
    if (contigLines.isEmpty()) {
      System.err.println("WARNING: VCF file did not contain any contig lines.");
      return null;
    } else {
      for (VCFContigHeaderLine line : contigLines) {
        final SAMSequenceRecord seqRecord = line.getSAMSequenceRecord();
        if (seqRecord.getSequenceName().equals("1") || seqRecord.getSequenceName().equals("chr1")) {
          if (seqRecord.getSequenceLength() == CHR1_HG19_LENGTH) {
            return seqRecord.getSequenceName().startsWith("chr")
                ? GenomeVersion.HG19
                : GenomeVersion.GRCH37;
          } else if (seqRecord.getSequenceLength() == CHR1_HG38_LENGTH) {
            return seqRecord.getSequenceName().startsWith("chr")
                ? GenomeVersion.HG38
                : GenomeVersion.GRCH37;
          }
        }
      }
    }

    return null;
  }
}
