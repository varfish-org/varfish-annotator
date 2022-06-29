package com.github.bihealth.varfish_annotator.utils;

/** Helper class for properly counting hemizygous variants. */
public class PseudoAutosomalRegionHelper {

  /** Return whether or not a chromosome is on chrX */
  public static boolean isChrX(String chromosome) {
    return chromosome.toLowerCase().contains("X");
  }

  /**
   * Return whether or not a position in PAR1 or PAR2.
   *
   * @param release The release to use, one of "GRCh37", "GRCh38" (case insensitive)
   * @param chromosome Chromosome name, will check for containing "X" or "Y" (case insensitive)
   * @param pos 1-based position
   * @return <code>true</code> if in PAR1 or PAR2, <code>false</code> otherwise
   */
  public static boolean isInPar(String release, String chromosome, int pos) {
    final boolean isChrX = chromosome.toLowerCase().contains("x");
    final boolean isChrY = chromosome.toLowerCase().contains("y");
    if (!isChrX && !isChrY) {
      return false;
    }
    if (release.equalsIgnoreCase("grch37")) {
      return ((isChrX && pos >= 10001 && pos <= 2781479)
          || (isChrY && pos >= 10001 && pos <= 2781479)
          || (isChrX && pos >= 155701383 && pos <= 156030895)
          || (isChrY && pos >= 56887903 && pos <= 57217415));
    } else if (release.equalsIgnoreCase("grch38")) {
      return ((isChrX && pos >= 60001 && pos <= 2699520)
          || (isChrY && pos >= 10001 && pos <= 2649520)
          || (isChrX && pos >= 154931044 && pos <= 155260560)
          || (isChrY && pos >= 59034050 && pos <= 59363566));
    } else {
      throw new RuntimeException("Invalid release: " + release);
    }
  }
}
