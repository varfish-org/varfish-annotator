package com.github.bihealth.varfish_annotator.annotate_svs;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class to implement UCSC-style binning.
 *
 * <ul>
 *   <li><a href="http://genomewiki.ucsc.edu/index.php/Bin_indexing_system">
 *       http://genomewiki.ucsc.edu/index.php/Bin_indexing_system</a>
 * </ul>
 *
 * @author <a href="mailto:manuel.holtgrewe@bihealth.de">Manuel Holtgrewe</a>
 */
final class UcscBinning {

  /** The bin offsets array. */
  private static final int binOffsets[] = {512 + 64 + 8 + 1, 64 + 8 + 1, 8 + 1, 1, 0};

  /** How much to shift to get to the finest bin. */
  private static final int binFirstShift = 17;

  /** How much to shift to get to the next larger bin. */
  private static final int binNextShift = 3;

  /** Largest position. */
  private static final int maxPos = Integer.MAX_VALUE;

  /** Largest bin. */
  private static final int maxBin = (maxPos >> binFirstShift);

  /**
   * Given an (0-based, half-open) interval {@code begin:end} return the smallest bin in which it
   * fits.
   *
   * @param begin interval start position
   * @param end interval end position
   * @return the smallest bin that contains {@code begin:end}
   */
  public static int getContainingBin(int begin, int end) {
    final BinGenerator gen = new BinGenerator(begin, end);
    while (gen.hasNext()) {
      final int[] startStop = gen.getNext();
      final int start = startStop[0];
      final int stop = startStop[1];
      if (start == stop) {
        return start;
      }
    }
    throw new RuntimeException("An unexpected error occured!");
  }

  /**
   * Given an (0-based, half-open) interval {@code begin:end} return all bins that overlap with it
   * by at least one base pair.
   *
   * @param begin interval start position
   * @param end interval end position
   * @return the smallest bin that contains {@code begin:end}
   */
  public static List<Integer> getOverlappingBins(int begin, int end) {
    final ArrayList<Integer> result = new ArrayList<>();
    final BinGenerator gen = new BinGenerator(begin, end);
    while (gen.hasNext()) {
      final int[] startStop = gen.getNext();
      final int start = startStop[0];
      final int stop = startStop[1];
      for (int bin = start; bin <= stop; ++bin) {
        result.add(bin);
      }
    }
    return result;
  }

  /**
   * Given an (0-based, half-open) interval {@code begin:end} return all bins that completely
   * contain {@code begin:end}.
   *
   * @param begin interval start position
   * @param end interval end position
   * @return the smallest bin that contains {@code begin:end}
   */
  public static List<Integer> getContainingBins(int begin, int end) {
    final ArrayList<Integer> result = new ArrayList<>();
    final int maxBin = getContainingBin(begin, end);
    for (int bin : getOverlappingBins(begin, end)) {
      if (bin <= maxBin) {
        result.add(bin);
      }
    }
    return result;
  }

  /**
   * Given an (0-based, half-open) interval {@code begin:end} return all bins that are completely
   * contained by {@code begin:end}.
   *
   * @param begin interval start position
   * @param end interval end position
   * @return the smallest bin that contains {@code begin:end}
   */
  public static List<Integer> getContainedBins(int begin, int end) {
    final ArrayList<Integer> result = new ArrayList<>();
    final int minBin = getContainingBin(begin, end);
    for (int bin : getOverlappingBins(begin, end)) {
      if (bin >= minBin) {
        result.add(bin);
      }
    }
    return result;
  }

  /** Return (0-based, half-open) interval that {@code bin} covers. */
  public static int[] getCoveredBin(int bin) {
    if (bin < 0 || bin > maxBin) {
      throw new RuntimeException("Invalid bin number " + bin + ", max bin is " + maxBin);
    }
    int shift = binFirstShift;
    for (int offset : binOffsets) {
      if (offset <= bin) {
        final int[] result = {(bin - offset) << shift, (bin + 1 - offset) << shift};
        return result;
      }
      shift += binNextShift;
    }
    throw new RuntimeException("Unexpected error when computing covered bins");
  }

  /**
   * Helper class for generating the first and last bins overlapping an interval {@code begin:end}.
   */
  private static class BinGenerator {

    /** Start bin. */
    private int startBin;
    /** Stop bin. */
    private int stopBin;
    /** Index into binOffsets */
    private int binOffsetIdx;

    /**
     * Construct new helper generator.
     *
     * @param begin interval start position
     * @param end interval end position
     */
    BinGenerator(int begin, int end) {
      if (begin < 0 || end >= maxPos) {
        throw new RuntimeException(
            "Interval " + begin + ":" + end + " is out of range, max position is " + maxPos);
      }

      this.binOffsetIdx = 0;

      // Zero-length intervals x:x are treated as `x:x+1`.
      this.startBin = begin >> binFirstShift;
      this.stopBin = Math.max(begin, end - 1) >> binFirstShift;
    }

    /**
     * Query whether a next item can be generated.
     *
     * @return {@code true} if there is another item.
     */
    boolean hasNext() {
      return binOffsetIdx < binOffsets.length;
    }

    /**
     * Get next smallest/largest interval containing {@code begin:end}.
     *
     * @return Integer array of length 2.
     */
    int[] getNext() {
      final int offset = binOffsets[binOffsetIdx++];
      final int[] result = {offset + startBin, offset + stopBin};
      startBin >>= binNextShift;
      stopBin >>= binNextShift;
      return result;
    }
  }
}
