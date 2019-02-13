package com.github.bihealth.varfish_annotator;

/** Information from variant from DB. */
public class DbInfo {

  /** Allele frequency in population with maximal frequency. */
  private final Double afPopmax;

  /** Number of total heterozygous state observation. */
  private final Integer hetTotal;

  /** Number of total homozygous state observation. */
  private final Integer homTotal;

  /** Number of total hemizygous state observation. */
  private final Integer hemiTotal;

  /** Construct with null values. */
  public static DbInfo nullValue() {
    return new DbInfo(null, null, null, null);
  }

  /** Constructor. */
  public DbInfo(Double afPopmax, Integer hetTotal, Integer homTotal, Integer hemiTotal) {
    this.afPopmax = afPopmax;
    this.hetTotal = hetTotal;
    this.homTotal = homTotal;
    this.hemiTotal = hemiTotal;
  }

  /**
   * @return String with allele frequency in population with maximal allele frequency or "." if
   *     null.
   */
  public String getAfPopmaxStr() {
    return afPopmax == null ? "0" : afPopmax.toString();
  }

  /** @return String with total number of heterozygous or "." if null. */
  public String getHetTotalStr() {
    return hetTotal == null ? "0" : hetTotal.toString();
  }

  /** @return String with total number of homozygous or "." if null. */
  public String getHomTotalStr() {
    return homTotal == null ? "0" : homTotal.toString();
  }

  /** @return String with total number of hemizygous or "." if null. */
  public String getHemiTotalStr() {
    return hemiTotal == null ? "0" : hemiTotal.toString();
  }
}
