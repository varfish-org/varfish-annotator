package com.github.bihealth.varfish_annotator.utils;

public class StringUtils {

  /**
   * (Overly) simple helper for escaping {@code s}.
   *
   * @param s String to escape.
   * @return Escaped version of {@code s}.
   */
  public static String tripleQuote(String s) {
    return "\"\"\"" + s.replaceAll("\"\"\"", "") + "\"\"\"";
  }
}
