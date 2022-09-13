package com.github.bihealth.varfish_annotator.checks;

/** Raised on incompatible VCF files. */
public class IncompatibleVcfException extends Exception {

  public IncompatibleVcfException(String message) {
    super(message);
  }

  private static final long serialVersionUID = 0;
}
