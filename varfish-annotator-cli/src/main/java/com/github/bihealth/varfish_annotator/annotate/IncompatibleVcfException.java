package com.github.bihealth.varfish_annotator.annotate;

/** Raised on incompatible VCF files. */
public class IncompatibleVcfException extends Exception {

  public IncompatibleVcfException(String message) {
    super(message);
  }
}
