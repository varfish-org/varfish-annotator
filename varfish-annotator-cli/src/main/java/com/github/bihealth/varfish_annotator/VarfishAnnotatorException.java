package com.github.bihealth.varfish_annotator;

/** Thrown in case of problems. */
public class VarfishAnnotatorException extends Exception {
  public VarfishAnnotatorException(String message) {
    super(message);
  }

  public VarfishAnnotatorException(String message, Throwable cause) {
    super(message, cause);
  }
}
