package com.github.bihealth.varfish_annotator.utils;

/** Raised when the self-test fails. */
public class SelfTestFailedException extends Exception {

  public SelfTestFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public SelfTestFailedException(String message) {
    super(message);
  }

  public SelfTestFailedException(Throwable cause) {
    super(cause);
  }
}
