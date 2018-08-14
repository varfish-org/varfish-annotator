package com.github.bihealth.varhab;

/**
 * Thrown in case of problems.
 */
public class VarhabException extends Exception {
  public VarhabException(String message) {
    super(message);
  }

  public VarhabException(String message, Throwable cause) {
    super(message, cause);
  }
}
