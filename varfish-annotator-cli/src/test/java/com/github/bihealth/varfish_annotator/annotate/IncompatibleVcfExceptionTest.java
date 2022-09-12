package com.github.bihealth.varfish_annotator.annotate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** @author <a href="mailto:manuel.holtgrewe@bih-charite.de">Manuel Holtgrewe</a> */
public class IncompatibleVcfExceptionTest {

  @Test
  public void testConstructorString() {
    Assertions.assertThrows(
        IncompatibleVcfException.class,
        () -> {
          throw new IncompatibleVcfException("Test Message");
        });
  }
}
