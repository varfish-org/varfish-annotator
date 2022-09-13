package com.github.bihealth.varfish_annotator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** @author <a href="mailto:manuel.holtgrewe@bih-charite.de">Manuel Holtgrewe</a> */
public class VarFishAnnotatorExceptionTest {

  @Test
  public void testConstructorString() {
    Assertions.assertThrows(
        VarfishAnnotatorException.class,
        () -> {
          throw new VarfishAnnotatorException("Test Message");
        });
  }

  @Test
  public void testConstructorStringThrowable() {
    Assertions.assertThrows(
        VarfishAnnotatorException.class,
        () -> {
          throw new VarfishAnnotatorException("Test Message", new Exception());
        });
  }
}
