package com.github.bihealth.varfish_annotator.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VariantDescriptionTest {

  @Test
  public void testGeneratedFunctions() {
    final VariantDescription desc1 = new VariantDescription("22", 1000, "G", "A");
    final VariantDescription desc2 = new VariantDescription("22", 1000, "G", "A");
    final VariantDescription desc3 = new VariantDescription("22", 1000, "G", "T");

    Assertions.assertTrue(desc1.equals(desc2));
    Assertions.assertFalse(desc1.equals(desc3));
    Assertions.assertEquals(desc1.hashCode(), 4428607);
  }
}
