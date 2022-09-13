package com.github.bihealth.varfish_annotator.annotate_svs;

import com.github.bihealth.varfish_annotator.ResourceUtils;
import java.io.File;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CallerSupportFactoryTest {

  @TempDir public File tmpFolder;
  File vcfFileManta;
  File vcfFileGeneric;

  @BeforeEach
  void initEach() {
    vcfFileManta = new File(tmpFolder + "/manta-head.vcf");
    ResourceUtils.copyResourceToFile("/callers-sv/manta-head.vcf", vcfFileManta);
    vcfFileGeneric = new File(tmpFolder + "/generic-head.vcf");
    ResourceUtils.copyResourceToFile("/callers-sv/generic-head.vcf", vcfFileGeneric);
  }

  @Test
  void testGetForManta() {
    CallerSupport callerSupport = CallerSupportFactory.getFor(vcfFileManta);
    Assertions.assertTrue(callerSupport instanceof CallerSupportManta);
  }

  @Test
  void testGetForGeneric() {
    CallerSupport callerSupport = CallerSupportFactory.getFor(vcfFileGeneric);
    Assertions.assertTrue(callerSupport instanceof CallerSupportGeneric);
  }
}
