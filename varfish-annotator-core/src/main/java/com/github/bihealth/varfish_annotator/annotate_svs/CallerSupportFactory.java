package com.github.bihealth.varfish_annotator.annotate_svs;

import com.google.common.collect.ImmutableList;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.File;

public class CallerSupportFactory {
  private static ImmutableList<CallerSupport> CALLER_SUPPORTS =
      ImmutableList.of(
          new CallerSupportManta(),
          new CallerSupportDelly2(),
          new CallerSupportDragenCnv(),
          new CallerSupportDragenSv(),
          new CallerSupportGatkGcnv(),
          new CallerSupportXhmm());
  private static CallerSupport GENERIC_CALLER = new CallerSupportGeneric();

  public static CallerSupport getFor(File vcfFile) {
    try (VCFFileReader reader = new VCFFileReader(vcfFile, false)) {
      for (CallerSupport callerSupport : CALLER_SUPPORTS) {
        if (callerSupport.isCompatible(reader.getHeader())) {
          return callerSupport;
        }
      }
    }
    return GENERIC_CALLER;
  }
}
