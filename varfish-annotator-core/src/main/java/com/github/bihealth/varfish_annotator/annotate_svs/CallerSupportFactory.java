package com.github.bihealth.varfish_annotator.annotate_svs;

import com.google.common.collect.ImmutableList;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.File;
import java.util.Map;

public class CallerSupportFactory {
  private final ImmutableList<CallerSupport> callerSupports;
  private final CallerSupport genericCaller;

  public CallerSupportFactory(Map<String, CoverageFromMaelstromReader> coverageReaders) {
    this.callerSupports =
        ImmutableList.of(
            new CallerSupportMelt(coverageReaders),
            new CallerSupportManta(coverageReaders),
            new CallerSupportDelly2(coverageReaders),
            new CallerSupportDragenCnv(coverageReaders),
            new CallerSupportDragenSv(coverageReaders),
            new CallerSupportGatkGcnv(coverageReaders),
            new CallerSupportXhmm(coverageReaders));
    this.genericCaller = new CallerSupportGeneric(coverageReaders);
  }

  public CallerSupport getFor(File vcfFile) {
    try (VCFFileReader reader = new VCFFileReader(vcfFile, false)) {
      for (CallerSupport callerSupport : callerSupports) {
        if (callerSupport.isCompatible(reader.getHeader())) {
          return callerSupport;
        }
      }
    }
    return genericCaller;
  }
}
