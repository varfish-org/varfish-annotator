package com.github.bihealth.varfish_annotator.utils;

import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import java.util.ArrayList;
import java.util.List;

public class HtsjdkUtils {
  public static List<VCFHeaderLine> getSourceHeaderLines(VCFHeader vcfHeader) {
    final ArrayList<VCFHeaderLine> result = new ArrayList<>();
    for (VCFHeaderLine headerLine : vcfHeader.getOtherHeaderLines()) {
      if (headerLine.getKey().equals("source")) {
        result.add(headerLine);
      }
    }
    return result;
  }
}
