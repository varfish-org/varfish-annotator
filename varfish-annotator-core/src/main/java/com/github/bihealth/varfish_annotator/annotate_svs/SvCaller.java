package com.github.bihealth.varfish_annotator.annotate_svs;

/** Enum with explicitely supported SV callers and generic support. */
public enum SvCaller {
  DELLY2_SV,
  DRAGEN_CNV,
  DRAGEN_SV,
  GATK_GCNV,
  GENERIC,
  MANTA,
  XHMM,
}
