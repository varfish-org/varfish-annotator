package com.github.bihealth.varfish_annotator.annotate_svs;

import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;

/** Helper that allows to read coverage from {@code }maelstrom-core bam-collect-doc} VCF files. */
public class CoverageFromMaelstromReader implements Closeable {
  private final VCFFileReader vcfReader;
  private final String sample;

  public CoverageFromMaelstromReader(File vcfPath) {
    this.vcfReader = new VCFFileReader(vcfPath);
    final List<String> samples = this.vcfReader.getHeader().getSampleNamesInOrder();
    if (samples.size() != 1) {
      throw new RuntimeException("Coverage VCF file must only have one sample but had: " + samples);
    }
    this.sample = samples.get(0);
  }

  public Result read(String chrom, int start, int end) {
    int windowSize = 0;
    double covSum = 0;
    double mqSum = 0;
    double count = 0.0;
    for (CloseableIterator<VariantContext> it = vcfReader.query(chrom, start, end);
        it.hasNext(); ) {
      final VariantContext ctx = it.next();
      final int windowEnd = ctx.getAttributeAsInt("END", 0);
      if (windowSize == 0) {
        windowSize = windowEnd - ctx.getStart() + 1;
      }

      // Use first and last window values only in fractions.
      double factor = 1.0;
      if (ctx.getStart() < start) {
        double covered = windowSize - (start - ctx.getStart());
        factor = covered / windowSize;
      } else if (windowEnd > end) {
        double covered = windowSize - (windowEnd - end);
        factor = covered / windowSize;
      }
      count += factor;

      final Genotype genotype = ctx.getGenotype(0);
      covSum += Double.parseDouble((String) genotype.getExtendedAttribute("CV")) * factor;
      mqSum += Double.parseDouble((String) genotype.getExtendedAttribute("MQ")) * factor;
    }
    if (count > 0.0) {
      return new Result(covSum / count, mqSum / count);
    } else {
      return new Result(0.0, 0.0);
    }
  }

  public String getSample() {
    return sample;
  }

  @Override
  public void close() throws IOException {
    this.vcfReader.close();
  }

  public static class Result {
    private final double medianCoverage;
    private final double medianMappingQuality;

    Result(double medianCoverage, double medianMappingQuality) {
      this.medianCoverage = medianCoverage;
      this.medianMappingQuality = medianMappingQuality;
    }

    public double getMedianCoverage() {
      return medianCoverage;
    }

    public double getMedianMappingQuality() {
      return medianMappingQuality;
    }

    @Override
    public String toString() {
      return "Result{"
          + "medianCoverage="
          + medianCoverage
          + ", medianMappingQuality="
          + medianMappingQuality
          + '}';
    }
  }
}
