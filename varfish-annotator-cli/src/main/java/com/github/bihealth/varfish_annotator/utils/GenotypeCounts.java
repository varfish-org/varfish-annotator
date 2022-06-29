package com.github.bihealth.varfish_annotator.utils;

import com.google.common.base.Joiner;
import de.charite.compbio.jannovar.pedigree.Pedigree;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import java.util.*;

/** Simple helper for counting the different genotype kinds. */
public class GenotypeCounts {

  /**
   * Count the different genotypes and return {@link GenotypeCounts}.
   *
   * @param ctx {@link VariantContext} from the input file.
   * @param alleleNo The allele number (first alternative is 1)
   * @param pedigree Pedigree ot use for sex information
   * @param release String to identify genome release.
   * @return The genotype counts for all samples.
   */
  public static GenotypeCounts buildGenotypeCounts(
      VariantContext ctx, int alleleNo, Pedigree pedigree, String release) {
    final GenotypeCounts result = new GenotypeCounts();

    for (String sample : ctx.getSampleNames()) {
      final boolean isMale =
          (pedigree != null
              && pedigree.hasPerson(sample)
              && pedigree.getNameToMember().get(sample).getPerson().isMale());

      final Genotype genotype = ctx.getGenotype(sample);
      final Map<String, String> gts = new TreeMap<>();
      final List<String> gtList = new ArrayList<>();
      for (Allele allele : genotype.getAlleles()) {
        if (allele.isNoCall()) {
          gtList.add(".");
        } else if (ctx.getAlleleIndex(allele) == alleleNo) {
          gtList.add("1");
        } else {
          gtList.add("0");
        }
      }

      final String gt;
      if (genotype.isPhased()) {
        gt = Joiner.on("|").join(gtList);
      } else {
        gtList.sort(Comparator.naturalOrder());
        gt = Joiner.on("/").join(gtList);
      }

      if (gt.equals("0/1") || gt.equals("1/0") || gt.equals("0|1") || gt.equals("1|0")) {
        result.numHet += 1;
      } else if (gt.equals("0/0") || gt.equals("0|0")) {
        if (isMale
            && PseudoAutosomalRegionHelper.isChrX(ctx.getContig())
            && !PseudoAutosomalRegionHelper.isInPar(release, ctx.getContig(), ctx.getStart())) {
          result.numHemiRef += 1;
        } else {
          result.numHomRef += 1;
        }
      } else if (gt.equals("1/1") || gt.equals("1|1")) {
        if (isMale
            && PseudoAutosomalRegionHelper.isChrX(ctx.getContig())
            && !PseudoAutosomalRegionHelper.isInPar(release, ctx.getContig(), ctx.getStart())) {
          result.numHemiAlt += 1;
        } else {
          result.numHomAlt += 1;
        }
      } else if (gt.equals("0")) {
        result.numHemiRef += 1;
      } else if (gt.equals("1")) {
        result.numHemiAlt += 1;
      }
    }

    return result;
  }

  /** Number of hom. alt. calls. */
  public int numHomAlt;
  /** Number of hom. ref. calls. */
  public int numHomRef;
  /** Number of het. calls. */
  public int numHet;
  /** Number of hemi. alt. calls. */
  public int numHemiAlt;
  /** Number of hemi. ref. calls. */
  public int numHemiRef;

  public GenotypeCounts() {
    this.numHomAlt = 0;
    this.numHomRef = 0;
    this.numHet = 0;
    this.numHemiAlt = 0;
    this.numHemiRef = 0;
  }
}
