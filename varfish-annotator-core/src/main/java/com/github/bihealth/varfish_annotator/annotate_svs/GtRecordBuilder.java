package com.github.bihealth.varfish_annotator.annotate_svs;

import com.github.bihealth.varfish_annotator.data.GenomeVersion;
import com.github.bihealth.varfish_annotator.utils.GenotypeCounts;
import com.github.bihealth.varfish_annotator.utils.UcscBinning;
import com.google.common.collect.ImmutableList;
import de.charite.compbio.jannovar.pedigree.Pedigree;
import de.charite.compbio.jannovar.reference.SVDescription;
import de.charite.compbio.jannovar.reference.SVGenomeVariant;
import htsjdk.variant.variantcontext.VariantContext;
import java.util.*;

/** Helper class for building record for {@code .gts.tsv} file. */
public class GtRecordBuilder {

  public static final String FEATURE_CHROM2_COLUMNS = "chrom2-columns";
  public static final String FEATURE_DBCOUNTS_COLUMNS = "dbcounts-columns";
  public static final String FEATURE_SUPPRESS_CARRIER_COUNTS = "suppress-carriers-in-info";
  public static final String FEATURE_CALLERS_ARRAY = "callers-array";

  private final String release;
  private final String svMethod;
  private final String optOutFeatures;
  private final String caseId;
  private final String setId;
  private final Pedigree pedigree;
  private final CallerSupport callerSupport;

  public GtRecordBuilder(
      String release,
      String svMethod,
      String optOutFeatures,
      String caseId,
      String setId,
      Pedigree pedigree,
      CallerSupport callerSupport) {
    this.release = release;
    this.svMethod = svMethod;
    this.optOutFeatures = optOutFeatures;
    this.caseId = caseId;
    this.setId = setId;
    this.pedigree = pedigree;
    this.callerSupport = callerSupport;
  }

  public GenotypeRecord buildRecord(
      UUID variantId,
      SVGenomeVariant svGenomeVar,
      VariantContext ctx,
      GenomeVersion genomeVersion,
      int alleleNo) {
    final GenotypeRecordBuilder builder = new GenotypeRecordBuilder();

    builder.setRelease(release);
    builder.setChromosome(
        (genomeVersion == GenomeVersion.HG19)
            ? svGenomeVar.getChrName().replaceFirst("chr", "")
            : svGenomeVar.getChrName());
    builder.setChromosomeNo(svGenomeVar.getChr());

    builder.setChromosome2(
        (genomeVersion == GenomeVersion.HG19)
            ? svGenomeVar.getChr2Name().replaceFirst("chr", "")
            : svGenomeVar.getChr2Name());
    builder.setChromosomeNo2(svGenomeVar.getChr2());

    if (svGenomeVar.getType() == SVDescription.Type.BND) {
      builder.setEnd(svGenomeVar.getPos() + 1);
      builder.setBin(UcscBinning.getContainingBin(svGenomeVar.getPos(), svGenomeVar.getPos() + 1));
      builder.setBin2(
          UcscBinning.getContainingBin(svGenomeVar.getPos() + 1, svGenomeVar.getPos() + 2));
    } else {
      builder.setEnd(svGenomeVar.getPos2());
      final int pos2 = svGenomeVar.getPos2();
      final int bin = UcscBinning.getContainingBin(svGenomeVar.getPos(), pos2);
      builder.setBin(bin);
      builder.setBin2(bin);
    }

    builder.setPeOrientation(getPeOrientation(ctx, svGenomeVar, alleleNo));
    builder.setStart(svGenomeVar.getPos() + 1);
    builder.setStartCiLeft(svGenomeVar.getPosCILowerBound());
    builder.setStartCiRight(svGenomeVar.getPosCIUpperBound());
    builder.setEndCiLeft(svGenomeVar.getPos2CILowerBound());
    builder.setEndCiRight(svGenomeVar.getPos2CIUpperBound());

    builder.setCaseId(caseId);
    builder.setSetId(setId);
    builder.setSvUuid(variantId.toString());

    builder.setCaller(svMethod);
    if (!".".equals(svMethod)) {
      builder.setCallers(ImmutableList.of(svMethod));
    }

    builder.setSvType(svGenomeVar.getType().toString());
    builder.setSvSubType(svGenomeVar.getType().toString());

    final TreeMap<String, Object> info = new TreeMap<>();
    if (!optOutFeatures.contains(FEATURE_DBCOUNTS_COLUMNS)) {
      augmentInfoValue(ctx, genomeVersion, svGenomeVar, info);
    }
    builder.setInfo(info);

    final GenotypeCounts gtCounts =
        GenotypeCounts.buildGenotypeCounts(ctx, alleleNo, pedigree, release);
    builder.setNumHomAlt(gtCounts.numHomAlt);
    builder.setNumHomRef(gtCounts.numHomRef);
    builder.setNumHet(gtCounts.numHet);
    builder.setNumHemiAlt(gtCounts.numHemiAlt);
    builder.setNumHemiRef(gtCounts.numHemiRef);

    builder.setGenotype(buildGenotypeValue(ctx, alleleNo));

    return builder.build();
  }

  private String getPeOrientation(VariantContext ctx, SVGenomeVariant svGenomeVar, int alleleNo) {
    if (svGenomeVar.getType() == SVDescription.Type.INV) {
      return "3to3";
    } else if (svGenomeVar.getType() == SVDescription.Type.DUP) {
      return "5to3";
    } else if (svGenomeVar.getType() == SVDescription.Type.DEL) {
      return "3to5";
    } else if (svGenomeVar.getType() == SVDescription.Type.BND) {
      final String altBases = ctx.getAlternateAllele(alleleNo - 1).getDisplayString();
      if (altBases.startsWith("[")) {
        return "3to5";
      } else if (altBases.startsWith("]")) {
        return "5to3";
      } else if (altBases.endsWith("[")) {
        return "3to5";
      } else if (altBases.endsWith("]")) {
        return "3to3";
      } else {
        return ".";
      }
    } else {
      return ".";
    }
  }

  private static void augmentInfoValue(
      VariantContext ctx,
      GenomeVersion genomeVersion,
      SVGenomeVariant svGenomeVar,
      Map<String, Object> info) {
    if (svGenomeVar.getType() == SVDescription.Type.BND) {
      final String contigName2 =
          (genomeVersion == GenomeVersion.HG19)
              ? svGenomeVar.getChr2Name().replaceFirst("chr", "")
              : svGenomeVar.getChr2Name();

      info.put("chr2", contigName2);
      info.put("pos2", svGenomeVar.getPos2());
    }

    info.put("backgroundCarriers", ctx.getCommonInfo().getAttributeAsInt("BACKGROUND_CARRIERS", 0));
    info.put("affectedCarriers", ctx.getCommonInfo().getAttributeAsInt("AFFECTED_CARRIERS", 0));
    info.put("unaffectedCarriers", ctx.getCommonInfo().getAttributeAsInt("UNAFFECTED_CARRIERS", 0));
  }

  private Map<String, Object> buildGenotypeValue(VariantContext ctx, int alleleNo) {
    final Map<String, Object> result = new TreeMap<>();
    for (String sample : ctx.getSampleNames()) {
      result.put(sample, callerSupport.buildSampleGenotype(ctx, alleleNo, sample).toMap());
    }
    return result;
  }
}
