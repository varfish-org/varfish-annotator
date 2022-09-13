package com.github.bihealth.varfish_annotator.annotate_svs;

import static com.github.bihealth.varfish_annotator.utils.StringUtils.tripleQuote;

import com.github.bihealth.varfish_annotator.data.GenomeVersion;
import com.github.bihealth.varfish_annotator.utils.GenotypeCounts;
import com.github.bihealth.varfish_annotator.utils.UcscBinning;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import de.charite.compbio.jannovar.pedigree.Pedigree;
import de.charite.compbio.jannovar.reference.SVDescription;
import de.charite.compbio.jannovar.reference.SVGenomeVariant;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import java.util.*;
import java.util.stream.Collectors;

/** Helper class for building record for {@code .gts.tsv} file. */
public class GtRecordBuilder {

  public static final String FEATURE_CHROM2_COLUMNS = "chrom2-columns";
  public static final String FEATURE_DBCOUNTS_COLUMNS = "dbcounts-columns";
  public static final String FEATURE_SUPPRESS_CARRIER_COUNTS = "suppress-carriers-in-info";

  /** Header fields for the SV and genotype file (part 1). */
  private static final ImmutableList<String> HEADERS_GT_PART_1 =
      ImmutableList.of("release", "chromosome", "chromosome_no", "bin");
  /** Header fields for the SV and genotype file (part 2, optional). */
  private static final ImmutableList<String> HEADERS_GT_PART_2 =
      ImmutableList.of("chromosome2", "chromosome_no2", "bin2", "pe_orientation");
  /** Header fields for the SV and genotype file (part 3). */
  private static final ImmutableList<String> HEADERS_GT_PART_3 =
      ImmutableList.of(
          "start",
          "end",
          "start_ci_left",
          "start_ci_right",
          "end_ci_left",
          "end_ci_right",
          "case_id",
          "set_id",
          "sv_uuid",
          "caller",
          "sv_type",
          "sv_sub_type",
          "info");
  /** Header fields for the SV and genotype file (part 4, optional). */
  private static final ImmutableList<String> HEADERS_GT_PART_4 =
      ImmutableList.of("num_hom_alt", "num_hom_ref", "num_het", "num_hemi_alt", "num_hemi_ref");
  /** Header fields for the SV and genotype file (part 5). */
  private static final ImmutableList<String> HEADERS_GT_PART_5 = ImmutableList.of("genotype");

  private final String release;
  private final String defaultSvMethod;
  private final String optOutFeatures;
  private final String caseId;
  private final String setId;
  private final Pedigree pedigree;
  private final CallerSupport callerSupport;

  public GtRecordBuilder(
      String release,
      String defaultSvMethod,
      String optOutFeatures,
      String caseId,
      String setId,
      Pedigree pedigree,
      CallerSupport callerSupport) {
    this.release = release;
    this.defaultSvMethod = defaultSvMethod;
    this.optOutFeatures = optOutFeatures;
    this.caseId = caseId;
    this.setId = setId;
    this.pedigree = pedigree;
    this.callerSupport = callerSupport;
  }

  public List<String> getHeaders() {
    // (Conditionally) write genotype header.
    final List<java.lang.String> headers = new ArrayList<>();
    headers.addAll(HEADERS_GT_PART_1);
    if (!optOutFeatures.contains(GtRecordBuilder.FEATURE_CHROM2_COLUMNS)) {
      headers.addAll(HEADERS_GT_PART_2);
    }
    headers.addAll(HEADERS_GT_PART_3);
    if (!optOutFeatures.contains(GtRecordBuilder.FEATURE_DBCOUNTS_COLUMNS)) {
      headers.addAll(HEADERS_GT_PART_4);
    }
    headers.addAll(HEADERS_GT_PART_5);
    return headers;
  }

  public List<Object> buildRecord(
      UUID variantId,
      SVGenomeVariant svGenomeVar,
      VariantContext ctx,
      GenomeVersion genomeVersion,
      int alleleNo) {
    String svMethod = ctx.getCommonInfo().getAttributeAsString("SVMETHOD", null);
    if (svMethod == null) {
      svMethod = defaultSvMethod == null ? "." : defaultSvMethod;
    }

    final int bin;
    final int bin2;
    final int pos2;
    if (svGenomeVar.getType() == SVDescription.Type.BND) {
      pos2 = svGenomeVar.getPos() + 1;
      bin = UcscBinning.getContainingBin(svGenomeVar.getPos(), svGenomeVar.getPos() + 1);
      bin2 = UcscBinning.getContainingBin(pos2, pos2 + 1);
    } else {
      pos2 = svGenomeVar.getPos2();
      bin = UcscBinning.getContainingBin(svGenomeVar.getPos(), pos2);
      bin2 = bin;
    }

    final String contigName =
        (genomeVersion == GenomeVersion.HG19)
            ? svGenomeVar.getChrName().replaceFirst("chr", "")
            : svGenomeVar.getChrName();
    final String contigName2 =
        (genomeVersion == GenomeVersion.HG19)
            ? svGenomeVar.getChr2Name().replaceFirst("chr", "")
            : svGenomeVar.getChr2Name();

    final ImmutableList.Builder result = ImmutableList.builder();
    // part 1
    result.add(release, contigName, svGenomeVar.getChr(), bin);
    // optional part 2
    if (!optOutFeatures.contains(FEATURE_CHROM2_COLUMNS)) {
      final String peOrientation;
      if (svGenomeVar.getType() == SVDescription.Type.INV) {
        peOrientation = "3to3";
      } else if (svGenomeVar.getType() == SVDescription.Type.DUP) {
        peOrientation = "5to3";
      } else if (svGenomeVar.getType() == SVDescription.Type.DEL) {
        peOrientation = "3to5";
      } else if (svGenomeVar.getType() == SVDescription.Type.BND) {
        final String altBases = ctx.getAlternateAllele(alleleNo - 1).getDisplayString();
        if (altBases.startsWith("[")) {
          peOrientation = "3to5";
        } else if (altBases.startsWith("]")) {
          peOrientation = "5to3";
        } else if (altBases.endsWith("[")) {
          peOrientation = "3to5";
        } else if (altBases.endsWith("]")) {
          peOrientation = "3to3";
        } else {
          peOrientation = ".";
        }
      } else {
        peOrientation = ".";
      }
      result.add(contigName2, svGenomeVar.getChr2(), bin2, peOrientation);
    }
    // part 3
    result.add(
        svGenomeVar.getPos() + 1,
        pos2,
        svGenomeVar.getPosCILowerBound(),
        svGenomeVar.getPosCIUpperBound(),
        svGenomeVar.getPos2CILowerBound(),
        svGenomeVar.getPos2CIUpperBound(),
        caseId,
        setId,
        variantId.toString(),
        svMethod,
        // TODO: improve type and sub type annotation!
        svGenomeVar.getType(),
        svGenomeVar.getType(),
        buildInfoValue(ctx, genomeVersion, svGenomeVar, optOutFeatures));
    // optional part 4
    if (!optOutFeatures.contains(FEATURE_DBCOUNTS_COLUMNS)) {
      final GenotypeCounts gtCounts =
          GenotypeCounts.buildGenotypeCounts(ctx, alleleNo, pedigree, release);
      result.add(
          String.valueOf(gtCounts.numHomAlt),
          String.valueOf(gtCounts.numHomRef),
          String.valueOf(gtCounts.numHet),
          String.valueOf(gtCounts.numHemiAlt),
          String.valueOf(gtCounts.numHemiRef));
    }
    // part 5
    result.add(buildGenotypeValue(ctx, alleleNo));

    return result.build();
  }

  private static String buildInfoValue(
      VariantContext ctx,
      GenomeVersion genomeVersion,
      SVGenomeVariant svGenomeVar,
      String optOutFeatures) {
    final List<String> mappings = new ArrayList<>();

    if (!optOutFeatures.contains(FEATURE_SUPPRESS_CARRIER_COUNTS)) {
      if (svGenomeVar.getType() == SVDescription.Type.BND) {
        final String contigName2 =
            (genomeVersion == GenomeVersion.HG19)
                ? svGenomeVar.getChr2Name().replaceFirst("chr", "")
                : svGenomeVar.getChr2Name();

        mappings.add(tripleQuote("chr2") + ":" + tripleQuote(contigName2));
        mappings.add(tripleQuote("pos2") + ":" + svGenomeVar.getPos2());
      }

      mappings.add(
          tripleQuote("backgroundCarriers")
              + ":"
              + ctx.getCommonInfo().getAttributeAsInt("BACKGROUND_CARRIERS", 0));
      mappings.add(
          tripleQuote("affectedCarriers")
              + ":"
              + ctx.getCommonInfo().getAttributeAsInt("AFFECTED_CARRIERS", 0));
      mappings.add(
          tripleQuote("unaffectedCarriers")
              + ":"
              + ctx.getCommonInfo().getAttributeAsInt("UNAFFECTED_CARRIERS", 0));
    }

    return "{" + Joiner.on(",").join(mappings) + "}";
  }

  private String buildGenotypeValue(VariantContext ctx, int alleleNo) {

    // Add "GT" field.
    final List<String> mappings = new ArrayList<>();
    final Comparator<String> c = Comparator.comparing(String::toString);
    final List<String> sortedSampleNames = Lists.newArrayList(ctx.getSampleNames());
    sortedSampleNames.sort(c);
    for (String sample : sortedSampleNames) {
      mappings.add(callerSupport.buildSampleGenotype(ctx, alleleNo, sample).buildStringFragment());
    }

    return "{" + Joiner.on(",").join(mappings) + "}";
  }

  private static List<String> buildSampleGenotypeValue(
      VariantContext ctx, int alleleNo, String sample) {
    final ArrayList<String> attrs = new ArrayList<>();

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
    if (genotype.isPhased()) {
      gts.put(sample, Joiner.on("|").join(gtList));
    } else {
      gtList.sort(Comparator.naturalOrder());
      gts.put(sample, Joiner.on("/").join(gtList));
    }
    attrs.add(Joiner.on("").join(tripleQuote("gt"), ":", tripleQuote(gts.get(sample))));

    // FT -- genotype filters
    if (genotype.hasExtendedAttribute("FT")
        && genotype.getFilters() != null
        && !genotype.getFilters().equals("")) {
      final List<String> fts =
          Arrays.stream(genotype.getFilters().split(","))
              .map(s -> tripleQuote(s))
              .collect(Collectors.toList());
      attrs.add(Joiner.on("").join(tripleQuote("ft"), ":{", Joiner.on(",").join(fts), "}"));
    }

    // GQ -- genotype quality
    if (genotype.hasGQ()) {
      attrs.add(Joiner.on("").join(tripleQuote("gq"), ":", genotype.getGQ()));
    }

    // Additional integer attributes, currently Delly only.
    boolean looksLikeDelly = ctx.getAttributeAsString("SVMETHOD", "").contains("DELLY");
    boolean looksLikeXHMM = ctx.getAttributeAsString("SVMETHOD", "").contains("XHMM");
    boolean looksLikeGcnv = ctx.getAttributeAsString("SVMETHOD", "").contains("gcnvkernel");
    if (looksLikeDelly) {
      // * DR -- reference pairs
      // * DV -- variant pairs
      // * RR -- reference junction
      // * RV -- variant junction
      final int dr = Integer.parseInt(genotype.getExtendedAttribute("DR", "0").toString());
      final int dv = Integer.parseInt(genotype.getExtendedAttribute("DV", "0").toString());
      final int rr = Integer.parseInt(genotype.getExtendedAttribute("RR", "0").toString());
      final int rv = Integer.parseInt(genotype.getExtendedAttribute("RV", "0").toString());

      // Attributes to write out.
      //
      // * pec - paired end coverage
      // * pev - paired end variant support
      // * src - split read coverage
      // * srv - split read end variant support
      attrs.add(Joiner.on("").join(tripleQuote("pec"), ":", String.valueOf(dr + dv)));
      attrs.add(Joiner.on("").join(tripleQuote("pev"), ":", String.valueOf(dv)));
      attrs.add(Joiner.on("").join(tripleQuote("src"), ":", String.valueOf(rr + rv)));
      attrs.add(Joiner.on("").join(tripleQuote("srv"), ":", String.valueOf(rv)));
    } else if (looksLikeXHMM) {
      // * DQ  -- diploid quality
      // * NDQ -- non-diploid quality
      // * RD  -- mean normalized read depth over region
      // * PL  -- genotype likelihoods, for [diploid, deletion, duplication]
      final float dq = Float.parseFloat(genotype.getExtendedAttribute("DQ", "0.0").toString());
      final float ndq = Float.parseFloat(genotype.getExtendedAttribute("NDQ", "0.0").toString());
      final float rd = Float.parseFloat(genotype.getExtendedAttribute("RD", "0.0").toString());
      final int pl[] = genotype.getPL();

      // Attributes to write out.
      //
      // * dq  -- diploid quality
      // * ndq -- non-diploid quality
      // * rd  -- mean normalized read depth over region
      // * pl  -- genotype likelihoods, for [diploid, deletion, duplication]
      attrs.add(Joiner.on("").join(tripleQuote("dq"), ":", String.valueOf(dq)));
      attrs.add(Joiner.on("").join(tripleQuote("ndq"), ":", String.valueOf(ndq)));
      attrs.add(Joiner.on("").join(tripleQuote("rd"), ":", String.valueOf(rd)));
      attrs.add(
          Joiner.on("").join(tripleQuote("pl"), ":[", Joiner.on(',').join(Ints.asList(pl)), "]"));
    } else if (looksLikeGcnv) {
      // * CN  -- copy number
      // * NP  -- number of points in segment
      // * QA  -- phred-scaled quality of all points agreeing
      // * QS  -- phred-scaled quality of at least one point agreeing
      // * QSS -- phred-scaled quality of start breakpoint
      // * QSE -- phred-scaled quality of end breakpoint
      final int cn = Integer.parseInt(genotype.getExtendedAttribute("CN", "0").toString());
      final int np = Integer.parseInt(genotype.getExtendedAttribute("NP", "0").toString());
      final int qa = Integer.parseInt(genotype.getExtendedAttribute("QA", "0").toString());
      final int qs = Integer.parseInt(genotype.getExtendedAttribute("QS", "0").toString());
      final int qss = Integer.parseInt(genotype.getExtendedAttribute("QSS", "0").toString());
      final int qse = Integer.parseInt(genotype.getExtendedAttribute("QSE", "0").toString());

      // Attributes to write out.
      //
      // * cn  -- copy number
      // * np  -- number of points in segment
      // * qa  -- phred-scaled quality of all points agreeing
      // * qs  -- phred-scaled quality of at least one point agreeing
      // * qss -- phred-scaled quality of start breakpoint
      // * qse -- phred-scaled quality of end breakpoint
      attrs.add(Joiner.on("").join(tripleQuote("cn"), ":", String.valueOf(cn)));
      attrs.add(Joiner.on("").join(tripleQuote("np"), ":", String.valueOf(np)));
      attrs.add(Joiner.on("").join(tripleQuote("qa"), ":", String.valueOf(qa)));
      attrs.add(Joiner.on("").join(tripleQuote("qs"), ":", String.valueOf(qs)));
      attrs.add(Joiner.on("").join(tripleQuote("qss"), ":", String.valueOf(qss)));
      attrs.add(Joiner.on("").join(tripleQuote("qse"), ":", String.valueOf(qse)));
    }

    return attrs;
  }
}
