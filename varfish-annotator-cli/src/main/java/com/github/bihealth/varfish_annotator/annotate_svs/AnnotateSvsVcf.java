package com.github.bihealth.varfish_annotator.annotate_svs;

import com.github.bihealth.varfish_annotator.VarfishAnnotatorException;
import com.github.bihealth.varfish_annotator.annotate.GenomeVersion;
import com.github.bihealth.varfish_annotator.annotate.IncompatibleVcfException;
import com.github.bihealth.varfish_annotator.annotate.VcfCompatibilityChecker;
import com.github.bihealth.varfish_annotator.init_db.DbReleaseUpdater;
import com.github.bihealth.varfish_annotator.utils.UcscBinning;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import de.charite.compbio.jannovar.annotation.SVAnnotation;
import de.charite.compbio.jannovar.annotation.SVAnnotations;
import de.charite.compbio.jannovar.annotation.VariantEffect;
import de.charite.compbio.jannovar.data.JannovarData;
import de.charite.compbio.jannovar.data.JannovarDataSerializer;
import de.charite.compbio.jannovar.data.SerializationException;
import de.charite.compbio.jannovar.hgvs.AminoAcidCode;
import de.charite.compbio.jannovar.htsjdk.InvalidBreakendDescriptionException;
import de.charite.compbio.jannovar.htsjdk.InvalidCoordinatesException;
import de.charite.compbio.jannovar.htsjdk.MissingEndInfoField;
import de.charite.compbio.jannovar.htsjdk.MissingSVTypeInfoField;
import de.charite.compbio.jannovar.htsjdk.MultipleSVAlleles;
import de.charite.compbio.jannovar.htsjdk.VariantContextAnnotator;
import de.charite.compbio.jannovar.htsjdk.VariantContextAnnotator.Options;
import de.charite.compbio.jannovar.reference.SVDescription.Type;
import de.charite.compbio.jannovar.reference.SVGenomeVariant;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

/** Implementation of the <tt>annotate-svs</tt> command. */
public final class AnnotateSvsVcf {

  /** Header fields for the SV and genotype file. */
  public static final ImmutableList<String> HEADERS_GT =
      ImmutableList.of(
          "release",
          "chromosome",
          "chromosome_no",
          "start",
          "end",
          "bin",
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
          "info",
          "genotype");

  /** Header fields for the gene-wise feature effects file. */
  public static final ImmutableList<String> HEADERS_FEATURE_EFFECTS =
      ImmutableList.of(
          "case_id",
          "set_id",
          "sv_uuid",
          "refseq_gene_id",
          "refseq_transcript_id",
          "refseq_transcript_coding",
          "refseq_effect",
          "ensembl_gene_id",
          "ensembl_transcript_id",
          "ensembl_transcript_coding",
          "ensembl_effect");

  /** Configuration for the command. */
  private final AnnotateSvsArgs args;

  /** Construct with the given configuration. */
  public AnnotateSvsVcf(AnnotateSvsArgs args) {
    this.args = args;
  }

  /** UUID counter for sequential UUID generation. */
  private long uuidCounter = 0;

  /** Generate next UUID. */
  private UUID nextUuid() {
    if (args.getSequentialUuids()) {
      final UUID result = new UUID(0, uuidCounter);
      uuidCounter += 1;
      return result;
    } else {
      return UUID.randomUUID();
    }
  }

  /** Execute the command. */
  public void run() {
    System.err.println("Running annotate-svs; args: " + args);
    if (!ImmutableList.of("GRCh37", "GRCh38").contains(args.getRelease())) {
      System.err.println("Invalid release: " + args.getRelease() + ", not one of GRCh37, GRCh38");
      System.exit(1);
    }

    String dbPath = args.getDbPath();
    if (dbPath.endsWith(".h2.db")) {
      dbPath = dbPath.substring(0, dbPath.length() - ".h2.db".length());
    }

    try (Connection conn =
            DriverManager.getConnection(
                "jdbc:h2:"
                    + dbPath
                    + ";MV_STORE=FALSE;MVCC=FALSE;ACCESS_MODE_DATA=r;"
                    + "DB_CLOSE_ON_EXIT=FALSE",
                "sa",
                "");
        VCFFileReader reader = new VCFFileReader(new File(args.getInputVcf()));
        FileWriter gtWriter = new FileWriter(new File(args.getOutputGts()));
        FileWriter featureEffectsWriter = new FileWriter(new File(args.getOutputFeatureEffects()));
        FileWriter dbInfoWriter = new FileWriter(new File(args.getOutputDbInfos()));
        BufferedWriter dbInfoBufWriter = new BufferedWriter(dbInfoWriter); ) {
      // Guess genome version.
      GenomeVersion genomeVersion = new VcfCompatibilityChecker(reader).guessGenomeVersion();

      new VcfCompatibilityChecker(reader).check(args.getRelease());
      System.err.println("Deserializing Jannovar file...");
      JannovarData refseqJvData = new JannovarDataSerializer(args.getRefseqSerPath()).load();
      JannovarData ensemblJvData = new JannovarDataSerializer(args.getEnsemblSerPath()).load();
      annotateSvVcf(
          conn, genomeVersion, reader, refseqJvData, ensemblJvData, gtWriter, featureEffectsWriter);
      writeDbInfos(conn, dbInfoBufWriter);
    } catch (SQLException e) {
      System.err.println("Problem with database connection");
      e.printStackTrace();
      System.exit(1);
    } catch (VarfishAnnotatorException e) {
      System.err.println("Problem executing annotate");
      e.printStackTrace();
      System.exit(1);
    } catch (SerializationException e) {
      System.err.println("Problem deserializing Jannovar database");
      e.printStackTrace();
      System.exit(1);
    } catch (IOException e) {
      System.err.println("Problem opening output files database");
      e.printStackTrace();
      System.exit(1);
    } catch (IncompatibleVcfException e) {
      System.err.println("Problem with VCF compatibility: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  /**
   * Write information about used databases to TSV file.
   *
   * @param conn Database connection to get the information from.
   * @param dbInfoWriter Writer for database information.
   * @throws VarfishAnnotatorException in case of problems
   */
  private void writeDbInfos(Connection conn, BufferedWriter dbInfoWriter)
      throws VarfishAnnotatorException {
    try {
      dbInfoWriter.write("genomebuild\tdb_name\trelease\n");
    } catch (IOException e) {
      throw new VarfishAnnotatorException("Could not write out headers", e);
    }

    final String query =
        "SELECT db_name, release FROM " + DbReleaseUpdater.TABLE_NAME + " ORDER BY db_name";
    try {
      final PreparedStatement stmt = conn.prepareStatement(query);

      try (ResultSet rs = stmt.executeQuery()) {
        while (true) {
          if (!rs.next()) {
            return;
          }
          final String versionString;
          if (rs.getString(1).equals("varfish-annotator")) {
            versionString = AnnotateSvsVcf.class.getPackage().getSpecificationVersion();
          } else {
            versionString = rs.getString(2);
          }
          dbInfoWriter.write(
              args.getRelease() + "\t" + rs.getString(1) + "\t" + versionString + "\n");
        }
      }
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem with querying database", e);
    } catch (IOException e) {
      throw new VarfishAnnotatorException("Could not write TSV info", e);
    }
  }

  /**
   * Perform the variant annotation.
   *
   * @param conn Database connection for getting ExAC/ClinVar information from.
   * @param reader Reader for the input VCF file.
   * @param refseqJv Deserialized RefSeq transcript database for Jannovar.
   * @param ensemblJv Deserialized ENSEMBL transcript database for Jannovar.
   * @param gtWriter Writer for variant call ("genotype") TSV file.
   * @param featureEffectsWriter Writer for gene-wise feature effects.
   * @throws VarfishAnnotatorException in case of problems
   */
  private void annotateSvVcf(
      Connection conn,
      GenomeVersion genomeVersion,
      VCFFileReader reader,
      JannovarData refseqJv,
      JannovarData ensemblJv,
      FileWriter gtWriter,
      FileWriter featureEffectsWriter)
      throws VarfishAnnotatorException {

    // Write out header.
    try {
      gtWriter.append(Joiner.on("\t").join(HEADERS_GT) + "\n");
      featureEffectsWriter.append(Joiner.on("\t").join(HEADERS_FEATURE_EFFECTS) + "\n");
    } catch (IOException e) {
      throw new VarfishAnnotatorException("Could not write out headers", e);
    }

    final VariantContextAnnotator refseqAnnotator =
        new VariantContextAnnotator(
            refseqJv.getRefDict(),
            refseqJv.getChromosomes(),
            new Options(false, AminoAcidCode.ONE_LETTER, false, false, false, false, false));
    final VariantContextAnnotator ensemblAnnotator =
        new VariantContextAnnotator(
            ensemblJv.getRefDict(),
            ensemblJv.getChromosomes(),
            new Options(false, AminoAcidCode.ONE_LETTER, false, false, false, false, false));

    // Collect names of skipped contigs.
    Set<String> skippedContigs = new HashSet<>();

    String prevChr = null;
    for (VariantContext ctx : reader) {
      if (ctx.getFilters().contains("LowQual")) {
        // TODO: make this configurable
        continue; // skip low-quality SVs
      }

      // Check whether contigs should be skipped.
      if (skippedContigs.contains(ctx.getContig())) {
        continue; // skip silently
      } else if (!ctx.getContig().matches(args.getContigRegex())) {
        System.err.println("Skipping contig " + ctx.getContig());
        skippedContigs.add(ctx.getContig());
        continue;
      }

      if (!ctx.getContig().equals(prevChr)) {
        System.err.println("Now on contig " + ctx.getContig());
      }
      annotateVariantContext(
          refseqAnnotator, ensemblAnnotator, ctx, genomeVersion, gtWriter, featureEffectsWriter);
      prevChr = ctx.getContig();
    }
  }

  /**
   * Annotate <tt>ctx</tt>, write out annotated variant call to <tt>gtWriter</tt> and annotated
   * variant to <tt>varWriter</tt>.
   *
   * @param refseqAnnotator Helper class to use for annotation of variants with Refseq
   * @param ensemblAnnotator Helper class to use for annotation of variants with ENSEMBL
   * @param ctx The variant to annotate.
   * @param genomeVersion The genome version that {@code ctx} uses.
   * @param gtWriter Writer for annotated genotypes.
   * @param featureEffectsWriter Writer for gene-wise feature effects.
   * @throws VarfishAnnotatorException in case of problems
   */
  private void annotateVariantContext(
      VariantContextAnnotator refseqAnnotator,
      VariantContextAnnotator ensemblAnnotator,
      VariantContext ctx,
      GenomeVersion genomeVersion,
      FileWriter gtWriter,
      FileWriter featureEffectsWriter)
      throws VarfishAnnotatorException {
    ImmutableList<SVAnnotations> refseqAnnotationsList =
        silentBuildAnnotations(ctx, refseqAnnotator);
    ImmutableList<SVAnnotations> ensemblAnnotationsList =
        silentBuildAnnotations(ctx, ensemblAnnotator);

    final int numAlleles = ctx.getAlleles().size();
    for (int i = 1; i < numAlleles; ++i) {
      // Create UUID for the variant.
      final UUID variantId = nextUuid();

      // Get annotations sorted descendingly by variant effect.
      final List<SVAnnotation> sortedRefseqAnnos = sortAnnos(refseqAnnotationsList, i);
      final List<SVAnnotation> sortedEnsemblAnnos = sortAnnos(ensemblAnnotationsList, i);

      // Build `SVGenomeVariant` from `ctx` regardless of any annotation.
      final SVGenomeVariant svGenomeVar;
      try {
        svGenomeVar = refseqAnnotator.buildSVGenomeVariant(ctx);
      } catch (MissingSVTypeInfoField
          | InvalidCoordinatesException
          | MissingEndInfoField
          | InvalidBreakendDescriptionException e) {
        throw new VarfishAnnotatorException("Problem creating SV genome annotation", e);
      }

      // Write out record with the genotype.
      final List<Object> gtOutRec = buildGtRecord(variantId, svGenomeVar, ctx, genomeVersion, i);
      try {
        gtWriter.append(Joiner.on("\t").useForNull(".").join(gtOutRec) + "\n");
      } catch (IOException e) {
        throw new VarfishAnnotatorException("Problem writing to genotypes call file.", e);
      }

      // Short-circuit here in case we don't have any feature annotation in either RefSeq and
      // ENSEMBL list.
      if (sortedRefseqAnnos.isEmpty() && sortedEnsemblAnnos.isEmpty()) {
        continue; // short-circuit
      }

      // Collecting RefSeq and ENSEMBL annotations per gene.  We collect the variants by gene for
      // RefSeq in the simplest way possible (mapping to ENSEMBL gene by using HGNC annotation from
      // Jannovar). Further, if either only yields one gene we force it to be the same as the
      // (lexicographically first) gene of the other.
      //
      // Intergenic variants are skipped.

      // Start out with the RefSeq annotations
      final HashMap<String, SVAnnotation> refseqAnnoByGene = new HashMap<>();
      for (SVAnnotation annotation : sortedRefseqAnnos) {
        if (annotation.getTranscript() == null) {
          continue; // skip, no transcript
        }
        String geneId = annotation.getTranscript().getAltGeneIDs().get("ENSEMBL_GENE_ID");
        if (geneId == null) {
          geneId = annotation.getTranscript().getGeneID();
        }
        if (!annotation.getEffects().contains(VariantEffect.INTERGENIC_VARIANT)
            && !refseqAnnoByGene.containsKey(geneId)) {
          refseqAnnoByGene.put(geneId, annotation);
        }
      }

      // Now, for the ENSEMBL annotations
      final HashMap<String, SVAnnotation> ensemblAnnoByGene = new HashMap<>();
      for (SVAnnotation annotation : sortedEnsemblAnnos) {
        if (annotation.getTranscript() == null) {
          continue; // skip, no transcript
        }
        final String geneId = annotation.getTranscript().getGeneID();
        if (!annotation.getEffects().contains(VariantEffect.INTERGENIC_VARIANT)
            && !ensemblAnnoByGene.containsKey(geneId)) {
          ensemblAnnoByGene.put(geneId, annotation);
        }
      }

      // Match RefSeq and ENSEMBL annotations
      final TreeSet<String> geneIds = new TreeSet<>();
      if (refseqAnnoByGene.size() == 1 && ensemblAnnoByGene.size() > 0) {
        geneIds.addAll(ensemblAnnoByGene.keySet());
        final String keyEnsembl = ensemblAnnoByGene.keySet().iterator().next();
        final String keyRefseq = refseqAnnoByGene.keySet().iterator().next();
        refseqAnnoByGene.put(keyEnsembl, refseqAnnoByGene.get(keyRefseq));
      } else if (refseqAnnoByGene.size() > 0 && ensemblAnnoByGene.size() == 1) {
        geneIds.addAll(refseqAnnoByGene.keySet());
        final String keyEnsembl = ensemblAnnoByGene.keySet().iterator().next();
        final String keyRefseq = refseqAnnoByGene.keySet().iterator().next();
        ensemblAnnoByGene.put(keyRefseq, ensemblAnnoByGene.get(keyEnsembl));
      } else {
        geneIds.addAll(ensemblAnnoByGene.keySet());
        geneIds.addAll(refseqAnnoByGene.keySet());
      }

      // Write one entry for each gene into the feature effect call file.
      for (String geneId : geneIds) {
        List<Object> featureEffectOutRec =
            buildFeatureEffectRecord(
                svGenomeVar,
                variantId,
                refseqAnnoByGene.get(geneId),
                ensemblAnnoByGene.get(geneId));
        try {
          featureEffectsWriter.append(
              Joiner.on("\t").useForNull(".").join(featureEffectOutRec) + "\n");
        } catch (IOException e) {
          throw new VarfishAnnotatorException("Problem writing to feature effects file.", e);
        }
      }
    }
  }

  /** Buidl string list with the information for the genotype call record. */
  private List<Object> buildGtRecord(
      UUID variantId,
      SVGenomeVariant svGenomeVar,
      VariantContext ctx,
      GenomeVersion genomeVersion,
      int alleleNo) {
    String svMethod = ctx.getCommonInfo().getAttributeAsString("SVMETHOD", null);
    if (svMethod == null) {
      svMethod = args.getDefaultSvMethod() == null ? "." : args.getDefaultSvMethod();
    }
    final boolean isBnd = (svGenomeVar.getType() == Type.BND);
    final int pos2 = isBnd ? svGenomeVar.getPos() + 1 : svGenomeVar.getPos2();

    final String contigName =
        (genomeVersion == GenomeVersion.HG19)
            ? svGenomeVar.getChrName().replaceFirst("chr", "")
            : svGenomeVar.getChrName();

    return ImmutableList.of(
        args.getRelease(),
        contigName,
        svGenomeVar.getChr(),
        svGenomeVar.getPos() + 1,
        pos2,
        UcscBinning.getContainingBin(svGenomeVar.getPos(), pos2),
        svGenomeVar.getPosCILowerBound(),
        svGenomeVar.getPosCIUpperBound(),
        svGenomeVar.getPos2CILowerBound(),
        svGenomeVar.getPos2CIUpperBound(),
        args.getCaseId(),
        args.getSetId(),
        variantId.toString(),
        svMethod,
        // TODO: improve type and sub type annotation!
        svGenomeVar.getType(),
        svGenomeVar.getType(),
        buildInfoValue(ctx, genomeVersion, svGenomeVar),
        buildGenotypeValue(ctx, alleleNo));
  }

  private String buildInfoValue(
      VariantContext ctx, GenomeVersion genomeVersion, SVGenomeVariant svGenomeVar) {
    final List<String> mappings = new ArrayList<>();

    if (svGenomeVar.getType() == Type.BND) {
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

    return "{" + Joiner.on(",").join(mappings) + "}";
  }

  private String buildGenotypeValue(VariantContext ctx, int alleleNo) {
    final ArrayList<String> attrs = new ArrayList<>();

    // Add "GT" field.
    final List<String> mappings = new ArrayList<>();
    final Comparator<String> c = Comparator.comparing(String::toString);
    final List<String> sortedSampleNames = Lists.newArrayList(ctx.getSampleNames());
    sortedSampleNames.sort(c);
    for (String sample : sortedSampleNames) {
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

      // Additional integer attributes, currently Delly and SV2 only.
      boolean looksLikeDelly = ctx.getAttributeAsString("SVMETHOD", "").contains("DELLY");
      boolean looksLikeSV2 = ctx.getAttributeAsString("SVMETHOD", "").contains("SV2");
      boolean looksLikeXHMM = ctx.getAttributeAsString("SVMETHOD", "").contains("XHMM");
      boolean looksLikeGcnv = ctx.getAttributeAsString("SVMETHOD", "").contains("gcnvkernel");
      boolean looksLikeCnvettiHomDel =
          ctx.getAttributeAsString("SVMETHOD", "").contains("cnvetti-homdel");
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
      } else if (looksLikeSV2) {
        // * CN -- copy number estimate
        // * PE -- normalized discordant paired-end count
        // * SR -- normalized split read count
        // * NS -- number of SNPs in the locus
        // * HA -- heterozygous allele ratio
        // * SQ -- phred-scaled genotype likelihood
        final float cn;
        final float pe;
        final float sr;
        final int ns;
        final float ha;
        final float sq;
        if ("nan".equals(genotype.getExtendedAttribute("CN", "0.0"))) {
          cn = 0;
        } else {
          cn = Float.parseFloat(genotype.getExtendedAttribute("CN", "0.0").toString());
        }
        if ("nan".equals(genotype.getExtendedAttribute("PE", "0.0"))) {
          pe = 0;
        } else {
          pe = Float.parseFloat(genotype.getExtendedAttribute("PE", "0.0").toString());
        }
        if ("nan".equals(genotype.getExtendedAttribute("SR", "0.0"))) {
          sr = 0;
        } else {
          sr = Float.parseFloat(genotype.getExtendedAttribute("SR", "0.0").toString());
        }
        if ("nan".equals(genotype.getExtendedAttribute("NS", "0.0"))) {
          ns = 0;
        } else {
          ns = Integer.parseInt(genotype.getExtendedAttribute("NS", "0").toString());
        }
        if ("nan".equals(genotype.getExtendedAttribute("HA", "0.0"))) {
          ha = 0;
        } else {
          ha = Float.parseFloat(genotype.getExtendedAttribute("HA", "0.0").toString());
        }
        if ("nan".equals(genotype.getExtendedAttribute("SQ", "0.0"))) {
          sq = 0;
        } else {
          sq = Float.parseFloat(genotype.getExtendedAttribute("SQ", "0.0").toString());
        }

        // Attributes to write out.
        //
        // * cn  - copy number estimate
        // * npe - normalized discordant paired-read count
        // * sre - normalized split read count
        // * ns  - number of NSPs in the locus
        // * har - heterozygous allele ratio
        // * gq  - phred-scaled genotype quality
        attrs.add(Joiner.on("").join(tripleQuote("cn"), ":", String.valueOf(cn)));
        attrs.add(Joiner.on("").join(tripleQuote("npe"), ":", String.valueOf(pe)));
        attrs.add(Joiner.on("").join(tripleQuote("sre"), ":", String.valueOf(sr)));
        attrs.add(Joiner.on("").join(tripleQuote("ns"), ":", String.valueOf(ns)));
        attrs.add(Joiner.on("").join(tripleQuote("har"), ":", String.valueOf(ha)));
        attrs.add(Joiner.on("").join(tripleQuote("gq"), ":", String.valueOf(Math.round(sq))));
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

      mappings.add(Joiner.on("").join(tripleQuote(sample), ":{", Joiner.on(",").join(attrs), "}"));
    }

    return "{" + Joiner.on(",").join(mappings) + "}";
  }

  /** Build string list with the information for one feature effect record. */
  private List<Object> buildFeatureEffectRecord(
      SVGenomeVariant svGenomeVar,
      UUID variantId,
      SVAnnotation refSeqAnno,
      SVAnnotation ensemblAnno) {
    List<Object> result =
        Lists.newArrayList(args.getCaseId(), args.getSetId(), variantId.toString());
    if (refSeqAnno == null) {
      result.addAll(Arrays.asList(null, null, "FALSE", "{}"));
    } else {
      result.addAll(
          Arrays.asList(
              refSeqAnno.getTranscript().getGeneID(),
              refSeqAnno.getTranscript().getAccession(),
              refSeqAnno.getTranscript().isCoding() ? "TRUE" : "FALSE",
              (refSeqAnno.getEffects() == null)
                  ? "{}"
                  : buildEffectsValue(refSeqAnno.getEffects())));
    }
    if (ensemblAnno == null) {
      result.addAll(Arrays.asList(null, null, "FALSE", "{}"));
    } else {
      result.addAll(
          Arrays.asList(
              ensemblAnno.getTranscript().getGeneID(),
              ensemblAnno.getTranscript().getAccession(),
              ensemblAnno.getTranscript().isCoding() ? "TRUE" : "FALSE",
              (ensemblAnno.getEffects() == null)
                  ? "{}"
                  : buildEffectsValue(ensemblAnno.getEffects())));
    }
    return result;
  }

  private ImmutableList<SVAnnotations> silentBuildAnnotations(
      VariantContext ctx, VariantContextAnnotator annotator) {
    try {
      return annotator.buildSVAnnotations(ctx);
    } catch (InvalidCoordinatesException
        | MultipleSVAlleles
        | MissingSVTypeInfoField
        | MissingEndInfoField
        | InvalidBreakendDescriptionException e) {
      return null;
    }
  }

  private List<SVAnnotation> sortAnnos(ImmutableList<SVAnnotations> refseqAnnotationsList, int i) {
    if (refseqAnnotationsList == null) {
      return new ArrayList<>();
    } else {
      return refseqAnnotationsList
          .get(i - 1)
          .getAnnotations()
          .stream()
          .sorted(
              Comparator.comparing(
                  SVAnnotation::getMostPathogenicVariantEffect,
                  (t1, t2) -> {
                    if (t1 == null && t2 == null) {
                      return 0;
                    } else if (t2 == null) {
                      return -1;
                    } else if (t1 == null) {
                      return 1;
                    } else {
                      return t1.compareTo(t2);
                    }
                  }))
          .collect(Collectors.toList());
    }
  }

  /**
   * (Overly) simple helper for escaping {@code s}.
   *
   * @param s String to escape.
   * @return Escaped version of {@code s}.
   */
  private static String tripleQuote(String s) {
    return "\"\"\"" + s.replaceAll("\"\"\"", "") + "\"\"\"";
  }

  /**
   * Build Postgres array expression for TSV file with variant effects.
   *
   * @param effects The effects to create expression for.
   * @return String with the variant effects.
   */
  private String buildEffectsValue(ImmutableSet<VariantEffect> effects) {
    final List<String> effectStrings =
        effects
            .stream()
            .map(e -> "\"" + e.getSequenceOntologyTerm() + "\"")
            .collect(Collectors.toList());
    return Joiner.on("").join("{", Joiner.on(',').join(effectStrings), "}");
  }
}
