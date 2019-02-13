package com.github.bihealth.varfish_annotator.annotate_svs;

import com.github.bihealth.varfish_annotator.VarfishAnnotatorException;
import com.github.bihealth.varfish_annotator.init_db.DbReleaseUpdater;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
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
          "start",
          "end",
          "start_ci_left",
          "start_ci_right",
          "end_ci_left",
          "end_ci_right",
          "case_id",
          "sv_uuid",
          "caller",
          "sv_type",
          "sv_sub_type",
          "info",
          "genotype");

  /** Header fields for the gene-wise feature effects file. */
  public static final ImmutableList<String> HEADERS_FEATURE_EFFECTS =
      ImmutableList.of(
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

  /** Execute the command. */
  public void run() {
    System.err.println("Running annotate-svs; args: " + args);

    String dbPath = args.getDbPath();
    if (dbPath.endsWith(".h2.db")) {
      dbPath = dbPath.substring(0, dbPath.length() - ".h2.db".length());
    }

    try (Connection conn =
            DriverManager.getConnection(
                "jdbc:h2:" + dbPath + ";MV_STORE=FALSE;MVCC=FALSE;ACCESS_MODE_DATA=r", "sa", "");
        VCFFileReader reader = new VCFFileReader(new File(args.getInputVcf()));
        FileWriter gtWriter = new FileWriter(new File(args.getOutputGts()));
        FileWriter featureEffectsWriter = new FileWriter(new File(args.getOutputFeatureEffects()));
        FileWriter dbInfoWriter = new FileWriter(new File(args.getOutputDbInfos()));
        BufferedWriter dbInfoBufWriter = new BufferedWriter(dbInfoWriter); ) {
      System.err.println("Deserializing Jannovar file...");
      JannovarData refseqJvData = new JannovarDataSerializer(args.getRefseqSerPath()).load();
      JannovarData ensemblJvData = new JannovarDataSerializer(args.getEnsemblSerPath()).load();
      annotateSvVcf(conn, reader, refseqJvData, ensemblJvData, gtWriter, featureEffectsWriter);
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
      dbInfoWriter.write("db_name\trelease\n");
    } catch (IOException e) {
      throw new VarfishAnnotatorException("Could not write out headers", e);
    }

    final String query =
        "SELECT db_name, release FROM " + DbReleaseUpdater.TABLE_NAME + " ORDER BY db_name";
    try {
      final PreparedStatement stmt = conn.prepareStatement(query);

      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          return;
        }
        dbInfoWriter.write(rs.getString(1) + "\t" + rs.getString(2) + "\n");
      }
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem with querying database", e);
    } catch (IOException e) {
      throw new VarfishAnnotatorException("Could nto write TSV info", e);
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
          conn, refseqAnnotator, ensemblAnnotator, ctx, gtWriter, featureEffectsWriter);
      prevChr = ctx.getContig();
    }
  }

  /**
   * Annotate <tt>ctx</tt>, write out annotated variant call to <tt>gtWriter</tt> and annotated
   * variant to <tt>varWriter</tt>.
   *
   * @param conn Database connection.
   * @param refseqAnnotator Helper class to use for annotation of variants with Refseq
   * @param ensemblAnnotator Helper class to use for annotation of variants with ENSEMBL
   * @param ctx The variant to annotate.
   * @param gtWriter Writer for annotated genotypes.
   * @param featureEffectsWriter Writer for gene-wise feature effects.
   * @throws VarfishAnnotatorException in case of problems
   */
  private void annotateVariantContext(
      Connection conn,
      VariantContextAnnotator refseqAnnotator,
      VariantContextAnnotator ensemblAnnotator,
      VariantContext ctx,
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
      final UUID variantId = UUID.randomUUID();

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
      final List<Object> gtOutRec = buildGtRecord(variantId, svGenomeVar, ctx, i);
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
      UUID variantId, SVGenomeVariant svGenomeVar, VariantContext ctx, int alleleNo) {
    final String svMethod = ctx.getCommonInfo().getAttributeAsString("SVMETHOD", null);
    return ImmutableList.of(
        args.getRelease(),
        svGenomeVar.getChrName(),
        svGenomeVar.getPos() + 1,
        svGenomeVar.getPos2() + 1,
        svGenomeVar.getPosCILowerBound(),
        svGenomeVar.getPosCIUpperBound(),
        svGenomeVar.getPos2CILowerBound(),
        svGenomeVar.getPos2CIUpperBound(),
        args.getCaseId(),
        variantId.toString(),
        svMethod,
        // TODO: improve type and sub type annotation!
        svGenomeVar.getType(),
        svGenomeVar.getType(),
        "{}",
        buildGenotypeValue(ctx, alleleNo));
  }

  private String buildGenotypeValue(VariantContext ctx, int alleleNo) {
    final List<String> mappings = new ArrayList<>();
    for (String sample : ctx.getSampleNames()) {
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

      final int[] ad = ctx.getGenotype(sample).getAD();
      mappings.add(
          Joiner.on("")
              .join(
                  tripleQuote(sample),
                  ":{",
                  tripleQuote("gt"),
                  ":",
                  tripleQuote(gts.get(sample)),
                  "}"));
    }

    return "{" + Joiner.on(",").join(mappings) + "}";
  }

  /** Build string list with the information for one feature effect record. */
  private List<Object> buildFeatureEffectRecord(
      SVGenomeVariant svGenomeVar,
      UUID variantId,
      SVAnnotation refSeqAnno,
      SVAnnotation ensemblAnno) {
    List<Object> result = Lists.newArrayList((Object) variantId.toString());
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
