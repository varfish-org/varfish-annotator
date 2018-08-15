package com.github.bihealth.varhab.annotate;

import com.github.bihealth.varhab.VarhabException;
import com.github.bihealth.varhab.utils.DnaUtils;
import com.github.bihealth.varhab.utils.VariantDescription;
import com.github.bihealth.varhab.utils.VariantNormalizer;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import de.charite.compbio.jannovar.annotation.Annotation;
import de.charite.compbio.jannovar.annotation.VariantAnnotations;
import de.charite.compbio.jannovar.annotation.VariantEffect;
import de.charite.compbio.jannovar.data.JannovarData;
import de.charite.compbio.jannovar.data.JannovarDataSerializer;
import de.charite.compbio.jannovar.data.SerializationException;
import de.charite.compbio.jannovar.hgvs.AminoAcidCode;
import de.charite.compbio.jannovar.hgvs.nts.change.NucleotideChange;
import de.charite.compbio.jannovar.hgvs.nts.change.NucleotideDeletion;
import de.charite.compbio.jannovar.hgvs.nts.change.NucleotideDuplication;
import de.charite.compbio.jannovar.hgvs.nts.change.NucleotideIndel;
import de.charite.compbio.jannovar.hgvs.nts.change.NucleotideInsertion;
import de.charite.compbio.jannovar.hgvs.nts.change.NucleotideInversion;
import de.charite.compbio.jannovar.htsjdk.InvalidCoordinatesException;
import de.charite.compbio.jannovar.htsjdk.VariantContextAnnotator;
import de.charite.compbio.jannovar.htsjdk.VariantContextAnnotator.Options;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Implementation of the <tt>annotate</tt> command. */
public final class AnnotateVcf {

  /** Name of table with ExAC variants. */
  public static final String EXAC_TABLE_NAME = "exac_var";

  /** Header fields for the genotypes file. */
  public static final ImmutableList<String> HEADERS_GT =
      ImmutableList.of(
          "release",
          "chromosome",
          "position",
          "reference",
          "alternative",
          "case_id",
          "frequency",
          "homozygous",
          "effect",
          "genotype",
          "in_clinvar",
          "gene_id",
          "transcript_id",
          "transcript_coding",
          "hgvs_c",
          "hgvs_p",
          "before_change",
          "after_change",
          "inserted_bases");

  /** Header fields for the variant file. */
  public static final ImmutableList<String> HEADERS_VAR =
      ImmutableList.of(
          "release",
          "chromosome",
          "position",
          "reference",
          "alternative",
          "effect",
          "gene_id",
          "transcript_id",
          "transcript_coding",
          "hgvs_c",
          "hgvs_p");

  /** Configuration for the command. */
  private final AnnotateArgs args;

  /** Construct with the given configuration. */
  public AnnotateVcf(AnnotateArgs args) {
    this.args = args;
  }

  /** Execute the command. */
  public void run() {
    System.err.println("Running annotate; args: " + args);

    try (Connection conn =
            DriverManager.getConnection(
                "jdbc:h2:" + args.getDbPath() + ";MV_STORE=FALSE;MVCC=FALSE", "sa", "");
        VCFFileReader reader = new VCFFileReader(new File(args.getInputVcf()));
        FileWriter gtWriter = new FileWriter(new File(args.getOutputGts()));
        BufferedWriter gtBufWriter = new BufferedWriter(gtWriter);
        FileWriter varWriter = new FileWriter(new File(args.getOutputVars()));
        BufferedWriter varBufWriter = new BufferedWriter(varWriter)) {
      System.err.println("Deserializing Jannovar file...");
      JannovarData jannovarData = new JannovarDataSerializer(args.getSerPath()).load();
      final VariantNormalizer normalizer = new VariantNormalizer(args.getRefPath());
      annotateVcf(conn, reader, jannovarData, normalizer, gtWriter, varWriter);
    } catch (SQLException e) {
      System.err.println("Problem with database conection");
      e.printStackTrace();
      System.exit(1);
    } catch (VarhabException e) {
      System.err.println("Problem executing init-db");
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
   * Perform the variant annotation.
   *
   * @param conn Database connection for getting ExAC/ClinVar information from.
   * @param reader Reader for the input VCF file.
   * @param jannovarData Deserialized transcript database for Jannovar.
   * @param normalizer Helper for normalizing variants.
   * @param gtWriter Writer for variant call ("genotype") TSV file.
   * @param varWriter Writer for variant annotation ("annotation") TSV file.
   * @throws VarhabException in case of problems
   */
  private void annotateVcf(
      Connection conn,
      VCFFileReader reader,
      JannovarData jannovarData,
      VariantNormalizer normalizer,
      FileWriter gtWriter,
      FileWriter varWriter)
      throws VarhabException {

    // Write out header.
    try {
      gtWriter.append(Joiner.on("\t").join(HEADERS_GT) + "\n");
      varWriter.append(Joiner.on("\t").join(HEADERS_VAR) + "\n");
    } catch (IOException e) {
      throw new VarhabException("Could not write out headers", e);
    }

    final VariantContextAnnotator annotator =
        new VariantContextAnnotator(
            jannovarData.getRefDict(),
            jannovarData.getChromosomes(),
            new Options(false, AminoAcidCode.ONE_LETTER, false, false, false, false, false));

    String prevChr = null;
    for (VariantContext ctx : reader) {
      if (!ctx.getContig().equals(prevChr)) {
        System.err.println("Now on chrom " + ctx.getContig());
      }
      annotateVariantContext(conn, annotator, normalizer, ctx, gtWriter, varWriter);
      prevChr = ctx.getContig();
    }
  }

  /**
   * Annotate <tt>ctx</tt>, write out annotated variant call to <tt>gtWriter</tt> and annotated
   * variant to <tt>varWriter</tt>.
   *
   * @param conn Database connection.
   * @param annotator Helper class to use for annotation of variants.
   * @param normalizer Helper for normalizing variants.
   * @param ctx The variant to annotate.
   * @param gtWriter Writer for annotated genotypes.
   * @param varWriter Writer for variants.
   * @throws VarhabException in case of problems
   */
  private void annotateVariantContext(
      Connection conn,
      VariantContextAnnotator annotator,
      VariantNormalizer normalizer,
      VariantContext ctx,
      FileWriter gtWriter,
      FileWriter varWriter)
      throws VarhabException {
    ImmutableList<VariantAnnotations> annotationsList = null;
    try {
      annotationsList = annotator.buildAnnotations(ctx);
    } catch (InvalidCoordinatesException e) {
      // Swallow.
    }

    final int numAlleles = ctx.getAlleles().size();
    for (int i = 1; i < numAlleles; ++i) {
      // Collect gene IDs for which we already wrote an effect.
      final List<String> seenGeneIds = new ArrayList<>();
      // Get annotations sorted descendingly by variant effect.
      final List<Annotation> sortedAnnos;
      if (annotationsList == null) {
        sortedAnnos = new ArrayList<>();
      } else {
        sortedAnnos =
            annotationsList
                .get(i - 1)
                .getAnnotations()
                .stream()
                .sorted(
                    Comparator.<Annotation, VariantEffect>comparing(
                            Annotation::getMostPathogenicVarType,
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
                            })
                        .reversed())
                .collect(Collectors.toList());
      }

      // Write out unannotated record to output file in case of problems with annotation.
      //
      // TODO: report errors?
      if (sortedAnnos.isEmpty()) {
        try {
          varWriter.append(
              Joiner.on("\t")
                      .join(
                          args.getRelease(),
                          ctx.getContig(),
                          ctx.getStart(),
                          ctx.getReference(),
                          ctx.getAlternateAllele(i - 1).getBaseString(),
                          ".",
                          ".",
                          ".",
                          ".",
                          ".",
                          ".")
                  + "\n");
        } catch (IOException e) {
          throw new VarhabException("Problem writing to variant annotation file.", e);
        }
        continue; // short-circuit
      }

      for (Annotation annotation : sortedAnnos) {
        final String geneId = annotation.getTranscript().getGeneID();

        // Query information in ExAC.
        final ExacInfo exacInfo =
            getExacInfo(
                conn,
                args.getRelease(),
                ctx.getContig(),
                ctx.getStart(),
                annotation.getRef(),
                annotation.getAlt());

        // Normalize the from the annotation (will probably pad variant to the left).
        final VariantDescription normalizedVar =
            normalizer.normalizeInsertion(
                new VariantDescription(
                    annotation.getChrName(),
                    annotation.getPos(),
                    annotation.getRef(),
                    annotation.getAlt()));

        // Write to variant annotation file.
        //
        // Construct output record.
        final List<String> varOutRecord =
            Lists.newArrayList(
                args.getRelease(),
                normalizedVar.getChrom(),
                String.valueOf(normalizedVar.getPos() + 1),
                normalizedVar.getRef(),
                normalizedVar.getAlt(),
                (annotation == null) ? "." : buildEffectsValue(annotation.getEffects()),
                geneId,
                annotation.getTranscript().getAccession(),
                annotation.getTranscript().isCoding() ? "TRUE" : "FALSE",
                annotation.getCDSNTChange() == null
                    ? "."
                    : (annotation.getTranscript().isCoding() ? "c." : "n.")
                        + annotation.getCDSNTChange().toHGVSString(AminoAcidCode.ONE_LETTER),
                annotation.getProteinChange() == null
                    ? "."
                    : "p."
                        + annotation
                            .getProteinChange()
                            .withOnlyPredicted(false)
                            .toHGVSString(AminoAcidCode.ONE_LETTER));

        // Write record to output stream.
        try {
          varWriter.append(Joiner.on("\t").join(varOutRecord) + "\n");
        } catch (IOException e) {
          throw new VarhabException("Problem writing to variant annotation file.", e);
        }

        // Guard against writing out genotype call twice.
        if (seenGeneIds.contains(geneId)) {
          continue; // Already wrote out genotype annotation for gene.
        } else {
          seenGeneIds.add(geneId);
        }

        // Write to genotypes call file.
        //
        // Get detailed information about the location of possibly deleted bases.
        Integer beforeChange = null;
        Integer afterChange = null;
        String insertedBases = null;
        if (annotation.getCDSNTChange() != null) {
          final NucleotideChange change = annotation.getCDSNTChange();
          if (change instanceof NucleotideDeletion) {
            final NucleotideDeletion castChange = (NucleotideDeletion) change;
            beforeChange = castChange.getRange().getFirstPos().getBasePos();
            afterChange = castChange.getRange().getLastPos().getBasePos() + 2;
          } else if (change instanceof NucleotideInsertion) {
            final NucleotideInsertion castChange = (NucleotideInsertion) change;
            beforeChange = castChange.getRange().getFirstPos().getBasePos();
            afterChange = castChange.getRange().getLastPos().getBasePos() + 2;
            insertedBases = castChange.getSeq().getNucleotides();
          } else if (change instanceof NucleotideDuplication) {
            final NucleotideDuplication castChange = (NucleotideDuplication) change;
            beforeChange = castChange.getRange().getFirstPos().getBasePos();
            afterChange = castChange.getRange().getLastPos().getBasePos() + 2;
            insertedBases = castChange.getSeq().getNucleotides();
          } else if (change instanceof NucleotideIndel) {
            final NucleotideIndel castChange = (NucleotideIndel) change;
            beforeChange = castChange.getRange().getFirstPos().getBasePos();
            afterChange = castChange.getRange().getLastPos().getBasePos() + 2;
            insertedBases = castChange.getInsSeq().getNucleotides();
          } else if (change instanceof NucleotideInversion) {
            final NucleotideInversion castChange = (NucleotideInversion) change;
            beforeChange = castChange.getRange().getFirstPos().getBasePos();
            afterChange = castChange.getRange().getLastPos().getBasePos() + 2;
            insertedBases = DnaUtils.reverseComplement(castChange.getSeq().getNucleotides());
          }
        }

        // Construct output record.
        final String gene_name = annotation.getTranscript().getAltGeneIDs().get("HGNC_SYMBOL");
        final List<String> gtOutRec =
            Lists.newArrayList(
                args.getRelease(),
                normalizedVar.getChrom(),
                String.valueOf(normalizedVar.getPos() + 1),
                normalizedVar.getRef(),
                normalizedVar.getAlt(),
                args.getCaseId(),
                exacInfo.getAfPopmaxStr(),
                exacInfo.getHomTotalStr(),
                (annotation == null) ? "." : buildEffectsValue(annotation.getEffects()),
                buildGenotypeValue(ctx, i),
                getClinVarInfo(
                        conn,
                        args.getRelease(),
                        ctx.getContig(),
                        ctx.getStart(),
                        annotation.getRef(),
                        annotation.getAlt())
                    ? "TRUE"
                    : "FALSE",
                geneId,
                annotation.getTranscript().getAccession(),
                annotation.getTranscript().isCoding() ? "TRUE" : "FALSE",
                annotation.getCDSNTChange() == null
                    ? "."
                    : (annotation.getTranscript().isCoding() ? "c." : "n.")
                        + annotation.getCDSNTChange().toHGVSString(AminoAcidCode.ONE_LETTER),
                annotation.getProteinChange() == null
                    ? "."
                    : "p."
                        + annotation
                            .getProteinChange()
                            .withOnlyPredicted(false)
                            .toHGVSString(AminoAcidCode.ONE_LETTER),
                beforeChange == null ? "." : beforeChange.toString(),
                afterChange == null ? "." : afterChange.toString(),
                (insertedBases == null || insertedBases.isEmpty())
                    ? "."
                    : insertedBases.toString());

        // Write record to output stream.
        try {
          gtWriter.append(Joiner.on("\t").join(gtOutRec) + "\n");
        } catch (IOException e) {
          throw new VarhabException("Problem writing to genotypes call file.", e);
        }
      }
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
   * Build genotypes JSON expression for Postgres TSV file.
   *
   * @param ctx {@link VariantContext} from the input file.
   * @param alleleNo The allele number (first alternative is 1)
   * @return {@link String} with the genotype value.
   */
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
                  ",",
                  tripleQuote("ad"),
                  ":",
                  String.valueOf(ad == null ? 0 : ad[alleleNo]),
                  ",",
                  tripleQuote("dp"),
                  ":",
                  String.valueOf(ctx.getGenotype(sample).getDP()),
                  ",",
                  tripleQuote("gq"),
                  ":",
                  String.valueOf(ctx.getGenotype(sample).getGQ()),
                  "}"));
    }
    return "{" + Joiner.on(",").join(mappings) + "}";
  }

  /**
   * Build Postgres array expression for TSV file with variant effects.
   *
   * @param effects The effects to create expression for.
   * @return String with the variant effects.
   */
  private String buildEffectsValue(ImmutableSortedSet<VariantEffect> effects) {
    final List<String> effectStrings =
        effects
            .stream()
            .map(e -> "\"" + e.getSequenceOntologyTerm() + "\"")
            .collect(Collectors.toList());
    return Joiner.on("").join("{", Joiner.on(',').join(effectStrings), "}");
  }

  /**
   * Query ExAC for information about variant.
   *
   * @param conn Database connection to use for query.
   * @param release Genome release.
   * @param contig Name of contig to query.
   * @param start 1-based start position of variant.
   * @param ref Reference bases.
   * @param alt Alternative bases.
   * @return {@link ExacInfo} with information from ExAC.
   * @throw VarhabException in case of problems with obtaining information
   */
  private ExacInfo getExacInfo(
      Connection conn, String release, String contig, int start, String ref, String alt)
      throws VarhabException {
    final String query =
        "SELECT exac_af_popmax, exac_hom FROM "
            + EXAC_TABLE_NAME
            + " WHERE (release = ?) AND (chrom = ?) AND (pos = ?) AND (ref = ?) AND (alt = ?)";
    try {
      final PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, release);
      stmt.setString(2, contig);
      stmt.setInt(3, start);
      stmt.setString(4, ref);
      stmt.setString(5, alt);

      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          return ExacInfo.nullValue();
        }
        final ExacInfo result = new ExacInfo(rs.getDouble(1), rs.getInt(2));
        if (rs.next()) {
          throw new VarhabException("ExAC returned more than one result");
        }
        return result;
      }
    } catch (SQLException e) {
      throw new VarhabException("Problem with querying ExAC", e);
    }
  }

  /**
   * Query ClinVar for information about variant.
   *
   * @param conn Database connection to use for query.
   * @param release Genome release.
   * @param contig Name of contig to query.
   * @param start 1-based start position of variant.
   * @param ref Reference bases.
   * @param alt Alternative bases.
   * @return {@code bool} specifying whether variant is in ClinVar.
   * @throw VarhabException in case of problems with obtaining information
   */
  private boolean getClinVarInfo(
      Connection conn, String release, String contig, int start, String ref, String alt)
      throws VarhabException {
    final String query =
        "SELECT COUNT(*) FROM clinvar_var "
            + "WHERE (release = ?) AND (chrom = ?) AND (pos = ?) AND (ref = ?) AND (alt = ?)";
    try {
      final PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, release);
      stmt.setString(2, contig);
      stmt.setInt(3, start);
      stmt.setString(4, ref);
      stmt.setString(5, alt);

      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          throw new VarhabException("ClinVar counter query returned less than one result");
        }
        final boolean result = (rs.getInt(1) > 0);
        if (rs.next()) {
          throw new VarhabException("ClinVar counter query returned more than one result");
        }
        return result;
      }
    } catch (SQLException e) {
      throw new VarhabException("Problem with querying ClinVar", e);
    }
  }

  /** ExAC information for variant. */
  private static class ExacInfo {
    /** Allele frequency in population with maximal frequency. */
    private final Double afPopmax;
    /** Number of total homozygous state observation. */
    private final Integer homTotal;

    /** Construct with null values. */
    public static ExacInfo nullValue() {
      return new ExacInfo(null, null);
    }

    /** Constructor. */
    public ExacInfo(Double afPopmax, Integer homTotal) {
      this.afPopmax = afPopmax;
      this.homTotal = homTotal;
    }

    /**
     * @return String with allele frequency in population with maximal allele frequency or "." if
     *     null.
     */
    public String getAfPopmaxStr() {
      return afPopmax == null ? "." : afPopmax.toString();
    }

    /** @return String with total number of homozygous or "." if null. */
    public String getHomTotalStr() {
      return homTotal == null ? "." : homTotal.toString();
    }
  }
}
