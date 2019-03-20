package com.github.bihealth.varfish_annotator.annotate;

import com.github.bihealth.varfish_annotator.DbInfo;
import com.github.bihealth.varfish_annotator.VarfishAnnotatorException;
import com.github.bihealth.varfish_annotator.init_db.DbReleaseUpdater;
import com.github.bihealth.varfish_annotator.utils.VariantDescription;
import com.github.bihealth.varfish_annotator.utils.VariantNormalizer;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import de.charite.compbio.jannovar.annotation.Annotation;
import de.charite.compbio.jannovar.annotation.VariantAnnotations;
import de.charite.compbio.jannovar.annotation.VariantEffect;
import de.charite.compbio.jannovar.data.JannovarData;
import de.charite.compbio.jannovar.data.JannovarDataSerializer;
import de.charite.compbio.jannovar.data.SerializationException;
import de.charite.compbio.jannovar.hgvs.AminoAcidCode;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** Implementation of the <tt>annotate</tt> command. */
public final class AnnotateVcf {

  /** Name of table with ExAC variants. */
  public static final String EXAC_PREFIX = "exac";

  /** Name of table with gnomAD exomes variants. */
  public static final String GNOMAD_EXOMES_PREFIX = "gnomad_exome";

  /** Name of table with gnomAD genomes variants. */
  public static final String GNOMAD_GENOMES_PREFIX = "gnomad_genome";

  /** Name of table with Thousand Genomes variants. */
  public static final String THOUSAND_GENOMES_PREFIX = "thousand_genomes";

  /** Header fields for the genotypes file. */
  public static final ImmutableList<String> HEADERS_GT =
      ImmutableList.of(
          "release",
          "chromosome",
          "position",
          "reference",
          "alternative",
          "var_type",
          "case_id",
          "genotype",
          "in_clinvar",
          "exac_frequency",
          "exac_homozygous",
          "exac_heterozygous",
          "exac_hemizygous",
          "thousand_genomes_frequency",
          "thousand_genomes_homozygous",
          "thousand_genomes_heterozygous",
          "thousand_genomes_hemizygous",
          "gnomad_exomes_frequency",
          "gnomad_exomes_homozygous",
          "gnomad_exomes_heterozygous",
          "gnomad_exomes_hemizygous",
          "gnomad_genomes_frequency",
          "gnomad_genomes_homozygous",
          "gnomad_genomes_heterozygous",
          "gnomad_genomes_hemizygous",
          "refseq_gene_id",
          "refseq_transcript_id",
          "refseq_transcript_coding",
          "refseq_hgvs_c",
          "refseq_hgvs_p",
          "refseq_effect",
          "ensembl_gene_id",
          "ensembl_transcript_id",
          "ensembl_transcript_coding",
          "ensembl_hgvs_c",
          "ensembl_hgvs_p",
          "ensembl_effect");

  /** Header fields for the variant file. */
  public static final ImmutableList<String> HEADERS_VAR =
      ImmutableList.of(
          "release",
          "chromosome",
          "position",
          "reference",
          "alternative",
          "database",
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

    String dbPath = args.getDbPath();
    if (dbPath.endsWith(".h2.db")) {
      dbPath = dbPath.substring(0, dbPath.length() - ".h2.db".length());
    }

    try (Connection conn =
            DriverManager.getConnection(
                "jdbc:h2:" + dbPath + ";MV_STORE=FALSE;MVCC=FALSE;ACCESS_MODE_DATA=r", "sa", "");
        VCFFileReader reader = new VCFFileReader(new File(args.getInputVcf()));
        FileWriter gtWriter = new FileWriter(new File(args.getOutputGts()));
        FileWriter varWriter = new FileWriter(new File(args.getOutputVars()));
        FileWriter dbInfoWriter = new FileWriter(new File(args.getOutputDbInfos()));
        BufferedWriter dbInfoBufWriter = new BufferedWriter(dbInfoWriter); ) {
      System.err.println("Deserializing Jannovar file...");
      JannovarData refseqJvData = new JannovarDataSerializer(args.getRefseqSerPath()).load();
      JannovarData ensemblJvData = new JannovarDataSerializer(args.getEnsemblSerPath()).load();
      final VariantNormalizer normalizer = new VariantNormalizer(args.getRefPath());
      annotateVcf(conn, reader, refseqJvData, ensemblJvData, normalizer, gtWriter, varWriter);
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
      dbInfoWriter.write("genomebuild\tdb_name\trelease\n");
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
        dbInfoWriter.write(
            args.getRelease() + "\t" + rs.getString(1) + "\t" + rs.getString(2) + "\n");
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
   * @param normalizer Helper for normalizing variants.
   * @param gtWriter Writer for variant call ("genotype") TSV file.
   * @param varWriter Writer for variant annotation ("annotation") TSV file.
   * @throws VarfishAnnotatorException in case of problems
   */
  private void annotateVcf(
      Connection conn,
      VCFFileReader reader,
      JannovarData refseqJv,
      JannovarData ensemblJv,
      VariantNormalizer normalizer,
      FileWriter gtWriter,
      FileWriter varWriter)
      throws VarfishAnnotatorException {

    // Write out header.
    try {
      gtWriter.append(Joiner.on("\t").join(HEADERS_GT) + "\n");
      varWriter.append(Joiner.on("\t").join(HEADERS_VAR) + "\n");
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
          conn, refseqAnnotator, ensemblAnnotator, normalizer, ctx, gtWriter, varWriter);
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
   * @param normalizer Helper for normalizing variants.
   * @param ctx The variant to annotate.
   * @param gtWriter Writer for annotated genotypes.
   * @param varWriter Writer for variants.
   * @throws VarfishAnnotatorException in case of problems
   */
  private void annotateVariantContext(
      Connection conn,
      VariantContextAnnotator refseqAnnotator,
      VariantContextAnnotator ensemblAnnotator,
      VariantNormalizer normalizer,
      VariantContext ctx,
      FileWriter gtWriter,
      FileWriter varWriter)
      throws VarfishAnnotatorException {
    ImmutableList<VariantAnnotations> refseqAnnotationsList =
        silentBuildAnnotations(ctx, refseqAnnotator);
    ImmutableList<VariantAnnotations> ensemblAnnotationsList =
        silentBuildAnnotations(ctx, ensemblAnnotator);

    final int numAlleles = ctx.getAlleles().size();
    for (int i = 1; i < numAlleles; ++i) {
      // Normalize the from the VCF (will probably pad variant to the left).
      final VariantDescription normalizedVar =
          normalizer.normalizeInsertion(
              new VariantDescription(
                  ctx.getContig(),
                  ctx.getStart() - 1,
                  ctx.getReference().getBaseString(),
                  ctx.getAlternateAllele(i - 1).getBaseString()));

      // Get annotations sorted descendingly by variant effect.
      final List<Annotation> sortedRefseqAnnos = sortAnnos(refseqAnnotationsList, i);
      final List<Annotation> sortedEnsemblAnnos = sortAnnos(ensemblAnnotationsList, i);

      // Write out unannotated record to output file in case of problems with annotation.
      //
      // TODO: report errors?
      if (sortedRefseqAnnos.isEmpty() && sortedEnsemblAnnos.isEmpty()) {
        writeEmptyAnnoLine(normalizedVar, varWriter, i);
        continue; // short-circuit
      }

      // Write out variant annotation, collecting RefSeq and ENSEMBL annotations per gene.  We
      // collect the variants by gene for RefSeq in the simplest way possible (mapping to
      // ENSEMBL gene by using HGNC annotation from Jannovar).  However, we ignore intergenic
      // annotations here as these are mostly on different genes anyway (because ENSEMBL has
      // so many more genes/transcripts).  Further, if either only yields one gene we force it
      // to be the same as the (lexicographically first) gene of the other.
      final HashMap<String, Annotation> refseqAnnoByGene = new HashMap<>();
      for (Annotation annotation : sortedRefseqAnnos) {
        writeVariantAnnotation(varWriter, annotation, normalizedVar, "refseq");
        if (annotation.getTranscript() == null) {
          continue; // skip, no transcript
        }
        String geneId = annotation.getTranscript().getAltGeneIDs().get("ENSEMBL_GENE_ID");
        if (geneId == null) {
          geneId = annotation.getTranscript().getGeneID();
        }
        if (!annotation.getEffects().equals(ImmutableSet.of(VariantEffect.INTERGENIC_VARIANT))
            && !refseqAnnoByGene.containsKey(geneId)) {
          refseqAnnoByGene.put(geneId, annotation);
        }
      }
      final HashMap<String, Annotation> ensemblAnnoByGene = new HashMap<>();
      for (Annotation annotation : sortedEnsemblAnnos) {
        writeVariantAnnotation(varWriter, annotation, normalizedVar, "ensembl");
        if (annotation.getTranscript() == null) {
          continue; // skip, no transcript
        }
        final String geneId = annotation.getTranscript().getGeneID();
        if (!annotation.getEffects().equals(ImmutableSet.of(VariantEffect.INTERGENIC_VARIANT))
            && !ensemblAnnoByGene.containsKey(geneId)) {
          ensemblAnnoByGene.put(geneId, annotation);
        }
      }
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

      // Query information in databases.
      final DbInfo exacInfo = getDbInfo(conn, args.getRelease(), normalizedVar, EXAC_PREFIX);
      final DbInfo gnomadExomesInfo =
          getDbInfo(conn, args.getRelease(), normalizedVar, GNOMAD_EXOMES_PREFIX);
      final DbInfo gnomadGenomesInfo =
          getDbInfo(conn, args.getRelease(), normalizedVar, GNOMAD_GENOMES_PREFIX);
      final DbInfo thousandGenomesInfo =
          getDbInfo(conn, args.getRelease(), normalizedVar, THOUSAND_GENOMES_PREFIX);
      final boolean inClinvar = getClinVarInfo(conn, args.getRelease(), normalizedVar);

      // Write one entry for each gene into the annotated genotype call file.
      for (String geneId : geneIds) {
        Annotation refseqAnno = refseqAnnoByGene.get(geneId);
        Annotation ensemblAnno = ensemblAnnoByGene.get(geneId);
        Annotation annotation = refseqAnno != null ? refseqAnno : ensemblAnno;

        final String varType;
        if ((normalizedVar.getRef().length() == 1) && (normalizedVar.getAlt().length() == 1)) {
          varType = "snv";
        } else if (normalizedVar.getRef().length() == normalizedVar.getAlt().length()) {
          varType = "mnv";
        } else {
          varType = "indel";
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
                varType,
                args.getCaseId(),
                buildGenotypeValue(ctx, i),
                // ClinVar
                inClinvar ? "TRUE" : "FALSE",
                // EXAC
                exacInfo.getAfPopmaxStr(),
                exacInfo.getHomTotalStr(),
                exacInfo.getHetTotalStr(),
                exacInfo.getHemiTotalStr(),
                // Thousand Genomes
                thousandGenomesInfo.getAfPopmaxStr(),
                thousandGenomesInfo.getHomTotalStr(),
                thousandGenomesInfo.getHetTotalStr(),
                thousandGenomesInfo.getHemiTotalStr(),
                // gnomAD exomes
                gnomadExomesInfo.getAfPopmaxStr(),
                gnomadExomesInfo.getHomTotalStr(),
                gnomadExomesInfo.getHetTotalStr(),
                gnomadExomesInfo.getHemiTotalStr(),
                // gnomAD genomes
                gnomadGenomesInfo.getAfPopmaxStr(),
                gnomadGenomesInfo.getHomTotalStr(),
                gnomadGenomesInfo.getHetTotalStr(),
                gnomadGenomesInfo.getHemiTotalStr(),
                // RefSeq
                refseqAnno == null ? "." : refseqAnno.getTranscript().getGeneID(),
                refseqAnno == null ? "." : refseqAnno.getTranscript().getAccession(),
                refseqAnno == null ? "." : refseqAnno.getTranscript().isCoding() ? "TRUE" : "FALSE",
                refseqAnno == null
                    ? "."
                    : refseqAnno.getCDSNTChange() == null
                        ? "."
                        : (refseqAnno.getTranscript().isCoding() ? "c." : "n.")
                            + refseqAnno.getCDSNTChange().toHGVSString(AminoAcidCode.ONE_LETTER),
                refseqAnno == null
                    ? "."
                    : refseqAnno.getProteinChange() == null
                        ? "."
                        : "p."
                            + refseqAnno
                                .getProteinChange()
                                .withOnlyPredicted(false)
                                .toHGVSString(AminoAcidCode.ONE_LETTER),
                (refseqAnno == null) ? "{}" : buildEffectsValue(refseqAnno.getEffects()),
                // ENSEMBL
                ensemblAnno == null ? "." : ensemblAnno.getTranscript().getGeneID(),
                ensemblAnno == null ? "." : ensemblAnno.getTranscript().getAccession(),
                ensemblAnno == null
                    ? "."
                    : ensemblAnno.getTranscript().isCoding() ? "TRUE" : "FALSE",
                ensemblAnno == null
                    ? "."
                    : ensemblAnno.getCDSNTChange() == null
                        ? "."
                        : (ensemblAnno.getTranscript().isCoding() ? "c." : "n.")
                            + ensemblAnno.getCDSNTChange().toHGVSString(AminoAcidCode.ONE_LETTER),
                ensemblAnno == null
                    ? "."
                    : ensemblAnno.getProteinChange() == null
                        ? "."
                        : "p."
                            + ensemblAnno
                                .getProteinChange()
                                .withOnlyPredicted(false)
                                .toHGVSString(AminoAcidCode.ONE_LETTER),
                (ensemblAnno == null) ? "{}" : buildEffectsValue(ensemblAnno.getEffects()));
        // Write record to output stream.
        try {
          gtWriter.append(Joiner.on("\t").join(gtOutRec) + "\n");
        } catch (IOException e) {
          throw new VarfishAnnotatorException("Problem writing to genotypes call file.", e);
        }
      }
    }
  }

  private void writeVariantAnnotation(
      FileWriter varWriter, Annotation annotation, VariantDescription normalizedVar, String dbName)
      throws VarfishAnnotatorException {
    if (annotation.getTranscript() == null) {
      return; // no transcript, no annotation
    }
    final String geneId = annotation.getTranscript().getGeneID();

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
            dbName,
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
      throw new VarfishAnnotatorException("Problem writing to variant annotation file.", e);
    }
  }

  private ImmutableList<VariantAnnotations> silentBuildAnnotations(
      VariantContext ctx, VariantContextAnnotator annotator) {
    try {
      return annotator.buildAnnotations(ctx);
    } catch (InvalidCoordinatesException e) {
      return null;
    }
  }

  private void writeEmptyAnnoLine(VariantDescription normalizedVar, FileWriter varWriter, int i)
      throws VarfishAnnotatorException {
    try {
      // TODO: normalize variant
      varWriter.append(
          Joiner.on("\t")
                  .join(
                      args.getRelease(),
                      normalizedVar.getChrom(),
                      normalizedVar.getPos() + 1,
                      normalizedVar.getRef(),
                      normalizedVar.getAlt(),
                      ".",
                      ".",
                      ".",
                      ".",
                      ".",
                      ".",
                      ".")
              + "\n");
    } catch (IOException e) {
      throw new VarfishAnnotatorException("Problem writing to variant annotation file.", e);
    }
  }

  private List<Annotation> sortAnnos(
      ImmutableList<VariantAnnotations> refseqAnnotationsList, int i) {
    if (refseqAnnotationsList == null) {
      return new ArrayList<>();
    } else {
      return refseqAnnotationsList
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
  private String buildEffectsValue(ImmutableSet<VariantEffect> effects) {
    final List<String> effectStrings =
        effects
            .stream()
            .map(e -> "\"" + e.getSequenceOntologyTerm() + "\"")
            .collect(Collectors.toList());
    return Joiner.on("").join("{", Joiner.on(',').join(effectStrings), "}");
  }

  /**
   * Query DB for information about variant.
   *
   * @param conn Database connection to use for query.
   * @param release Genome release.
   * @param normalizedVar Normalized variant.
   * @param prefix Prefix for fields and table.
   * @return {@link DbInfo} with information from ExAC.
   * @throw VarfishAnnotatorException in case of problems with obtaining information
   */
  private DbInfo getDbInfo(
      Connection conn, String release, VariantDescription normalizedVar, String prefix)
      throws VarfishAnnotatorException {
    final String query =
        "SELECT "
            + prefix
            + "_af_popmax, "
            + prefix
            + "_het, "
            + prefix
            + "_hom, "
            + prefix
            + "_hemi FROM "
            + prefix
            + "_var WHERE (release = ?) AND (chrom = ?) AND (pos = ?) AND (ref = ?) AND (alt = ?)";
    try {
      final PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, release);
      stmt.setString(2, normalizedVar.getChrom());
      stmt.setInt(3, normalizedVar.getPos() + 1);
      stmt.setString(4, normalizedVar.getRef());
      stmt.setString(5, normalizedVar.getAlt());

      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          return DbInfo.nullValue();
        }
        final DbInfo result = new DbInfo(rs.getDouble(1), rs.getInt(2), rs.getInt(3), rs.getInt(4));
        if (rs.next()) {
          throw new VarfishAnnotatorException("ExAC returned more than one result");
        }
        return result;
      }
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem with querying ExAC", e);
    }
  }

  /**
   * Query ClinVar for information about variant.
   *
   * @param conn Database connection to use for query.
   * @param release Genome release.
   * @param normalizedVar Normalized variant to query with.
   * @return {@code bool} specifying whether variant is in ClinVar.
   * @throw VarfishAnnotatorException in case of problems with obtaining information
   */
  private boolean getClinVarInfo(Connection conn, String release, VariantDescription normalizedVar)
      throws VarfishAnnotatorException {
    final String query =
        "SELECT COUNT(*) FROM clinvar_var "
            + "WHERE (release = ?) AND (chrom = ?) AND (pos = ?) AND (ref = ?) AND (alt = ?)";
    try {
      final PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, release);
      stmt.setString(2, normalizedVar.getChrom());
      stmt.setInt(3, normalizedVar.getPos() + 1);
      stmt.setString(4, normalizedVar.getRef());
      stmt.setString(5, normalizedVar.getAlt());

      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          throw new VarfishAnnotatorException(
              "ClinVar counter query returned less than one result");
        }
        final boolean result = (rs.getInt(1) > 0);
        if (rs.next()) {
          throw new VarfishAnnotatorException(
              "ClinVar counter query returned more than one result");
        }
        return result;
      }
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem with querying ClinVar", e);
    }
  }
}
