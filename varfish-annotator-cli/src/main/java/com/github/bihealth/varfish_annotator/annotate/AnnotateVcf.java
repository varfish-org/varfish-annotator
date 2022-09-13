package com.github.bihealth.varfish_annotator.annotate;

import com.github.bihealth.varfish_annotator.VarfishAnnotatorException;
import com.github.bihealth.varfish_annotator.checks.IncompatibleVcfException;
import com.github.bihealth.varfish_annotator.checks.VcfCompatibilityChecker;
import com.github.bihealth.varfish_annotator.data.GenomeVersion;
import com.github.bihealth.varfish_annotator.data.VcfConstants;
import com.github.bihealth.varfish_annotator.db.DbInfo;
import com.github.bihealth.varfish_annotator.db.DbInfoWriterHelper;
import com.github.bihealth.varfish_annotator.utils.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import de.charite.compbio.jannovar.annotation.Annotation;
import de.charite.compbio.jannovar.annotation.VariantAnnotations;
import de.charite.compbio.jannovar.annotation.VariantEffect;
import de.charite.compbio.jannovar.data.JannovarData;
import de.charite.compbio.jannovar.data.JannovarDataSerializer;
import de.charite.compbio.jannovar.data.ReferenceDictionary;
import de.charite.compbio.jannovar.data.SerializationException;
import de.charite.compbio.jannovar.hgvs.AminoAcidCode;
import de.charite.compbio.jannovar.htsjdk.InvalidCoordinatesException;
import de.charite.compbio.jannovar.htsjdk.VariantContextAnnotator;
import de.charite.compbio.jannovar.htsjdk.VariantContextAnnotator.Options;
import de.charite.compbio.jannovar.pedigree.PedFileContents;
import de.charite.compbio.jannovar.pedigree.PedFileReader;
import de.charite.compbio.jannovar.pedigree.PedParseException;
import de.charite.compbio.jannovar.pedigree.Pedigree;
import de.charite.compbio.jannovar.reference.GenomeInterval;
import de.charite.compbio.jannovar.reference.Strand;
import de.charite.compbio.jannovar.reference.TranscriptModel;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
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

  /** Configuration for the command. */
  private final AnnotateArgs args;

  /** Pedigree to use for annotation. */
  private Pedigree pedigree;

  /** Construct with the given configuration. */
  public AnnotateVcf(AnnotateArgs args) {
    this.args = args;
    this.pedigree = null;
  }

  /** Execute the command. */
  public void run() {
    System.err.println("Running annotate; args: " + args);
    if (!ImmutableList.of("GRCh37", "GRCh38").contains(args.getRelease())) {
      System.err.println("Invalid release: " + args.getRelease() + ", not one of GRCh37, GRCh38");
      System.exit(1);
    }

    String dbPath = args.getDbPath();
    if (dbPath.endsWith(".h2.db")) {
      dbPath = dbPath.substring(0, dbPath.length() - ".h2.db".length());
    }

    if (args.getInputPed() != null) {
      final PedFileReader pedReader = new PedFileReader(new File(args.getInputPed()));
      final PedFileContents pedContents;
      try {
        pedContents = pedReader.read();
        this.pedigree =
            new Pedigree(pedContents, pedContents.getIndividuals().get(0).getPedigree());
      } catch (PedParseException | IOException e) {
        System.err.println("Problem loading pedigree");
        System.exit(1);
      }
    }
    try (Connection conn =
            DriverManager.getConnection(
                "jdbc:h2:"
                    + dbPath
                    + ";TRACE_LEVEL_FILE=0;MV_STORE=FALSE;MVCC=FALSE;ACCESS_MODE_DATA=r"
                    + ";DB_CLOSE_ON_EXIT=FALSE",
                "sa",
                "");
        VCFFileReader reader = new VCFFileReader(new File(args.getInputVcf()));
        OutputStream gtsStream = Files.newOutputStream(Paths.get(args.getOutputGts()));
        OutputStream dbInfoStream = Files.newOutputStream(Paths.get(args.getOutputDbInfos()));
        Writer gtWriter = GzipUtil.maybeOpenGzipOutputStream(gtsStream, args.getOutputGts());
        Writer dbInfoWriter =
            GzipUtil.maybeOpenGzipOutputStream(dbInfoStream, args.getOutputDbInfos());
        BufferedWriter dbInfoBufWriter = new BufferedWriter(dbInfoWriter); ) {
      new VcfCompatibilityChecker(reader).check(args.getRelease());
      new DatabaseSelfTest(conn)
          .selfTest(args.getRelease(), args.isSelfTestChr1Only(), args.isSelfTestChr22Only());

      System.err.println("Deserializing Jannovar file...");
      JannovarData refseqJvData = new JannovarDataSerializer(args.getRefseqSerPath()).load();
      JannovarData ensemblJvData = new JannovarDataSerializer(args.getEnsemblSerPath()).load();
      final VariantNormalizer normalizer = new VariantNormalizer(args.getRefPath());
      annotateVcf(conn, reader, refseqJvData, ensemblJvData, normalizer, gtWriter);
      new DbInfoWriterHelper()
          .writeDbInfos(conn, dbInfoBufWriter, args.getRelease(), AnnotateVcf.class);
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
    } catch (SelfTestFailedException e) {
      System.err.println("Problem with database self-test: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
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
   * @throws VarfishAnnotatorException in case of problems
   */
  private void annotateVcf(
      Connection conn,
      VCFFileReader reader,
      JannovarData refseqJv,
      JannovarData ensemblJv,
      VariantNormalizer normalizer,
      Writer gtWriter)
      throws VarfishAnnotatorException {
    // Guess genome version.
    final GenomeVersion genomeVersion = new VcfCompatibilityChecker(reader).guessGenomeVersion();

    // Write out header.
    try {
      gtWriter.append(Joiner.on("\t").join(VcfConstants.HEADERS_GT) + "\n");
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
          conn,
          genomeVersion,
          refseqJv.getRefDict(),
          refseqAnnotator,
          ensemblAnnotator,
          normalizer,
          ctx,
          gtWriter);
      prevChr = ctx.getContig();
    }
  }

  /**
   * Annotate <tt>ctx</tt>, write out annotated variant call to <tt>gtWriter</tt>.
   *
   * @param conn Database connection.
   * @param genomeVersion The genome version of the VCF file.
   * @param refDict {@code ReferenceDictionary} to use for chromosome mapping
   * @param refseqAnnotator Helper class to use for annotation of variants with Refseq
   * @param ensemblAnnotator Helper class to use for annotation of variants with ENSEMBL
   * @param normalizer Helper for normalizing variants.
   * @param ctx The variant to annotate.
   * @param gtWriter Writer for annotated genotypes.
   * @throws VarfishAnnotatorException in case of problems
   */
  private void annotateVariantContext(
      Connection conn,
      GenomeVersion genomeVersion,
      ReferenceDictionary refDict,
      VariantContextAnnotator refseqAnnotator,
      VariantContextAnnotator ensemblAnnotator,
      VariantNormalizer normalizer,
      VariantContext ctx,
      Writer gtWriter)
      throws VarfishAnnotatorException {
    ImmutableList<VariantAnnotations> refseqAnnotationsList =
        silentBuildAnnotations(ctx, refseqAnnotator);
    ImmutableList<VariantAnnotations> ensemblAnnotationsList =
        silentBuildAnnotations(ctx, ensemblAnnotator);

    final String contigName =
        (genomeVersion == GenomeVersion.HG19)
            ? ctx.getContig().replaceFirst("chr", "")
            : ctx.getContig();

    final int numAlleles = ctx.getAlleles().size();
    for (int i = 1; i < numAlleles; ++i) {
      // Skip allele if it is the asterisk allele.
      final String baseString = ctx.getAlternateAllele(i - 1).getBaseString();
      if ("*".equals(baseString)) {
        continue;
      }

      // Normalize the from the VCF (will probably pad variant to the left).
      final VariantDescription normalizedVar =
          normalizer.normalizeInsertion(
              new VariantDescription(
                  contigName, ctx.getStart() - 1, ctx.getReference().getBaseString(), baseString));
      // Get variant type string.
      final String varType = getVarType(normalizedVar);

      // Get annotations sorted descendingly by variant effect.
      //
      // Collect RefSeq and ENSEMBL annotations per gene.  Jannovar provides gene ID mappings
      // *to* both RefSeq and ENSEMBL *from* both RefSeq and ENSEMBL.  We use an (arbitrarily)
      // fixed lookup order as any should give sensible results except for genes whose annotation
      // is not stable (and would thus be less interpretable in a rare disease setting anyway).
      //
      // The only exception are variants that only have intergenic variants.  Here, we match
      // one arbitrary annotation for each transcript set.  The reason is that ENSEMBL contains
      // so many more transcripts/genes such that we would expect them to never match.

      // RefSeq first
      final List<Annotation> sortedRefseqAnnos =
          AnnoSorting.sortAnnotations(refseqAnnotationsList, i);
      final HashMap<String, Annotation> refSeqAnnoByRefSeqGene = new HashMap<>();
      final HashMap<String, Annotation> refSeqAnnoByEnsemblGene = new HashMap<>();
      extractAnnotations(sortedRefseqAnnos, refSeqAnnoByRefSeqGene, refSeqAnnoByEnsemblGene, true);

      // Then ENSEMBL
      final List<Annotation> sortedEnsemblAnnos =
          AnnoSorting.sortAnnotations(ensemblAnnotationsList, i);
      final HashMap<String, Annotation> ensemblAnnoByRefSeqGene = new HashMap<>();
      final HashMap<String, Annotation> ensemblAnnoByEnsemblGene = new HashMap<>();
      extractAnnotations(
          sortedEnsemblAnnos, ensemblAnnoByRefSeqGene, ensemblAnnoByEnsemblGene, false);

      // Query for frequency/presence information in databases.
      final String gnomadChrPrefix = "GRCh38".equals(args.getRelease()) ? "chr" : "";
      final DbInfo exacInfo =
          getDbInfo(conn, args.getRelease(), normalizedVar, VcfConstants.EXAC_PREFIX, "");
      final DbInfo gnomadExomesInfo =
          getDbInfo(
              conn,
              args.getRelease(),
              normalizedVar,
              VcfConstants.GNOMAD_EXOMES_PREFIX,
              gnomadChrPrefix);
      final DbInfo gnomadGenomesInfo =
          getDbInfo(
              conn,
              args.getRelease(),
              normalizedVar,
              VcfConstants.GNOMAD_GENOMES_PREFIX,
              gnomadChrPrefix);
      final DbInfo thousandGenomesInfo =
          getDbInfo(
              conn, args.getRelease(), normalizedVar, VcfConstants.THOUSAND_GENOMES_PREFIX, "");
      final boolean inClinvar = getClinVarInfo(conn, args.getRelease(), normalizedVar);

      // Build a list of all gene IDs that we will iterate over later.
      final TreeSet<String> geneIds = new TreeSet<>();
      geneIds.addAll(refSeqAnnoByRefSeqGene.keySet());
      geneIds.addAll(refSeqAnnoByEnsemblGene.keySet());
      geneIds.addAll(ensemblAnnoByRefSeqGene.keySet());
      geneIds.addAll(ensemblAnnoByEnsemblGene.keySet());

      // List of gene IDs that have been processed now.
      final TreeSet<String> doneGeneIds = new TreeSet<>();

      // Additional information.
      final String infoStr = "{}";

      // Build per-genotype counts, taking into consideration the sex information from pedigree.
      final GenotypeCounts gtCounts =
          GenotypeCounts.buildGenotypeCounts(ctx, i, pedigree, args.getRelease());

      // Write output record (alsow write out empty one if necessary).
      writeOutputRecords(
          refDict,
          ctx,
          gtWriter,
          i,
          normalizedVar,
          varType,
          refSeqAnnoByRefSeqGene,
          refSeqAnnoByEnsemblGene,
          ensemblAnnoByRefSeqGene,
          ensemblAnnoByEnsemblGene,
          exacInfo,
          gnomadExomesInfo,
          gnomadGenomesInfo,
          thousandGenomesInfo,
          inClinvar,
          geneIds,
          doneGeneIds,
          infoStr,
          gtCounts);
    }
  }

  private static String getVarType(VariantDescription normalizedVar) {
    final String varType;
    if ((normalizedVar.getRef().length() == 1) && (normalizedVar.getAlt().length() == 1)) {
      varType = "snv";
    } else if (normalizedVar.getRef().length() == normalizedVar.getAlt().length()) {
      varType = "mnv";
    } else {
      varType = "indel";
    }
    return varType;
  }

  /**
   * Write output records with the data built for {@code ctx).
   *
   * This function will also take care of writing out an empty record in case there were no overlapping genes.
   */
  private void writeOutputRecords(
      ReferenceDictionary refDict,
      VariantContext ctx,
      Writer gtWriter,
      int i,
      VariantDescription normalizedVar,
      String varType,
      HashMap<String, Annotation> refSeqAnnoByRefSeqGene,
      HashMap<String, Annotation> refSeqAnnoByEnsemblGene,
      HashMap<String, Annotation> ensemblAnnoByRefSeqGene,
      HashMap<String, Annotation> ensemblAnnoByEnsemblGene,
      DbInfo exacInfo,
      DbInfo gnomadExomesInfo,
      DbInfo gnomadGenomesInfo,
      DbInfo thousandGenomesInfo,
      boolean inClinvar,
      TreeSet<String> geneIds,
      TreeSet<String> doneGeneIds,
      String infoStr,
      GenotypeCounts gtCounts)
      throws VarfishAnnotatorException {
    // Write one entry for each gene into the annotated genotype call file.
    for (String geneId : geneIds) {
      if (doneGeneIds.contains(geneId)) {
        continue; // Do not process twice.
      }

      // Select both RefSeq and ENSEMBL annotation for the given (RefSeq or ENSEMBL gene ID).
      final Annotation refseqAnno;
      if (refSeqAnnoByRefSeqGene.containsKey(geneId)) {
        refseqAnno = refSeqAnnoByRefSeqGene.get(geneId);
      } else if (refSeqAnnoByEnsemblGene.containsKey(geneId)) {
        refseqAnno = refSeqAnnoByEnsemblGene.get(geneId);
      } else {
        refseqAnno = null;
      }
      final Annotation ensemblAnno;
      if (ensemblAnnoByEnsemblGene.containsKey(geneId)) {
        ensemblAnno = ensemblAnnoByEnsemblGene.get(geneId);
      } else if (ensemblAnnoByRefSeqGene.containsKey(geneId)) {
        ensemblAnno = ensemblAnnoByRefSeqGene.get(geneId);
      } else {
        ensemblAnno = null;
      }

      // Mark all gene IDs as done.
      if (ensemblAnno != null && ensemblAnno.getTranscript() != null) {
        doneGeneIds.add(ensemblAnno.getTranscript().getGeneID());
        if (ensemblAnno.getTranscript().getAltGeneIDs().containsKey("ENTREZ_ID")) {
          doneGeneIds.add(ensemblAnno.getTranscript().getAltGeneIDs().get("ENTREZ_ID"));
        }
      }
      if (refseqAnno != null && refseqAnno.getTranscript() != null) {
        doneGeneIds.add(refseqAnno.getTranscript().getGeneID());
        if (refseqAnno.getTranscript().getAltGeneIDs().containsKey("ENSEMBL_GENE_ID")) {
          doneGeneIds.add(refseqAnno.getTranscript().getAltGeneIDs().get("ENSEMBL_GENE_ID"));
        }
      }

      // Distance to next base of exon.
      final int refSeqExonDist =
          getDistance(normalizedVar, refseqAnno == null ? null : refseqAnno.getTranscript());
      final int ensemblExonDist =
          getDistance(normalizedVar, ensemblAnno == null ? null : ensemblAnno.getTranscript());

      // Construct output record.
      final List<Object> gtOutRec =
          constructOutputRecord(
              refDict,
              ctx,
              i,
              normalizedVar,
              varType,
              exacInfo,
              gnomadExomesInfo,
              gnomadGenomesInfo,
              thousandGenomesInfo,
              inClinvar,
              infoStr,
              gtCounts,
              refseqAnno,
              ensemblAnno,
              refSeqExonDist,
              ensemblExonDist);
      // Write record to output stream.
      try {
        gtWriter.append(Joiner.on("\t").join(gtOutRec) + "\n");
      } catch (IOException e) {
        throw new VarfishAnnotatorException("Problem writing to genotypes call file.", e);
      }
    }

    if (geneIds.isEmpty()) {
      // Construct output record.
      final List<Object> gtOutRec =
          constructEmptyOutputRecord(
              refDict,
              ctx,
              i,
              normalizedVar,
              varType,
              exacInfo,
              gnomadExomesInfo,
              gnomadGenomesInfo,
              thousandGenomesInfo,
              inClinvar,
              infoStr,
              gtCounts);
      // Write record to output stream.
      try {
        gtWriter.append(Joiner.on("\t").join(gtOutRec) + "\n");
      } catch (IOException e) {
        throw new VarfishAnnotatorException("Problem writing to genotypes call file.", e);
      }
    }
  }

  private List<Object> constructOutputRecord(
      ReferenceDictionary refDict,
      VariantContext ctx,
      int i,
      VariantDescription normalizedVar,
      String varType,
      DbInfo exacInfo,
      DbInfo gnomadExomesInfo,
      DbInfo gnomadGenomesInfo,
      DbInfo thousandGenomesInfo,
      boolean inClinvar,
      String infoStr,
      GenotypeCounts gtCounts,
      Annotation refseqAnno,
      Annotation ensemblAnno,
      int refSeqExonDist,
      int ensemblExonDist) {
    return Lists.newArrayList(
        args.getRelease(),
        normalizedVar.getChrom(),
        refDict.getContigNameToID().get(normalizedVar.getChrom()),
        String.valueOf(normalizedVar.getPos() + 1),
        String.valueOf(normalizedVar.getPos() + normalizedVar.getRef().length()),
        UcscBinning.getContainingBin(
            normalizedVar.getPos(), normalizedVar.getPos() + normalizedVar.getRef().length()),
        normalizedVar.getRef(),
        normalizedVar.getAlt(),
        varType,
        args.getCaseId(),
        args.getSetId(),
        infoStr,
        buildGenotypeValue(ctx, i),
        String.valueOf(gtCounts.numHomAlt),
        String.valueOf(gtCounts.numHomRef),
        String.valueOf(gtCounts.numHet),
        String.valueOf(gtCounts.numHemiAlt),
        String.valueOf(gtCounts.numHemiRef),
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
        (refseqAnno == null || refseqAnno.getTranscript() == null)
            ? "."
            : refseqAnno.getTranscript().getGeneID(),
        (refseqAnno == null || refseqAnno.getTranscript() == null)
            ? "."
            : refseqAnno.getTranscript().getAccession(),
        (refseqAnno == null || refseqAnno.getTranscript() == null)
            ? "."
            : refseqAnno.getTranscript().isCoding() ? "TRUE" : "FALSE",
        (refseqAnno == null || refseqAnno.getTranscript() == null)
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
        (refseqAnno == null || refseqAnno.getTranscript() == null)
            ? "{}"
            : buildEffectsValue(refseqAnno.getEffects()),
        (refSeqExonDist >= 0) ? Integer.toString(refSeqExonDist) : ".",
        // ENSEMBL
        (ensemblAnno == null || ensemblAnno.getTranscript() == null)
            ? "."
            : ensemblAnno.getTranscript().getGeneID(),
        (ensemblAnno == null || ensemblAnno.getTranscript() == null)
            ? "."
            : ensemblAnno.getTranscript().getAccession(),
        (ensemblAnno == null || ensemblAnno.getTranscript() == null)
            ? "."
            : ensemblAnno.getTranscript().isCoding() ? "TRUE" : "FALSE",
        (ensemblAnno == null || ensemblAnno.getTranscript() == null)
            ? "."
            : ensemblAnno.getCDSNTChange() == null
                ? "."
                : (ensemblAnno.getTranscript().isCoding() ? "c." : "n.")
                    + ensemblAnno.getCDSNTChange().toHGVSString(AminoAcidCode.ONE_LETTER),
        (ensemblAnno == null || ensemblAnno.getTranscript() == null)
            ? "."
            : ensemblAnno.getProteinChange() == null
                ? "."
                : "p."
                    + ensemblAnno
                        .getProteinChange()
                        .withOnlyPredicted(false)
                        .toHGVSString(AminoAcidCode.ONE_LETTER),
        (ensemblAnno == null || ensemblAnno.getTranscript() == null)
            ? "{}"
            : buildEffectsValue(ensemblAnno.getEffects()),
        (ensemblExonDist >= 0) ? Integer.toString(ensemblExonDist) : ".");
  }

  private List<Object> constructEmptyOutputRecord(
      ReferenceDictionary refDict,
      VariantContext ctx,
      int i,
      VariantDescription normalizedVar,
      String varType,
      DbInfo exacInfo,
      DbInfo gnomadExomesInfo,
      DbInfo gnomadGenomesInfo,
      DbInfo thousandGenomesInfo,
      boolean inClinvar,
      String infoStr,
      GenotypeCounts gtCounts) {
    return Lists.newArrayList(
        args.getRelease(),
        normalizedVar.getChrom(),
        refDict.getContigNameToID().get(normalizedVar.getChrom()),
        String.valueOf(normalizedVar.getPos() + 1),
        String.valueOf(normalizedVar.getPos() + normalizedVar.getRef().length()),
        UcscBinning.getContainingBin(
            normalizedVar.getPos(), normalizedVar.getPos() + normalizedVar.getRef().length()),
        normalizedVar.getRef(),
        normalizedVar.getAlt(),
        varType,
        args.getCaseId(),
        args.getSetId(),
        infoStr,
        buildGenotypeValue(ctx, i),
        String.valueOf(gtCounts.numHomAlt),
        String.valueOf(gtCounts.numHomRef),
        String.valueOf(gtCounts.numHet),
        String.valueOf(gtCounts.numHemiAlt),
        String.valueOf(gtCounts.numHemiRef),
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
        ".",
        ".",
        ".",
        ".",
        ".",
        ".",
        ".",
        // ENSEMBL
        ".",
        ".",
        ".",
        ".",
        ".",
        ".",
        ".");
  }

  /**
   * Collect RefSeq or ENSEMBL annotations per gene.
   *
   * <p>See comment in {@link #annotateVariantContext} on the reasoning behind "__integergenic__".
   *
   * @param sortedAnnos Sorted annotations to process.
   * @param annoByRefSeqGene Annotations by RefSeq gene are written here.
   * @param annoByEnsemblGene Annotations by ENSEMBL gene ID.
   * @param isRefSeq Whether to process as refseq or
   */
  private static void extractAnnotations(
      List<Annotation> sortedAnnos,
      HashMap<String, Annotation> annoByRefSeqGene,
      HashMap<String, Annotation> annoByEnsemblGene,
      boolean isRefSeq) {
    for (Annotation annotation : sortedAnnos) {
      if (annotation.getEffects().isEmpty()
          || annotation.getEffects().equals(ImmutableSet.of(VariantEffect.INTERGENIC_VARIANT))) {
        // Put into map under pseudo-identifier "__intergenic__". This ways, intergenic variants
        // are written out only once for RefSeq/ENSEMBL and not twice if different genes are
        // closest.
        annoByRefSeqGene.put("__intergenic__", annotation);
        annoByEnsemblGene.put("__intergenic__", annotation);
      } else {
        final String refseqGeneId;
        final String ensemblGeneId;
        if (isRefSeq) {
          refseqGeneId = annotation.getTranscript().getGeneID();
          ensemblGeneId = annotation.getTranscript().getAltGeneIDs().get("ENSEMBL_GENE_ID");
        } else {
          refseqGeneId = annotation.getTranscript().getAltGeneIDs().get("ENTREZ_ID");
          ensemblGeneId = annotation.getTranscript().getGeneID();
        }
        if (refseqGeneId != null && !annoByRefSeqGene.containsKey(refseqGeneId)) {
          annoByRefSeqGene.put(refseqGeneId, annotation);
        }
        if (ensemblGeneId != null && !annoByEnsemblGene.containsKey(ensemblGeneId)) {
          annoByEnsemblGene.put(ensemblGeneId, annotation);
        }
      }
    }
  }

  /**
   * Return distance from {@code normalizedVar} to exons of {@code transcript}.
   *
   * @param normalizedVar The normalized variant.
   * @param transcript The transcript to compute distance to.
   * @return Distance to base of exon, {@code -1} if {@code transcript} is {@code null}.
   */
  private int getDistance(VariantDescription normalizedVar, TranscriptModel transcript) {
    if (transcript == null) {
      return -1;
    } else {
      final ReferenceDictionary refDict = transcript.getTXRegion().getRefDict();
      final String txChrom = refDict.getContigIDToName().get(transcript.getChr());

      if (!normalizedVar.getChrom().equals(txChrom)) {
        return -1;
      }

      final GenomeInterval varInterval =
          new GenomeInterval(
              refDict,
              Strand.FWD,
              transcript.getChr(),
              normalizedVar.getPos(),
              normalizedVar.getEnd());

      int result = -1;
      for (GenomeInterval exon : transcript.getExonRegions()) {
        exon = exon.withStrand(Strand.FWD); // normalize strand
        if (exon.overlapsWith(varInterval)) { // variant overlaps with exon
          result = 0;
          break;
        } else {
          // Get distance between {@code exon} and {@code varInterval}.
          final int distance;
          if (varInterval.getEndPos() <= exon.getBeginPos()) { // variant left of exon
            distance = (exon.getBeginPos() - varInterval.getEndPos()) + 1;
          } else { // variant right of exon
            if (!(varInterval.getBeginPos() >= exon.getEndPos())) {
              throw new RuntimeException("Invariant violated!");
            }
            distance = (varInterval.getBeginPos() - exon.getEndPos()) + 1;
          }
          // Update {@code result} if necessary.
          if (result == -1 || result > distance) {
            result = distance;
          }
        }
      }
      return result;
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
                  String.valueOf(ad == null ? -1 : (alleleNo >= ad.length ? -1 : ad[alleleNo])),
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
   * @param chrPrefix Prefix for chromosome values.
   * @return {@link DbInfo} with information from ExAC.
   * @throw VarfishAnnotatorException in case of problems with obtaining information
   */
  private DbInfo getDbInfo(
      Connection conn,
      String release,
      VariantDescription normalizedVar,
      String prefix,
      String chrPrefix)
      throws VarfishAnnotatorException {
    // Return "not found" if looking for ExAC or Thousand Genomes but not using GRCh37.
    // These are only available for GRCh37.
    if (!"GRCh37".equals(release)
        && ImmutableList.of(VcfConstants.EXAC_PREFIX, VcfConstants.THOUSAND_GENOMES_PREFIX)
            .contains(prefix)) {
      return DbInfo.nullValue();
    }

    final String query =
        "SELECT "
            + prefix
            + "_af, "
            + prefix
            + "_het, "
            + prefix
            + "_hom, "
            + prefix
            + "_hemi FROM "
            + prefix
            + "_var WHERE (release = ?) AND (chrom = ?) AND (start = ?) AND (ref = ?) AND (alt = ?)";
    try {
      final PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, release);
      final String chrom;
      if (chrPrefix.isEmpty() && normalizedVar.getChrom().startsWith("chr")) {
        chrom = normalizedVar.getChrom().substring(3);
      } else if (!chrPrefix.isEmpty() && !normalizedVar.getChrom().startsWith("chr")) {
        chrom = "chr" + normalizedVar.getChrom();
      } else {
        chrom = normalizedVar.getChrom();
      }
      stmt.setString(2, chrom);
      stmt.setInt(3, normalizedVar.getPos() + 1);
      stmt.setString(4, normalizedVar.getRef());
      stmt.setString(5, normalizedVar.getAlt());

      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          return DbInfo.nullValue();
        }
        final DbInfo result = new DbInfo(rs.getDouble(1), rs.getInt(2), rs.getInt(3), rs.getInt(4));
        if (rs.next()) {
          throw new VarfishAnnotatorException(prefix + " returned more than one result");
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
            + "WHERE (release = ?) AND (chrom = ?) AND (start = ?) AND (ref = ?) AND (alt = ?)";
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
