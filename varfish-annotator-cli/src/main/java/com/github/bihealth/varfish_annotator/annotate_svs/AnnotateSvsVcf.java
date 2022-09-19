package com.github.bihealth.varfish_annotator.annotate_svs;

import com.github.bihealth.varfish_annotator.VarfishAnnotatorException;
import com.github.bihealth.varfish_annotator.annotate.AnnotateVcf;
import com.github.bihealth.varfish_annotator.checks.IncompatibleVcfException;
import com.github.bihealth.varfish_annotator.checks.VcfCompatibilityChecker;
import com.github.bihealth.varfish_annotator.data.GenomeVersion;
import com.github.bihealth.varfish_annotator.db.DbInfoWriterHelper;
import com.github.bihealth.varfish_annotator.utils.*;
import com.google.code.externalsorting.csv.CSVRecordBuffer;
import com.google.code.externalsorting.csv.CsvExternalSort;
import com.google.code.externalsorting.csv.CsvSortOptions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import de.charite.compbio.jannovar.annotation.SVAnnotation;
import de.charite.compbio.jannovar.annotation.SVAnnotations;
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
import de.charite.compbio.jannovar.pedigree.PedFileContents;
import de.charite.compbio.jannovar.pedigree.PedFileReader;
import de.charite.compbio.jannovar.pedigree.PedParseException;
import de.charite.compbio.jannovar.pedigree.Pedigree;
import de.charite.compbio.jannovar.reference.SVGenomeVariant;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/** Implementation of the <tt>annotate-svs</tt> command. */
public final class AnnotateSvsVcf {

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

  /** Pedigree to use for annotation. */
  private Pedigree pedigree;

  /** Construct with the given configuration. */
  public AnnotateSvsVcf(AnnotateSvsArgs args) {
    this.args = args;
    this.pedigree = null;
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
    checkOptOutFeatures();
    checkWriteOutBndMates();

    System.err.println("Running annotate-svs; args: " + args);
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

    Path tmpDir = null;
    try {
      tmpDir = Files.createTempDirectory("varfish-annotator");
    } catch (IOException e) {
      System.err.println("Could not create temporary directory");
      System.exit(1);
    }
    final Path tmpGtsPath = Paths.get(tmpDir.toString(), "tmp.gts.tsv");

    try (Connection conn =
            DriverManager.getConnection(
                "jdbc:h2:"
                    + dbPath
                    + ";MV_STORE=FALSE;MVCC=FALSE;ACCESS_MODE_DATA=r;"
                    + "DB_CLOSE_ON_EXIT=FALSE",
                "sa",
                "");
        VCFFileReader reader = new VCFFileReader(new File(args.getInputVcf()));
        OutputStream featureEffectsStream =
            Files.newOutputStream(Paths.get(args.getOutputFeatureEffects()));
        OutputStream dbInfoStream = Files.newOutputStream(Paths.get(args.getOutputDbInfos()));
        OutputStream tmpGtsStream = Files.newOutputStream(tmpGtsPath);
        Writer tmpGtsWriter =
            GzipUtil.maybeOpenGzipOutputStream(tmpGtsStream, tmpGtsPath.toString());
        Writer featureEffectsWriter =
            GzipUtil.maybeOpenGzipOutputStream(
                featureEffectsStream, args.getOutputFeatureEffects());
        Writer dbInfoWriter =
            GzipUtil.maybeOpenGzipOutputStream(dbInfoStream, args.getOutputDbInfos());
        BufferedWriter dbInfoBufWriter = new BufferedWriter(dbInfoWriter);
        Closer covVcfCloser = Closer.create(); ) {
      // Guess genome version.
      GenomeVersion genomeVersion = new VcfCompatibilityChecker(reader).guessGenomeVersion();

      new VcfCompatibilityChecker(reader).check(args.getRelease());
      new DatabaseSelfTest(conn)
          .selfTest(args.getRelease(), args.isSelfTestChr1Only(), args.isSelfTestChr22Only());

      final Map<String, CoverageFromMaelstromReader> covReaders = new TreeMap<>();
      for (String covVcf : args.getCoverageVcfs()) {
        final CoverageFromMaelstromReader covReader =
            new CoverageFromMaelstromReader(new File(covVcf));
        covReaders.put(covReader.getSample(), covReader);
        covVcfCloser.register(covReader);
      }
      final CallerSupport callerSupport =
          new CallerSupportFactory(covReaders).getFor(new File(args.getInputVcf()));

      System.err.println("Deserializing Jannovar file...");
      JannovarData refseqJvData = new JannovarDataSerializer(args.getRefseqSerPath()).load();
      JannovarData ensemblJvData = new JannovarDataSerializer(args.getEnsemblSerPath()).load();
      annotateSvVcf(
          genomeVersion,
          reader,
          refseqJvData,
          ensemblJvData,
          callerSupport,
          tmpGtsWriter,
          featureEffectsWriter);

      // Finalize genotypes and write out sorted
      tmpGtsWriter.close();
      writeSortedGts(tmpGtsPath);

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

  /** Finalize and write out sorted files. */
  private void writeSortedGts(Path tmpGtsPath) throws IOException {
    // Configuration for sorting
    final boolean hasChrom2Columns =
        !args.getOptOutFeatures().contains(GtRecordBuilder.FEATURE_CHROM2_COLUMNS);
    final CsvSortOptions sortOptions =
        new CsvSortOptions.Builder(
                new VarFishGtsTsvComparator(hasChrom2Columns),
                CsvExternalSort.DEFAULTMAXTEMPFILES,
                CsvExternalSort.estimateAvailableMemory())
            .charset(Charset.defaultCharset())
            .distinct(false)
            .numHeader(1)
            .skipHeader(false)
            .format(
                CSVFormat.DEFAULT
                    .builder()
                    .setDelimiter('\t')
                    .setIgnoreSurroundingSpaces(true)
                    .setQuote((Character) null)
                    .build())
            .build();

    // Sort genotypes file and write final file to output.
    final ArrayList<CSVRecord> gtHeader = new ArrayList<>();
    final List<File> gtSortInBatch =
        CsvExternalSort.sortInBatch(tmpGtsPath.toFile(), null, sortOptions, gtHeader);
    try (OutputStream gtsStream = Files.newOutputStream(Paths.get(args.getOutputGts()));
        Writer gtsWriter = GzipUtil.maybeOpenGzipOutputStream(gtsStream, args.getOutputGts());
        BufferedWriter bufWriter = new BufferedWriter(gtsWriter)) {
      List<CSVRecordBuffer> bfbs = new ArrayList<>();
      for (File f : gtSortInBatch) {
        InputStream in = new FileInputStream(f);
        BufferedReader fbr =
            new BufferedReader(new InputStreamReader(in, sortOptions.getCharset()));
        CSVParser parser = new CSVParser(fbr, sortOptions.getFormat());
        CSVRecordBuffer bfb = new CSVRecordBuffer(parser);
        bfbs.add(bfb);
      }

      CsvExternalSort.mergeSortedFiles(bufWriter, sortOptions, bfbs, gtHeader);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Problem with external file sort", e);
    }
  }

  private void checkWriteOutBndMates() {
    if (!ImmutableList.of("true", "false", "auto").contains(args.getWriteBndMates())) {
      System.err.println("Unsupported feature in --opt-out: " + args.getWriteBndMates());
      System.exit(1);
    }
  }

  private void checkOptOutFeatures() {
    if ("".equals(args.getOptOutFeatures()) || args.getOptOutFeatures() == null) {
      return;
    }

    final ImmutableList<String> supportedFeatures =
        ImmutableList.of(
            GtRecordBuilder.FEATURE_CHROM2_COLUMNS,
            GtRecordBuilder.FEATURE_DBCOUNTS_COLUMNS,
            GtRecordBuilder.FEATURE_SUPPRESS_CARRIER_COUNTS);
    final String features[] = args.getOptOutFeatures().split(",");
    boolean allGood = true;
    for (String feature : features) {
      if (!supportedFeatures.contains(feature)) {
        System.err.println("Unsupported feature in --opt-out: " + feature);
        allGood = false;
      }
    }
    if (!allGood) {
      System.exit(1);
    }
  }

  /**
   * Perform the variant annotation.
   *
   * @param reader Reader for the input VCF file.
   * @param refseqJv Deserialized RefSeq transcript database for Jannovar.
   * @param ensemblJv Deserialized ENSEMBL transcript database for Jannovar.
   * @param callerSupport Helper to use for adapting to SV caller.
   * @param gtWriter Writer for variant call ("genotype") TSV file.
   * @param featureEffectsWriter Writer for gene-wise feature effects.
   * @throws VarfishAnnotatorException in case of problems
   */
  private void annotateSvVcf(
      GenomeVersion genomeVersion,
      VCFFileReader reader,
      JannovarData refseqJv,
      JannovarData ensemblJv,
      CallerSupport callerSupport,
      Writer gtWriter,
      Writer featureEffectsWriter)
      throws VarfishAnnotatorException {
    // Get list of filter values to skip.
    final ImmutableSet skipFilters = ImmutableSet.copyOf(args.getSkipFilters().split(","));

    // Helpers for building record for `.gts.tsv` and `.feature-effects.tsv` records.
    final GtRecordBuilder gtRecordBuilder =
        new GtRecordBuilder(
            args.getRelease(),
            args.getDefaultSvMethod(),
            args.getOptOutFeatures(),
            args.getCaseId(),
            args.getSetId(),
            pedigree,
            callerSupport);
    final FeatureEffectsRecordBuilder feRecordBuilder =
        new FeatureEffectsRecordBuilder(args.getCaseId(), args.getSetId());

    // Write out header.
    try {
      gtWriter.append(
          GenotypeRecord.tsvHeader(
                  !args.getOptOutFeatures().contains(GtRecordBuilder.FEATURE_CHROM2_COLUMNS),
                  !args.getOptOutFeatures().contains(GtRecordBuilder.FEATURE_DBCOUNTS_COLUMNS))
              + "\n");
      // Write feature-effects header.
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

    // Collect names of skipped contigs so we only create one log skipping them once.
    Set<String> skippedContigs = new HashSet<>();

    String prevChr = null;
    for (VariantContext ctx : reader) {
      if (skipRecord(ctx, skipFilters, skippedContigs)) {
        continue;
      } else if (!ctx.getContig().equals(prevChr)) {
        System.err.println("Now on contig " + ctx.getContig());
      }

      annotateVariantContext(
          refseqAnnotator,
          ensemblAnnotator,
          ctx,
          genomeVersion,
          gtRecordBuilder,
          feRecordBuilder,
          gtWriter,
          featureEffectsWriter);

      // Maybe generate mate's record and write it out.
      maybeWriteOutBndMate(
          genomeVersion,
          gtRecordBuilder,
          feRecordBuilder,
          gtWriter,
          featureEffectsWriter,
          refseqAnnotator,
          ensemblAnnotator,
          ctx);

      prevChr = ctx.getContig();
    }
  }

  /**
   * Write out BND mate record if the SV caller does not write it out.
   *
   * <p>Currently, we have explicit support for Delly2, Manta, and DRAGEN (which creates output
   * equivalent to Manta). In the case of "auto" mode for BND mate generation, we write out if the
   * method is Delly2 only.
   */
  private void maybeWriteOutBndMate(
      GenomeVersion genomeVersion,
      GtRecordBuilder gtRecordBuilder,
      FeatureEffectsRecordBuilder feRecordBuilder,
      Writer gtWriter,
      Writer featureEffectsWriter,
      VariantContextAnnotator refseqAnnotator,
      VariantContextAnnotator ensemblAnnotator,
      VariantContext ctx)
      throws VarfishAnnotatorException {
    final boolean isBnd = "BND".equals(ctx.getAttributeAsString("SVTYPE", ""));
    final boolean isDelly = ctx.getAttributeAsString("SVMETHOD", "").startsWith("EMBL.DELLYv");
    if (isBnd
        && (args.getWriteBndMates().equals("true")
            || args.getWriteBndMates().equals("auto") && isDelly)) {
      final VariantContext mateCtx = buildMateCtx(ctx);
      if (mateCtx != null) {
        annotateVariantContext(
            refseqAnnotator,
            ensemblAnnotator,
            mateCtx,
            genomeVersion,
            gtRecordBuilder,
            feRecordBuilder,
            gtWriter,
            featureEffectsWriter);
      }
    }
  }

  /** Handle logic for skipping a record, based on contig regular expressions, filters, etc. */
  private boolean skipRecord(
      VariantContext ctx, ImmutableSet skipFilters, Set<String> skippedContigs) {
    // Skip filtered SVs, e.g., LowQual
    if (ctx.getFilters().stream().anyMatch((String ft) -> skipFilters.contains(ft))) {
      return true;
    }
    // Skip DRAGEN "reference" lines; they have no alternate allele.
    if (ctx.getNAlleles() == 1) {
      return true;
    }

    // Check whether contigs should be skipped.
    if (skippedContigs.contains(ctx.getContig())) {
      return true; // skip silently
    } else if (!ctx.getContig().matches(args.getContigRegex())) {
      System.err.println("Skipping contig " + ctx.getContig());
      skippedContigs.add(ctx.getContig());
      return true;
    }

    return false;
  }

  /**
   * Build the {@link VariantContext} for the mate of <code>ctx</code>.
   *
   * <p>We do not try to guess the mate's reference base currently but place "N" there.
   *
   * @param ctx The {@link VariantContext} to build the mate for.
   * @return The BND mate's record.
   */
  private VariantContext buildMateCtx(VariantContext ctx) {
    // From the VCF4.2 docs (Section 5.4)
    //
    //    2  321681 bndW G G]17:198982] 6 PASS SVTYPE=BND 5to5    hasLeading       leftOpen
    //    17 198982 bndY A A]2:321681]  6 PASS SVTYPE=BND 5to5    hasLeading       leftOpen
    //
    //    2  321682 bndV T ]13:123456]T 6 PASS SVTYPE=BND 3to5   !hasLeading       leftOpen
    //    13 123456 bndU C C[2:321682[  6 PASS SVTYPE=BND 5to3    hasLeading      !leftOpen
    //
    //    13 123457 bndX A [17:198983[A 6 PASS SVTYPE=BND 3to3   !hasLeading      !leftOpen
    //    17 198983 bndZ C [13:123457[C 6 PASS SVTYPE=BND 3to3   !hasLeading      !leftOpen

    if (!"BND".equals(ctx.getAttributeAsString("SVTYPE", ""))) {
      throw new RuntimeException("Can only build mate records for SVTYPE=BND");
    }

    final VariantContextBuilder result = new VariantContextBuilder(ctx);
    result.attribute("CHR2", ctx.getContig());
    result.attribute("END", ctx.getStart());

    final ImmutableMap ctMap = ImmutableMap.of("3to5", "5to3", "5to3", "3to5");
    final String ct = ctx.getAttributeAsString("CT", null);
    result.attribute("CT", ctMap.getOrDefault(ct, ct));

    final String pattern = "^([a-zA-Z]*)([\\]\\[])([^:]+):(\\d+)([\\]\\[])([a-zA-Z]*)$";
    final Pattern r = Pattern.compile(pattern);
    final Matcher m = r.matcher(ctx.getAlternateAllele(0).getDisplayString());
    if (!m.matches()) {
      System.err.println("WARNING: could not build mate variant for " + ctx);
      return null;
    }
    final String leading = m.group(1);
    final String leftBracket = m.group(2);
    final String chrom = m.group(3);
    final int pos = Integer.valueOf(m.group(4));

    boolean hasLeading = leading != null;
    boolean leftOpen = "]".equals(leftBracket);

    final String mateDisplayBases;
    if (hasLeading && leftOpen) {
      mateDisplayBases = "N]" + ctx.getContig() + ":" + ctx.getStart() + "]";
    } else if (!hasLeading && !leftOpen) {
      mateDisplayBases = "[" + ctx.getContig() + ":" + ctx.getStart() + "[N";
    } else {
      if (hasLeading) { // => !leftOpen
        mateDisplayBases = "]" + ctx.getContig() + ":" + ctx.getStart() + "]N";
      } else { // => !hasLeading && leftOpen
        mateDisplayBases = "N]" + ctx.getContig() + ":" + ctx.getStart() + "]";
      }
    }
    result.chr(chrom);
    result.start(pos);
    result.stop(ctx.getStart());
    result.alleles("N", mateDisplayBases);
    result.attribute("CIPOS", ctx.getAttribute("CIEND"));
    result.attribute("CIEND", ctx.getAttribute("CIPOS"));

    return result.make();
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
      GtRecordBuilder gtRecordBuilder,
      FeatureEffectsRecordBuilder feRecordBuilder,
      Writer gtWriter,
      Writer featureEffectsWriter)
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
      final List<SVAnnotation> sortedRefseqAnnos =
          AnnoSorting.sortSvAnnos(refseqAnnotationsList, i);
      final List<SVAnnotation> sortedEnsemblAnnos =
          AnnoSorting.sortSvAnnos(ensemblAnnotationsList, i);

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
      final GenotypeRecord gtOutRec =
          gtRecordBuilder.buildRecord(variantId, svGenomeVar, ctx, genomeVersion, i);
      try {
        gtWriter.append(
            gtOutRec.toTsv(
                    !args.getOptOutFeatures().contains(GtRecordBuilder.FEATURE_CHROM2_COLUMNS),
                    !args.getOptOutFeatures().contains(GtRecordBuilder.FEATURE_DBCOUNTS_COLUMNS))
                + "\n");
      } catch (IOException e) {
        throw new VarfishAnnotatorException("Problem writing to genotypes call file.", e);
      }

      // Short-circuit here in case we don't have any feature annotation in either RefSeq and
      // ENSEMBL list.
      if (sortedRefseqAnnos.isEmpty() && sortedEnsemblAnnos.isEmpty()) {
        continue; // short-circuit
      }

      // Write one entry for each gene into the feature effect call file.
      final FeatureEffectsRecordBuilder.Result feResult =
          feRecordBuilder.buildAnnosByDb(sortedEnsemblAnnos, sortedRefseqAnnos);
      for (String geneId : feResult.getGeneIds()) {
        List<Object> featureEffectOutRec =
            feRecordBuilder.buildRecord(
                variantId,
                feResult.getRefseqAnnoByGene().get(geneId),
                feResult.getEnsemblAnnoByGene().get(geneId));
        try {
          featureEffectsWriter.append(
              Joiner.on("\t").useForNull(".").join(featureEffectOutRec) + "\n");
        } catch (IOException e) {
          throw new VarfishAnnotatorException("Problem writing to feature effects file.", e);
        }
      }
    }
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
}
