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
import htsjdk.samtools.util.IntervalTree;
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
    final Path tmpFesPath = Paths.get(tmpDir.toString(), "tmp.feature-effects.tsv");
    final Path tmpSortedGtsPath = Paths.get(tmpDir.toString(), "tmp.gts-sorted.tsv");

    try (Connection conn =
            DriverManager.getConnection(
                "jdbc:h2:"
                    + dbPath
                    + ";MV_STORE=FALSE;MVCC=FALSE;ACCESS_MODE_DATA=r;"
                    + "DB_CLOSE_ON_EXIT=FALSE",
                "sa",
                "");
        OutputStream dbInfoStream = Files.newOutputStream(Paths.get(args.getOutputDbInfos()));
        OutputStream tmpGtsStream = Files.newOutputStream(tmpGtsPath);
        OutputStream tmpFeStream = Files.newOutputStream(tmpFesPath);
        Writer tmpGtsWriter =
            GzipUtil.maybeOpenGzipOutputStream(tmpGtsStream, tmpGtsPath.toString());
        Writer tmpFeatureEffectsWriter =
            GzipUtil.maybeOpenGzipOutputStream(tmpFeStream, String.valueOf(tmpFesPath));
        Writer dbInfoWriter =
            GzipUtil.maybeOpenGzipOutputStream(dbInfoStream, args.getOutputDbInfos());
        BufferedWriter dbInfoBufWriter = new BufferedWriter(dbInfoWriter);
        Closer covVcfCloser = Closer.create(); ) {
      // Guess genome version, check for compatibility, perform database self-test.
      final GenomeVersion genomeVersion;
      try (VCFFileReader reader = new VCFFileReader(new File(args.getInputVcf().get(0))); ) {
        genomeVersion = new VcfCompatibilityChecker(reader).guessGenomeVersion();
        new VcfCompatibilityChecker(reader).check(args.getRelease());
        new DatabaseSelfTest(conn)
            .selfTest(args.getRelease(), args.isSelfTestChr1Only(), args.isSelfTestChr22Only());
      }

      // Initialize coverage readers.
      final Map<String, CoverageFromMaelstromReader> covReaders = new TreeMap<>();
      for (String covVcf : args.getCoverageVcfs()) {
        final CoverageFromMaelstromReader covReader =
            new CoverageFromMaelstromReader(new File(covVcf));
        covReaders.put(covReader.getSample(), covReader);
        covVcfCloser.register(covReader);
      }

      System.err.println("Deserializing Jannovar file...");
      JannovarData refseqJvData = new JannovarDataSerializer(args.getRefseqSerPath()).load();
      JannovarData ensemblJvData = new JannovarDataSerializer(args.getEnsemblSerPath()).load();

      // Process each input VCF file.
      boolean isFirst = true;
      for (String inputVcf : args.getInputVcf()) {
        System.err.println("Handling input VCF file: " + inputVcf);
        try (VCFFileReader reader = new VCFFileReader(new File(inputVcf)); ) {
          // Initialize per-tool helper for the current VCF file.
          final CallerSupport callerSupport =
              new CallerSupportFactory(covReaders).getFor(new File(inputVcf));

          annotateSvVcf(
              genomeVersion,
              reader,
              refseqJvData,
              ensemblJvData,
              callerSupport,
              tmpGtsWriter,
              tmpFeatureEffectsWriter,
              isFirst);
          isFirst = false;
        }
      }

      // Finalize genotypes and write out in (chrom, pos, start, end) sorted order.
      final Set<String> denySvUuid = new HashSet<>(); // SV UUIDs to remove, if any
      tmpGtsWriter.flush();
      tmpGtsWriter.close();
      if (args.getInputVcf().size() == 1) {
        // External sort and write to output file.
        writeSortedGts(tmpGtsPath, Paths.get(args.getOutputGts()));
      } else {
        // External sort and write to temporary file.  Merge by tool from there.
        writeSortedGts(tmpGtsPath, tmpSortedGtsPath);
        mergeSortedGts(tmpSortedGtsPath, args.getOutputGts(), covReaders, denySvUuid);
      }
      // Write out feature effect records, removing those from denySvUuid.
      tmpFeatureEffectsWriter.flush();
      tmpFeatureEffectsWriter.close();
      writeFeatureEffects(tmpFesPath, denySvUuid);

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
   * Merge sorted genotypes from tmpGtsPath to outputGtsPath.
   *
   * <p>Build deny set of SV UUIDs denySvUuid on demand.
   */
  private void mergeSortedGts(
      Path tmpGtsPath,
      String outputGtsPath,
      Map<String, CoverageFromMaelstromReader> covReaders,
      Set<String> denySvUuid)
      throws IOException {
    InputStream in = new FileInputStream(tmpGtsPath.toFile());
    BufferedReader fbr = new BufferedReader(new InputStreamReader(in, Charset.defaultCharset()));

    OutputStream outStream = Files.newOutputStream(Paths.get(outputGtsPath));
    Writer writer = GzipUtil.maybeOpenGzipOutputStream(outStream, args.getOutputGts());

    List<GenotypeRecord> chromRecords = new ArrayList<>();
    List<String> header = null;
    String prevContig = null;
    String line;
    while ((line = fbr.readLine()) != null) {
      final GenotypeRecord gtRecord;
      if (header == null) {
        header = Arrays.asList(line.split("\t"));
        writer.write(line);
        writer.write('\n');
        continue;
      }
      gtRecord = GenotypeRecord.fromTsv(Arrays.asList(line.split("\t")), header);
      chromRecords.add(gtRecord);
      if (!gtRecord.getChromosome().equals(prevContig)) {
        mergeChromRecords(chromRecords, denySvUuid, covReaders);
        chromRecords.sort(new GenotypeRecord.Compare());
        writeRecords(chromRecords, writer);
        chromRecords.clear();
      }
      prevContig = gtRecord.getChromosome();
    }

    if (prevContig != null) {
      mergeChromRecords(chromRecords, denySvUuid, covReaders);
      chromRecords.sort(new GenotypeRecord.Compare());
      writeRecords(chromRecords, writer);
    }
  }

  /** Helper class to store record with its index. */
  private static class GenotypeRecordWithIndex {
    private GenotypeRecord record;
    private int index;

    public GenotypeRecordWithIndex(GenotypeRecord record, int index) {
      this.record = record;
      this.index = index;
    }

    public GenotypeRecord getRecord() {
      return record;
    }

    public void setRecord(GenotypeRecord record) {
      this.record = record;
    }

    public int getIndex() {
      return index;
    }

    public void setIndex(int index) {
      this.index = index;
    }

    @Override
    public String toString() {
      return "GenotypeRecordWithIndex{" + "record=" + record + ", index=" + index + '}';
    }
  }

  /** Perform the merging of the records on the chromosome. */
  private void mergeChromRecords(
      List<GenotypeRecord> chromRecords,
      Set<String> denySvUuid,
      Map<String, CoverageFromMaelstromReader> covReaders) {
    // 1 First pass: assign to record per input file.
    // 1.1 Initialize
    final List<List<GenotypeRecord>> byCaller = new ArrayList<>();
    final Map<String, Integer> callerToIndex = new TreeMap<>();
    final List<String> indexToCaller = new ArrayList<>();
    CallerSupportFactory factory = new CallerSupportFactory(covReaders);
    for (String inputVcf : args.getInputVcf()) {
      try (VCFFileReader vcfReader = new VCFFileReader(new File(inputVcf))) {
        final CallerSupport callerSupport = factory.getFor(new File(inputVcf));
        final int index = byCaller.size();
        final String caller = callerSupport.getSvMethod(vcfReader);
        callerToIndex.put(caller, index);
        byCaller.add(new ArrayList<>());
        indexToCaller.add(caller);
      }
    }
    // 1.2 Collect by caller
    for (GenotypeRecord record : chromRecords) {
      byCaller.get(callerToIndex.get(record.getCaller())).add(record);
    }

    // Interlude: clear chromRecords, will write out there.
    chromRecords.clear();

    // Second pass: iterate over the record, stratified by caller, updating IntervalTree, and
    // denySvUuid.
    final IntervalTree<GenotypeRecordWithIndex> intervalTree = new IntervalTree();
    for (int callerIndex = 0; callerIndex < byCaller.size(); callerIndex++) {
      for (GenotypeRecord record : byCaller.get(callerIndex)) {
        if (record.getSvType().equals("BND") || record.getSvType().equals("INS")) {
          addNonoverlappingBndIns(chromRecords, intervalTree, denySvUuid, record);
        } else {
          addNonoverlappingLinear(chromRecords, intervalTree, denySvUuid, record);
        }
      }
    }
  }

  /**
   * Add to chromRecords if non-overlapping with any prior one for breakends (BND) and insertions
   * (INS).
   */
  private void addNonoverlappingBndIns(
      List<GenotypeRecord> chromRecords,
      IntervalTree<GenotypeRecordWithIndex> intervalTree,
      Set<String> denySvUuid,
      GenotypeRecord record) {
    final int radius = args.getMergeBndRadius();
    final OverlapCandidate<Integer> bestCandidate = new OverlapCandidate<>(-1, 0);
    final Iterator<IntervalTree.Node<GenotypeRecordWithIndex>> overlappers =
        intervalTree.overlappers(record.getStart() - radius, record.getStart() + radius);
    for (Iterator<IntervalTree.Node<GenotypeRecordWithIndex>> it = overlappers; it.hasNext(); ) {
      final IntervalTree.Node<GenotypeRecordWithIndex> node = it.next();
      final GenotypeRecord otherRecord = node.getValue().getRecord();
      // For BND/INS, type and paired-end orientation must be compatible and the second positions
      // must be compatible. The type and PE orientation is checked in the if-clause.  We check
      // for compatibility below and within the body we check overlap of the second point.
      if (otherRecord.getSvType().equals(record.getSvType())
          && otherRecord.getPeOrientation().equals(record.getPeOrientation())) {
        final int startA = record.getEnd() - radius;
        final int endA = record.getEnd() + radius;
        final int startB = otherRecord.getEnd() - radius;
        final int endB = otherRecord.getEnd() + radius;
        if (record.getChromosomeNo2() != otherRecord.getChromosomeNo2()
            || !((startA < startB) && (endA > endB))) {
          continue;
        }

        final int overlapStart =
            Math.max(record.getStart() - radius, otherRecord.getStart() - radius);
        final int overlapEnd = Math.min(record.getEnd() + radius, otherRecord.getEnd() + radius);
        final int overlapLength = overlapEnd - overlapStart + 1;
        if (overlapLength > bestCandidate.getOverlap()) {
          bestCandidate.update(node.getValue().getIndex(), overlapLength);
        }
      }
    }
    if (bestCandidate.getIndex() > -1) {
      // This record has an overlap and will not be written out.  We will note down the overlap
      // by adding the second method there as well (except if it was found twice by the same
      // caller).
      final GenotypeRecordBuilder builder = new GenotypeRecordBuilder();
      builder.init(chromRecords.get(bestCandidate.getIndex()));
      if (!builder.getCaller().contains(record.getCaller())) {
        builder.setCaller(builder.getCaller() + ";" + record.getCaller());
      }
      chromRecords.set(bestCandidate.getIndex(), builder.build());
      denySvUuid.add(record.getSvUuid());
    } else {
      // This record has no overlap and will be written out.
      final int index = chromRecords.size();
      chromRecords.add(record);
      intervalTree.put(
          record.getStart() - radius,
          record.getStart() + radius,
          new GenotypeRecordWithIndex(record, index));
    }
  }

  /** Add to chromRecords if non-overlapping with any prior one (linear, non-BND/non-INS case). */
  private void addNonoverlappingLinear(
      List<GenotypeRecord> chromRecords,
      IntervalTree<GenotypeRecordWithIndex> intervalTree,
      Set<String> denySvUuid,
      GenotypeRecord record) {
    final OverlapCandidate<Double> bestCandidate = new OverlapCandidate<>(-1, 0.0);
    final Iterator<IntervalTree.Node<GenotypeRecordWithIndex>> overlappers =
        intervalTree.overlappers(record.getStart() - 1, record.getEnd());
    for (Iterator<IntervalTree.Node<GenotypeRecordWithIndex>> it = overlappers; it.hasNext(); ) {
      final IntervalTree.Node<GenotypeRecordWithIndex> node = it.next();
      final GenotypeRecord otherRecord = node.getValue().getRecord();
      if (otherRecord.getSvType().equals(record.getSvType())) {
        final int overlapStart = Math.max(record.getStart(), otherRecord.getStart());
        final int overlapEnd = Math.min(record.getEnd(), otherRecord.getEnd());
        final int overlapLength = overlapEnd - overlapStart + 1;
        final double overlap1 =
            ((double) overlapLength) / (record.getEnd() - record.getStart() + 1);
        final double overlap2 =
            ((double) overlapLength) / (otherRecord.getEnd() - otherRecord.getStart() + 1);
        final double recOverlap = Math.min(overlap1, overlap2);
        if (recOverlap > bestCandidate.getOverlap()) {
          bestCandidate.update(node.getValue().getIndex(), recOverlap);
        }
      }
    }
    if (bestCandidate.getOverlap() >= args.getMergeOverlap()) {
      // This record has an overlap and will not be written out.  We will note down the overlap
      // by adding the second method there as well (except if it was found twice by the same
      // caller).
      final GenotypeRecordBuilder builder = new GenotypeRecordBuilder();
      builder.init(chromRecords.get(bestCandidate.getIndex()));
      if (!builder.getCaller().contains(record.getCaller())) {
        builder.setCaller(builder.getCaller() + ";" + record.getCaller());
      }
      chromRecords.set(bestCandidate.getIndex(), builder.build());
      denySvUuid.add(record.getSvUuid());
    } else {
      // This record has no overlap and will be written out.
      final int index = chromRecords.size();
      chromRecords.add(record);
      intervalTree.put(
          record.getStart() - 1, record.getEnd(), new GenotypeRecordWithIndex(record, index));
    }
  }

  private static class OverlapCandidate<TOverlap> {
    private int index = -1;
    private TOverlap overlap;

    public OverlapCandidate(int index, TOverlap overlap) {
      this.index = index;
      this.overlap = overlap;
    }

    public void update(int bestIndex, TOverlap bestOverlap) {
      this.index = bestIndex;
      this.overlap = bestOverlap;
    }

    public int getIndex() {
      return index;
    }

    public TOverlap getOverlap() {
      return overlap;
    }
  }

  private void writeRecords(List<GenotypeRecord> chromRecords, Writer writer) throws IOException {
    for (GenotypeRecord record : chromRecords) {
      writer.write(
          record.toTsv(
              !args.getOptOutFeatures().contains(GtRecordBuilder.FEATURE_CHROM2_COLUMNS),
              !args.getOptOutFeatures().contains(GtRecordBuilder.FEATURE_DBCOUNTS_COLUMNS)));
      writer.write('\n');
    }
  }

  /** Write out feature effects record from tmpFesPath if their UUID is not in denySvUuuid. */
  private void writeFeatureEffects(Path tmpFesPath, Set<String> denySvUuid) throws IOException {
    try (OutputStream outStream = Files.newOutputStream(Paths.get(args.getOutputFeatureEffects()));
        Writer writer =
            GzipUtil.maybeOpenGzipOutputStream(outStream, args.getOutputFeatureEffects());
        InputStream in = new FileInputStream(tmpFesPath.toFile());
        BufferedReader fbr =
            new BufferedReader(new InputStreamReader(in, Charset.defaultCharset())); ) {
      String line;
      while ((line = fbr.readLine()) != null) {
        final String[] arr = line.split("\t");
        final String svUuid = arr[2];
        if (!denySvUuid.contains(svUuid)) {
          writer.write(line);
          writer.write('\n');
        }
      }
    }
  }

  /** Write out sorted files, ready for merging (if necessary). */
  private void writeSortedGts(Path tmpGtsPath, Path toPath) throws IOException {
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
    try (OutputStream gtsStream = Files.newOutputStream(toPath);
        Writer gtsWriter = GzipUtil.maybeOpenGzipOutputStream(gtsStream, toPath.toString());
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
   * @param writeHeader Whether or not to write out header.
   * @throws VarfishAnnotatorException in case of problems
   */
  private void annotateSvVcf(
      GenomeVersion genomeVersion,
      VCFFileReader reader,
      JannovarData refseqJv,
      JannovarData ensemblJv,
      CallerSupport callerSupport,
      Writer gtWriter,
      Writer featureEffectsWriter,
      boolean writeHeader)
      throws VarfishAnnotatorException {
    // Get list of filter values to skip.
    final ImmutableSet skipFilters = ImmutableSet.copyOf(args.getSkipFilters().split(","));

    final String defaultSvMethod;
    if (callerSupport.getSvCaller() == SvCaller.GENERIC && !args.getDefaultSvMethod().equals(".")) {
      defaultSvMethod = args.getDefaultSvMethod();
    } else {
      defaultSvMethod = callerSupport.getSvMethod(reader);
    }

    // Helpers for building record for `.gts.tsv` and `.feature-effects.tsv` records.
    final GtRecordBuilder gtRecordBuilder =
        new GtRecordBuilder(
            args.getRelease(),
            defaultSvMethod,
            args.getOptOutFeatures(),
            args.getCaseId(),
            args.getSetId(),
            pedigree,
            callerSupport);
    final FeRecordBuilder feRecordBuilder = new FeRecordBuilder(args.getCaseId(), args.getSetId());

    if (writeHeader) {
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
      FeRecordBuilder feRecordBuilder,
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
      FeRecordBuilder feRecordBuilder,
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
      final FeRecordBuilder.Result feResult =
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
