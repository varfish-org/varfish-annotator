package com.github.bihealth.varfish_annotator.init_db;

import com.github.bihealth.varfish_annotator.VarfishAnnotatorException;
import com.github.bihealth.varfish_annotator.utils.VariantDescription;
import com.github.bihealth.varfish_annotator.utils.VariantNormalizer;
import com.google.common.collect.ImmutableList;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/** Base class for gnomAD import. */
abstract class GnomadImporter {

  protected abstract String getTableName();

  protected abstract String getFieldPrefix();

  /** The JDBC connection. */
  protected final Connection conn;

  /** Path to gnomAD VCF path. */
  protected final List<String> gnomadVcfPaths;

  /** Helper to use for variant normalization. */
  protected final String refFastaPath;

  /** Chromosome of selected region. */
  private final String chrom;

  /** 1-based start position of selected region. */
  private final int start;

  /** 1-based end position of selected region. */
  private final int end;

  /**
   * Construct the <tt>GnomadImporter</tt> object.
   *
   * @param conn Connection to database
   * @param gnomadVcfPaths Path to gnomAD VCF path.
   * @param genomicRegion Genomic regiin {@code CHR:START-END} to process.
   */
  GnomadImporter(
      Connection conn, List<String> gnomadVcfPaths, String refFastaPath, String genomicRegion) {
    this.conn = conn;
    this.gnomadVcfPaths = gnomadVcfPaths;
    this.refFastaPath = refFastaPath;

    if (genomicRegion == null) {
      this.chrom = null;
      this.start = -1;
      this.end = -1;
    } else {
      this.chrom = genomicRegion.split(":", 2)[0];
      this.start = Integer.parseInt(genomicRegion.split(":", 2)[1].split("-")[0].replace(",", ""));
      this.end = Integer.parseInt(genomicRegion.split(":", 2)[1].split("-")[1].replace(",", ""));
    }
  }

  /** Execute gnomAD import. */
  public void run() throws VarfishAnnotatorException {
    System.err.println("Re-creating table in database...");
    recreateTable();

    System.err.println("Importing gnomAD...");
    final VariantNormalizer normalizer = new VariantNormalizer(refFastaPath);
    String prevChr = null;
    for (String gnomadVcfPath : gnomadVcfPaths) {
      try (VCFFileReader reader = new VCFFileReader(new File(gnomadVcfPath), true)) {
        final CloseableIterator<VariantContext> it;
        if (this.chrom != null) {
          it = reader.query(this.chrom, this.start, this.end);
        } else {
          it = reader.iterator();
        }

        while (it.hasNext()) {
          final VariantContext ctx = it.next();
          if (!ctx.getContig().equals(prevChr)) {
            System.err.println("Now on chrom " + ctx.getContig());
          }
          importVariantContext(normalizer, ctx);
          prevChr = ctx.getContig();
        }
      } catch (SQLException e) {
        throw new VarfishAnnotatorException(
            "Problem with inserting into " + getTableName() + " table", e);
      }
    }

    System.err.println("Done with importing gnomAD...");
  }

  /**
   * Re-create the gnomAD table in the database.
   *
   * <p>After calling this method, the table has been created and is empty.
   */
  private void recreateTable() throws VarfishAnnotatorException {
    final String dropQuery = "DROP TABLE IF EXISTS " + getTableName();
    try (PreparedStatement stmt = conn.prepareStatement(dropQuery)) {
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem with DROP TABLE statement", e);
    }

    final String createQuery =
        "CREATE TABLE "
            + getTableName()
            + "("
            + "release VARCHAR(10) NOT NULL, "
            + "chrom VARCHAR(20) NOT NULL, "
            + "start INTEGER NOT NULL, "
            + "end INTEGER NOT NULL, "
            + "ref VARCHAR("
            + InitDb.VARCHAR_LEN
            + ") NOT NULL, "
            + "alt VARCHAR("
            + InitDb.VARCHAR_LEN
            + ") NOT NULL, "
            + getFieldPrefix()
            + "_het INTEGER NOT NULL, "
            + getFieldPrefix()
            + "_hom INTEGER NOT NULL, "
            + getFieldPrefix()
            + "_hemi INTEGER NOT NULL, "
            + getFieldPrefix()
            + "_af DOUBLE NOT NULL, "
            + ")";
    try (PreparedStatement stmt = conn.prepareStatement(createQuery)) {
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem with CREATE TABLE statement", e);
    }

    final ImmutableList<String> indexQueries =
        ImmutableList.of(
            "CREATE PRIMARY KEY ON " + getTableName() + " (release, chrom, start, ref, alt)",
            "CREATE INDEX ON " + getTableName() + " (release, chrom, start, end)");
    for (String query : indexQueries) {
      try (PreparedStatement stmt = conn.prepareStatement(query)) {
        stmt.executeUpdate();
      } catch (SQLException e) {
        throw new VarfishAnnotatorException("Problem with CREATE INDEX statement", e);
      }
    }
  }

  /** Insert the data from <tt>ctx</tt> into the database. */
  @SuppressWarnings("unchecked")
  private void importVariantContext(VariantNormalizer normalizer, VariantContext ctx)
      throws SQLException {
    final String insertQuery =
        "MERGE INTO "
            + getTableName()
            + " (release, chrom, start, end, ref, alt, "
            + getFieldPrefix()
            + "_het, "
            + getFieldPrefix()
            + "_hom, "
            + getFieldPrefix()
            + "_hemi, "
            + getFieldPrefix()
            + "_af) VALUES ('GRCh37', ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    final int numAlleles = ctx.getAlleles().size();
    for (int i = 1; i < numAlleles; ++i) {
      final VariantDescription rawVariant =
          new VariantDescription(
              ctx.getContig(),
              ctx.getStart() - 1,
              ctx.getReference().getBaseString(),
              ctx.getAlleles().get(i).getBaseString());
      final VariantDescription finalVariant = normalizer.normalizeInsertion(rawVariant);

      final PreparedStatement stmt = conn.prepareStatement(insertQuery);
      stmt.setString(1, finalVariant.getChrom());
      stmt.setInt(2, finalVariant.getPos() + 1);
      stmt.setInt(3, finalVariant.getPos() + finalVariant.getRef().length());
      stmt.setString(4, finalVariant.getRef());
      stmt.setString(5, finalVariant.getAlt());

      // Compute het., hom. alt, hemi. alt. counts.
      final int countHemi;
      final int countHomAlt;
      if (ctx.getCommonInfo().hasAttribute("nonpar")) {
        // Male is hemizygous.
        countHemi = ctx.getCommonInfo().getAttributeAsInt("AC_male", 0);
        countHomAlt = ctx.getCommonInfo().getAttributeAsInt("nhomalt", 0);
      } else {
        // Male is homozygous.
        countHemi = 0;
        countHomAlt = ctx.getCommonInfo().getAttributeAsInt("nhomalt", 0);
      }
      stmt.setInt(7, countHomAlt);
      stmt.setInt(8, countHemi);

      final int countHet =
          ctx.getCommonInfo().getAttributeAsInt("AC", 0) - countHemi - 2 * countHomAlt;
      stmt.setInt(6, countHet);

      stmt.setDouble(9, ctx.getCommonInfo().getAttributeAsDouble("AF", 0.0));

      stmt.executeUpdate();
      stmt.close();
    }
  }
}
