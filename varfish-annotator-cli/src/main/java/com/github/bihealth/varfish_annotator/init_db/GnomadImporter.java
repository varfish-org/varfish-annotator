package com.github.bihealth.varfish_annotator.init_db;

import com.github.bihealth.varfish_annotator.VarfishAnnotatorException;
import com.github.bihealth.varfish_annotator.utils.VariantDescription;
import com.github.bihealth.varfish_annotator.utils.VariantNormalizer;
import com.google.common.collect.ImmutableList;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Base class for gnomAD import. */
abstract class GnomadImporter {

  protected abstract String getTableName();

  protected abstract String getFieldPrefix();

  public static final ImmutableList<String> popNames =
      ImmutableList.of("AFR", "AMR", "ASJ", "EAS", "FIN", "NFE", "OTH", "SAS");

  /** The JDBC connection. */
  protected final Connection conn;

  /** Path to gnomAD VCF path. */
  protected final String gnomadVcfPath;

  /** Helper to use for variant normalization. */
  protected final String refFastaPath;

  /**
   * Construct the <tt>GnomadImporter</tt> object.
   *
   * @param conn Connection to database
   * @param gnomadVcfPath Path to gnomAD VCF path.
   */
  GnomadImporter(Connection conn, String gnomadVcfPath, String refFastaPath) {
    this.conn = conn;
    this.gnomadVcfPath = gnomadVcfPath;
    this.refFastaPath = refFastaPath;
  }

  /** Execute gnomAD import. */
  public void run() throws VarfishAnnotatorException {
    System.err.println("Re-creating table in database...");
    recreateTable();

    System.err.println("Importing gnomAD...");
    final VariantNormalizer normalizer = new VariantNormalizer(refFastaPath);
    String prevChr = null;
    try (VCFFileReader reader = new VCFFileReader(new File(gnomadVcfPath), true)) {
      for (VariantContext ctx : reader) {
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
            + "pos INTEGER NOT NULL, "
            + "pos_end INTEGER NOT NULL, "
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
            + "_af_popmax DOUBLE NOT NULL, "
            + ")";
    try (PreparedStatement stmt = conn.prepareStatement(createQuery)) {
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new VarfishAnnotatorException("Problem with CREATE TABLE statement", e);
    }

    final ImmutableList<String> indexQueries =
        ImmutableList.of(
            "CREATE PRIMARY KEY ON " + getTableName() + " (release, chrom, pos, ref, alt)",
            "CREATE INDEX ON " + getTableName() + " (release, chrom, pos, pos_end)");
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
            + " (release, chrom, pos, pos_end, ref, alt, "
            + getFieldPrefix()
            + "_het, "
            + getFieldPrefix()
            + "_hom, "
            + getFieldPrefix()
            + "_hemi, "
            + getFieldPrefix()
            + "_af_popmax) VALUES ('GRCh37', ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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

      int countHet = 0;
      List<Integer> hets;
      if (numAlleles == 2) {
        try {
          hets = ImmutableList.of(ctx.getCommonInfo().getAttributeAsInt("Het", 0));
        } catch (NumberFormatException e) {
          hets = ImmutableList.of(0);
        }
      } else {
        hets = new ArrayList<>();
        for (String s :
            (List<String>) ctx.getCommonInfo().getAttribute("Het", ImmutableList.<String>of())) {
          try {
            hets.add(Integer.parseInt(s));
          } catch (NumberFormatException e) {
            hets.add(0);
          }
        }
      }
      if (hets.size() >= i) {
        countHet = hets.get(i - 1);
      }
      stmt.setInt(6, countHet);

      int countHom = 0;
      List<Integer> homs;
      if (numAlleles == 2) {
        try {
          homs = ImmutableList.of(ctx.getCommonInfo().getAttributeAsInt("Hom", 0));
        } catch (NumberFormatException e) {
          homs = ImmutableList.of(0);
        }
      } else {
        homs = new ArrayList<>();
        for (String s :
            (List<String>) ctx.getCommonInfo().getAttribute("Hom", ImmutableList.<String>of())) {
          try {
            homs.add(Integer.parseInt(s));
          } catch (NumberFormatException e) {
            homs.add(0);
          }
        }
      }
      if (homs.size() >= i) {
        countHom = homs.get(i - 1);
      }
      stmt.setInt(7, countHom);

      int countHemi = 0;
      List<Integer> hemis;
      if (numAlleles == 2) {
        try {
          hemis = ImmutableList.of(ctx.getCommonInfo().getAttributeAsInt("Hemi", 0));
        } catch (NumberFormatException e) {
          hemis = ImmutableList.of(0);
        }
      } else {
        hemis = new ArrayList<>();
        for (String s :
            (List<String>) ctx.getCommonInfo().getAttribute("Hemi", ImmutableList.<String>of())) {
          try {
            hemis.add(Integer.parseInt(s));
          } catch (NumberFormatException e) {
            hemis.add(0);
          }
        }
      }
      if (hemis.size() >= i) {
        countHemi = homs.get(i - 1);
      }
      stmt.setInt(8, countHemi);

      double alleleFreqPopMax = 0.0;
      for (String pop : popNames) {
        final List<Integer> acs;
        if (numAlleles == 2) {
          acs = ImmutableList.of(ctx.getCommonInfo().getAttributeAsInt("AC_" + pop, 0));
        } else {
          acs = new ArrayList<>();
          for (String s :
              (List<String>)
                  ctx.getCommonInfo().getAttribute("AC_" + pop, ImmutableList.<String>of())) {
            acs.add(Integer.parseInt(s));
          }
        }
        final int an = ctx.getCommonInfo().getAttributeAsInt("AN_" + pop, 0);
        if (an > 0 && acs.size() >= i) {
          alleleFreqPopMax = Math.max(alleleFreqPopMax, ((double) acs.get(i - 1)) / ((double) an));
        } else if (an > 0) {
          System.err.println("Warning, could not update AF_POPMAX (" + pop + ") for " + ctx);
        }
      }
      stmt.setDouble(9, alleleFreqPopMax);

      stmt.executeUpdate();
      stmt.close();
    }
  }
}
