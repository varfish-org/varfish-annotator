package com.github.bihealth.varhab.init_db;

import com.github.bihealth.varhab.VarhabException;
import com.google.common.collect.ImmutableList;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of ExAC import.
 *
 * <p>The code will also normalize the ExAC data per-variant.
 */
public final class ExacImporter {

  /** The name of the table in the database. */
  public static final String TABLE_NAME = "exac_var";

  /** The population names. */
  public static final ImmutableList<String> popNames =
      ImmutableList.of("AFR", "AMR", "EAS", "FIN", "NFE", "OTH", "SAS");

  /** The JDBC connection. */
  private final Connection conn;

  /** Path to ExAC VCF path. */
  private final String exacVcfPath;

  /** Helper to use for variant normalization. */
  private final String refFastaPath;

  /**
   * Construct the <tt>ExacImporter</tt> object.
   *
   * @param conn Connection to database
   * @param exacVcfPath Path to ExAC VCF path.
   */
  public ExacImporter(Connection conn, String exacVcfPath, String refFastaPath) {
    this.conn = conn;
    this.exacVcfPath = exacVcfPath;
    this.refFastaPath = refFastaPath;
  }

  /** Execute ExAC import. */
  public void run() throws VarhabException {
    System.err.println("Re-creating table in database...");
    recreateTable();

    System.err.println("Importing ExAC...");
    final VariantNormalizer normalizer = new VariantNormalizer(refFastaPath);
    try (VCFFileReader reader = new VCFFileReader(new File(exacVcfPath), true)) {
      for (VariantContext ctx : reader) {
        importVariantContext(normalizer, ctx);
      }
    } catch (SQLException e) {
      throw new VarhabException("Problem with inserting into exac_vars table", e);
    }

    System.err.println("Done with importing ExAC...");
  }

  /**
   * Re-create the ExAC table in the database.
   *
   * <p>After calling this method, the table has been created and is empty.
   */
  private void recreateTable() throws VarhabException {
    final String dropQuery = "DROP TABLE IF EXISTS " + TABLE_NAME;
    try (PreparedStatement stmt = conn.prepareStatement(dropQuery)) {
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new VarhabException("Problem with DROP TABLE statement", e);
    }

    final String createQuery =
        "CREATE TABLE "
            + TABLE_NAME
            + "("
            + "chrom VARCHAR(20) NOT NULL, "
            + "pos INTEGER NOT NULL, "
            + "pos_end INTEGER NOT NULL, "
            + "ref VARCHAR(100) NOT NULL, "
            + "alt VARCHAR(100) NOT NULL, "
            + "exac_hom INTEGER NOT NULL, "
            + "exac_af_popmax DOUBLE NOT NULL, "
            + ")";
    try (PreparedStatement stmt = conn.prepareStatement(createQuery)) {
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new VarhabException("Problem with CREATE TABLE statement", e);
    }

    final ImmutableList<String> indexQueries =
        ImmutableList.of(
            "CREATE INDEX ON " + TABLE_NAME + "(chrom, pos, ref, alt)",
            "CREATE INDEX ON " + TABLE_NAME + "(chrom, pos, pos_end)");
    for (String query : indexQueries) {
      try (PreparedStatement stmt = conn.prepareStatement(query)) {
        stmt.executeUpdate();
      } catch (SQLException e) {
        throw new VarhabException("Problem with CREATE INDEX statement", e);
      }
    }
  }

  /** Insert the data from <tt>ctx</tt> into the database. */
  @SuppressWarnings("unchecked")
  private void importVariantContext(VariantNormalizer normalizer, VariantContext ctx)
      throws SQLException {
    final String insertQuery =
        "INSERT INTO "
            + TABLE_NAME
            + " (chrom, pos, pos_end, ref, alt, exac_hom, exac_af_popmax)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?)";

    final int numAlleles = ctx.getAlleles().size();
    for (int i = 1; i < numAlleles; ++i) {
      final VariantDescription rawVariant =
          new VariantDescription(
              ctx.getChr(),
              ctx.getStart() - 1,
              ctx.getReference().getBaseString(),
              ctx.getAlleles().get(i).getBaseString());
      final VariantDescription normVariant = normalizer.normalizeVariant(rawVariant);
      final VariantDescription finalVariant;
      if (normVariant.getRef().isEmpty()) {
        finalVariant = normalizer.normalizeInsertion(rawVariant);
      } else {
        finalVariant = normVariant;
      }

      final PreparedStatement stmt = conn.prepareStatement(insertQuery);
      stmt.setString(1, finalVariant.getChrom());
      stmt.setInt(2, finalVariant.getPos());
      stmt.setInt(3, finalVariant.getEnd());
      stmt.setString(4, finalVariant.getRef());
      stmt.setString(5, finalVariant.getAlt());

      int exacHom = 0;
      for (String pop : popNames) {
        final List<Integer> homs;
        if (numAlleles == 2) {
          homs = ImmutableList.of(ctx.getCommonInfo().getAttributeAsInt("AC_Hom", 0));
        } else {
          homs = new ArrayList<>();
          for (String s :
              (List<String>)
                  ctx.getCommonInfo().getAttribute("AC_Hom", ImmutableList.<String>of())) {
            homs.add(Integer.parseInt(s));
          }
        }
        if (homs.size() >= i) {
          exacHom = homs.get(i - 1);
        } else {
          System.err.println("Warning, could not update AC_Hom for " + ctx);
        }
      }
      stmt.setInt(6, exacHom);

      double exacAlleleFreqPopMax = 0.0;
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
          exacAlleleFreqPopMax =
              Math.max(exacAlleleFreqPopMax, ((double) acs.get(i - 1)) / ((double) an));
        } else if (an > 0) {
          System.err.println("Warning, could not update AF_POPMAX (" + pop + ") for " + ctx);
        }
      }
      stmt.setDouble(7, exacAlleleFreqPopMax);

      stmt.executeUpdate();
      stmt.close();
    }
  }
}
