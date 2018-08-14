package com.github.bihealth.varhab.annotate;

import com.github.bihealth.varhab.VarhabException;
import de.charite.compbio.jannovar.data.JannovarData;
import de.charite.compbio.jannovar.data.JannovarDataSerializer;
import de.charite.compbio.jannovar.data.SerializationException;
import de.charite.compbio.jannovar.hgvs.AminoAcidCode;
import de.charite.compbio.jannovar.htsjdk.VariantContextAnnotator;
import de.charite.compbio.jannovar.htsjdk.VariantContextAnnotator.Options;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Implementation of the <tt>annotate</tt> command. */
public final class AnnotateVcf {

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
      annotateVcf(conn, reader, jannovarData, gtWriter, varWriter);
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
   * @param gtWriter Writer for variant call ("genotype") TSV file.
   * @param varWriter Writer for variant annotation ("annotation") TSV file.
   * @throws VarhabException in case of problems
   */
  private void annotateVcf(
      Connection conn,
      VCFFileReader reader,
      JannovarData jannovarData,
      FileWriter gtWriter,
      FileWriter varWriter)
      throws VarhabException {

    final VariantContextAnnotator annotator =
        new VariantContextAnnotator(
            jannovarData.getRefDict(),
            jannovarData.getChromosomes(),
            new Options(false, AminoAcidCode.ONE_LETTER, false, false, false, false, false));

    String prevChr = null;
    for (VariantContext ctx : reader) {
      if (!ctx.getChr().equals(prevChr)) {
        System.err.println("Now on chrom " + ctx.getChr());
      }
      annotateVariantContext(annotator, ctx, gtWriter, varWriter);
      prevChr = ctx.getChr();
    }
  }

  /**
   * Annotate <tt>ctx</tt>, write out annotated variant call to <tt>gtWriter</tt> and annotated
   * variant to <tt>varWriter</tt>.
   *
   * @param annotator Helper class to use for annotation of variants.
   * @param ctx The variant to annotate.
   * @param gtWriter Writer for annotated genotypes.
   * @param varWriter Writer for variants.
   * @throws VarhabException in case of problems
   */
  private void annotateVariantContext(
      VariantContextAnnotator annotator,
      VariantContext ctx,
      FileWriter gtWriter,
      FileWriter varWriter)
      throws VarhabException {
    if (false) {
      throw new VarhabException("Meh!");
    }
  }
}
