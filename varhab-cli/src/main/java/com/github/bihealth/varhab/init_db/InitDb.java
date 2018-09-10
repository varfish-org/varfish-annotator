package com.github.bihealth.varhab.init_db;

import com.github.bihealth.varhab.VarhabException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Implementation of the <tt>init-db</tt> command. */
public final class InitDb {

  public static final int VARCHAR_LEN = 1000;

  /** Configuration for the command. */
  private final InitDbArgs args;

  /** Construct with the given configuration. */
  public InitDb(InitDbArgs args) {
    this.args = args;
  }

  /** Execute the command. */
  public void run() {
    System.err.println("Running init-db; args: " + args);

    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:h2:" + args.getDbPath() + ";MV_STORE=FALSE;MVCC=FALSE", "sa", "")) {
      if (args.getGnomadExomesPath() != null) {
        System.err.println("Importing gnomAD VCF files...");
        new GnomadExomesImporter(conn, args.getGnomadExomesPath(), args.getRefPath()).run();
      }
      if (args.getGnomadGenomesPath() != null) {
        System.err.println("Importing gnomAD VCF files...");
        new GnomadGenomesImporter(conn, args.getGnomadGenomesPath(), args.getRefPath()).run();
      }
      if (args.getThousandGenomesPaths() != null && args.getThousandGenomesPaths().size() > 0) {
        System.err.println("Importing 1000 Genomes VCF files...");
        new ThousandGenomesImporter(conn, args.getThousandGenomesPaths(), args.getRefPath()).run();
      }
      if (args.getExacPath() != null) {
        System.err.println("Importing ExAC VCF files...");
        new ExacImporter(conn, args.getExacPath(), args.getRefPath()).run();
      }
      if (args.getClinvarPaths() != null && args.getClinvarPaths().size() > 0) {
        System.err.println("Importing Clinvar TSV files...");
        new ClinvarImporter(conn, args.getClinvarPaths()).run();
      }
    } catch (SQLException e) {
      System.err.println("Problem with database conection");
      e.printStackTrace();
      System.exit(1);
    } catch (VarhabException e) {
      System.err.println("Problem executing init-db");
      e.printStackTrace();
      System.exit(1);
    }
  }
}
