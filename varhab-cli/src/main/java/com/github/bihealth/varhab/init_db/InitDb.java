package com.github.bihealth.varhab.init_db;

import com.github.bihealth.varhab.CommandInitDb;
import com.github.bihealth.varhab.VarhabException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Implementation of the <tt>init-db</tt> command. */
public final class InitDb {

  /** Configuration for the command. */
  private final CommandInitDb args;

  /** Construct with the given configuration. */
  public InitDb(CommandInitDb args) {
    this.args = args;
  }

  /** Execute the command. */
  public void run() {
    System.err.println("Running init-db; args: " + args);

    try (Connection conn =
        DriverManager.getConnection(
            "jdbc:h2:" + args.getDbPath() + ";MV_STORE=FALSE;MVCC=FALSE", "sa", "")) {
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
