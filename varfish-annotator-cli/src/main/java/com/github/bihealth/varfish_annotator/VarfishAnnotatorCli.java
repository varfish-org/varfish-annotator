package com.github.bihealth.varfish_annotator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.MissingCommandException;
import com.github.bihealth.varfish_annotator.annotate.AnnotateArgs;
import com.github.bihealth.varfish_annotator.annotate.AnnotateVcf;
import com.github.bihealth.varfish_annotator.annotate_svs.AnnotateSvsArgs;
import com.github.bihealth.varfish_annotator.annotate_svs.AnnotateSvsVcf;
import com.github.bihealth.varfish_annotator.dbstats.DbStats;
import com.github.bihealth.varfish_annotator.dbstats.DbStatsArgs;
import com.github.bihealth.varfish_annotator.init_db.InitDb;
import com.github.bihealth.varfish_annotator.init_db.InitDbArgs;

public class VarfishAnnotatorCli {

  public static void main(String[] args) {
    final InitDbArgs initDb = new InitDbArgs();
    final AnnotateArgs annotate = new AnnotateArgs();
    final AnnotateSvsArgs annotateSvs = new AnnotateSvsArgs();
    final DbStatsArgs dbStats = new DbStatsArgs();

    final JCommander jc =
        JCommander.newBuilder()
            .addCommand("init-db", initDb)
            .addCommand("annotate", annotate)
            .addCommand("annotate-svs", annotateSvs)
            .addCommand("db-stats", dbStats)
            .build();

    if ((args == null || args.length == 0)) {
      jc.usage();
      System.exit(1);
    }

    try {
      jc.parse(args);
    } catch (MissingCommandException e) {
      System.exit(1);
      return;
    }

    final String cmd = jc.getParsedCommand();
    if (cmd == null) {
      jc.usage();
      System.exit(1);
    }

    switch (cmd) {
      case "init-db":
        if (initDb.isHelp()) {
          jc.usage("init-db");
        } else {
          new InitDb(initDb).run();
        }
        break;
      case "annotate":
        System.err.println("annotate: " + annotate);
        if (annotate.isHelp()) {
          jc.usage("annotate");
        } else {
          new AnnotateVcf(annotate).run();
        }
        break;
      case "annotate-svs":
        System.err.println("annotate-svs: " + annotateSvs);
        if (annotate.isHelp()) {
          jc.usage("annotate-svs");
        } else {
          new AnnotateSvsVcf(annotateSvs).run();
        }
        break;
      case "db-stats":
        System.err.println("db-stats: " + dbStats);
        if (dbStats.isHelp()) {
          jc.usage("db-stats");
        } else {
          new DbStats(dbStats).run();
        }
      default:
        System.err.println("Unknown command: " + cmd);
        System.exit(1);
    }
  }
}
