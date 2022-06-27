package com.github.bihealth.varfish_annotator;

import com.beust.jcommander.ParameterException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.jupiter.api.Assertions;

/**
 * Smoke test for varfish-annotator
 *
 * @author <a href="mailto:manuel.holtgrewe@bih-charite.de">Manuel Holtgrewe</a>
 */
public class SmokeTest {

  @Rule public final ExpectedSystemExit exit = ExpectedSystemExit.none();

  @Test
  public void testCallNonexistingCommand() {
    exit.expectSystemExitWithStatus(1);
    VarfishAnnotatorCli.main(new String[] {"i-dont-exist"});
  }

  @Test
  public void testCallAnnotate() {
    Assertions.assertThrows(
        ParameterException.class,
        () -> {
          VarfishAnnotatorCli.main(new String[] {"annotate"});
        });
  }

  @Test
  public void testCallAnnotateSvs() {
    Assertions.assertThrows(
        ParameterException.class,
        () -> {
          VarfishAnnotatorCli.main(new String[] {"annotate-svs"});
        });
  }

  @Test
  public void testCallDbStats() {
    Assertions.assertThrows(
        ParameterException.class,
        () -> {
          VarfishAnnotatorCli.main(new String[] {"db-stats"});
        });
  }

  @Test
  public void testCallInitDb() {
    Assertions.assertThrows(
        ParameterException.class,
        () -> {
          VarfishAnnotatorCli.main(new String[] {"init-db"});
        });
  }
}
