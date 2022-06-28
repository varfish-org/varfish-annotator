package com.github.bihealth.varfish_annotator;

import com.beust.jcommander.ParameterException;
import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Smoke test for varfish-annotator
 *
 * @author <a href="mailto:manuel.holtgrewe@bih-charite.de">Manuel Holtgrewe</a>
 */
public class SmokeTest {

  @Test
  @ExpectSystemExitWithStatus(1)
  public void testCallNonexistingCommand() {
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
