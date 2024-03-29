package com.github.bihealth.varfish_annotator;

import java.io.*;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.IOUtils;

/**
 * Helper class with static methods for handling resources in tests
 *
 * @author <a href="mailto:manuel.holtgrewe@bihealth.de">Manuel Holtgrewe</a>
 */
public class ResourceUtils {

  /** Helper function for reading resources into memory */
  public static String readResource(String path) {
    StringWriter writer = new StringWriter();
    try {
      IOUtils.copy(ResourceUtils.class.getResourceAsStream(path), writer, "UTF-8");
    } catch (IOException e) {
      throw new RuntimeException("Problem reading resource " + path, e);
    }
    return writer.toString();
  }

  /** Copy resource at the given path to the given output {@link File}. */
  public static void copyResourceToFile(String path, File outFile) {
    try (InputStream input = ResourceUtils.class.getResourceAsStream(path);
        OutputStream os = new FileOutputStream(outFile)) {
      byte[] buffer = new byte[1024];
      int length;
      while ((length = input.read(buffer)) > 0) {
        os.write(buffer, 0, length);
      }
    } catch (IOException e) {
      throw new RuntimeException("Problem with copying resource to file", e);
    }
  }

  /** gzunip resource at the given path to the given output {@link File}. */
  public static void gunzipResourceToFile(String path, File outFile) {
    try (InputStream inputRaw = ResourceUtils.class.getResourceAsStream(path);
        GZIPInputStream input = new GZIPInputStream(inputRaw);
        OutputStream os = new FileOutputStream(outFile)) {
      byte[] buffer = new byte[1024];
      int length;
      while ((length = input.read(buffer)) > 0) {
        os.write(buffer, 0, length);
      }
    } catch (IOException e) {
      throw new RuntimeException("Problem with copying resource to file", e);
    }
  }
}
