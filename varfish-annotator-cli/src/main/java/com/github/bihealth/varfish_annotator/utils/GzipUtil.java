package com.github.bihealth.varfish_annotator.utils;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipUtil {

  /**
   * Checks if an input stream is gzipped.
   *
   * @param in
   * @return
   */
  public static boolean isGZipped(InputStream in) {
    if (!in.markSupported()) {
      in = new BufferedInputStream(in);
    }
    in.mark(2);
    int magic = 0;
    try {
      magic = in.read() & 0xff | ((in.read() << 8) & 0xff00);
      in.reset();
    } catch (IOException e) {
      e.printStackTrace(System.err);
      return false;
    }
    return magic == GZIPInputStream.GZIP_MAGIC;
  }

  /** Open GZipOutputStream if file name ends with <code>".gz"</code>. */
  public static OutputStreamWriter maybeOpenGzipOutputStream(OutputStream os, String fileName)
      throws IOException {
    if (fileName.endsWith(".gz")) {
      return new OutputStreamWriter(new GZIPOutputStream(os), "UTF-8");
    } else {
      return new OutputStreamWriter(os);
    }
  }
}
