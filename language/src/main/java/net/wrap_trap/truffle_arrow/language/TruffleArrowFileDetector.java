package net.wrap_trap.truffle_arrow.language;

import com.oracle.truffle.api.TruffleFile;

import java.io.IOException;
import java.nio.charset.Charset;

public class TruffleArrowFileDetector implements TruffleFile.FileTypeDetector {
  @Override
  public String findMimeType(TruffleFile file) throws IOException {
    String name = file.getName();
    if (name != null && name.endsWith(".sl")) {
      return TruffleArrowLanguage.MIME_TYPE;
    }
    return null;
  }

  @Override
  public Charset findEncoding(TruffleFile file) throws IOException {
    return null;
  }
}
