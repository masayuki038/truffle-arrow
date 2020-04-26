package net.wrap_trap.truffle_arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Schema for Apache Arrow
 */
public class ArrowSchema extends AbstractSchema {

  private Map<String, Table> tableMap;
  private File directory;

  public ArrowSchema(File directory) {
    this.directory = directory;
  }

  private String trim(String s, String suffix) {
    String trimmed = trimOrNull(s, suffix);
    if (trimmed == null) {
      return s;
    }
    return trimmed;
  }

  private String trimOrNull(String s, String suffix) {
    if (s.endsWith(suffix)) {
      return s.substring(0, s.length() - suffix.length());
    }
    return null;
  }

  public Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = new HashMap<>();
      File[] arrowFiles = directory.listFiles((dir, name) -> name.endsWith(".arrow"));
      Arrays.stream(arrowFiles).forEach(file -> {
        try {
          VectorSchemaRoot[] vectorSchemaRoots = ArrowUtils.load(file.getAbsolutePath());
          UInt4Vector selectionVector = ArrowUtils.createSelectionVector();
          tableMap.put(
            trim(file.getName(), ".arrow").toUpperCase(),
            new ArrowTable(vectorSchemaRoots, selectionVector, null));
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      });
    }
    return tableMap;
  }
}
