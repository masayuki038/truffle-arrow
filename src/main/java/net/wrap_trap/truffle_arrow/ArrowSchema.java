package net.wrap_trap.truffle_arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schema for Apache Arrow
 */
public class ArrowSchema extends AbstractSchema {

  private static final Logger log = LoggerFactory.getLogger(ArrowSchema.class);

  private Map<String, Table> tableMap;
  private File directory;

  public ArrowSchema(File directory) {
    this.directory = directory;
    log.debug("directory: " + directory.getAbsolutePath());
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
          String path = file.getAbsolutePath();
          log.debug("Found: " + path);
          VectorSchemaRoot[] vectorSchemaRoots = ArrowUtils.load(path);
          tableMap.put(
            trim(file.getName(), ".arrow").toUpperCase(),
            new ArrowTable(vectorSchemaRoots, null));
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      });
    }
    return tableMap;
  }
}
