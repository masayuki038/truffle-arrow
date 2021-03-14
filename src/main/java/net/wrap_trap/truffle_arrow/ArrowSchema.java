package net.wrap_trap.truffle_arrow;

import org.apache.arrow.memory.BufferAllocator;
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
public abstract class ArrowSchema extends AbstractSchema {

  private static final Logger log = LoggerFactory.getLogger(ArrowSchema.class);

  protected Map<String, Table> tableMap;
  protected File directory;

  public ArrowSchema(File directory) {
    this.directory = directory;
    log.debug("directory: " + directory.getAbsolutePath());
  }

  protected String trim(String s, String suffix) {
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

  abstract public Map<String, Table> getTableMap();
}
