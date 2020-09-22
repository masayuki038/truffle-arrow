package net.wrap_trap.truffle_arrow.storage.columnar;

import net.wrap_trap.truffle_arrow.ArrowSchema;
import org.apache.calcite.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ArrowColumnarSchema extends ArrowSchema {

  private static final Logger log = LoggerFactory.getLogger(ArrowColumnarSchema.class);

  public ArrowColumnarSchema(File directory) {
    super(directory);
  }

  @Override
  public Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = new HashMap<>();
      File[] tableDirs = directory.listFiles(f -> f.isDirectory());
      Arrays.stream(tableDirs).forEach(dir -> {
        tableMap.put(
          dir.getName().toUpperCase(),
          new ArrowColumnarTable(dir, null));
      });
    }
    return tableMap;
  }
}
