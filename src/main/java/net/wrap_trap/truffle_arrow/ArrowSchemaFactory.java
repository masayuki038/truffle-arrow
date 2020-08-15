package net.wrap_trap.truffle_arrow;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * SchemaFactory for Apache Arrow
 */
public class ArrowSchemaFactory implements SchemaFactory {

  private static final Logger log = LoggerFactory.getLogger(ArrowSchemaFactory.class);

  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    String directory = (String) operand.get("directory");
    log.debug("directory: " + directory);

    File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    File directoryFile = new File(directory);
    if (base != null && !directoryFile.isAbsolute()) {
      directoryFile = new File(base, directory);
    }
    return new ArrowSchema(directoryFile);
  }
}
