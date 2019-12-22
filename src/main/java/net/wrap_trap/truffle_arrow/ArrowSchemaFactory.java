package net.wrap_trap.truffle_arrow;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.Map;

/**
 * SchemaFactory for Apache Arrow
 */
public class ArrowSchemaFactory implements SchemaFactory {
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    String directory = (String) operand.get("directory");
    File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    File directoryFile = new File(directory);
    if (base != null && !directoryFile.isAbsolute()) {
      directoryFile = new File(base, directory);
    }
    return new ArrowSchema(directoryFile);
  }
}
