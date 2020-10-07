package net.wrap_trap.truffle_arrow;

import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import net.wrap_trap.truffle_arrow.storage.columnar.ArrowColumnarSchema;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class TruffleArrowConfig {

  private static final Logger log = LoggerFactory.getLogger(TruffleArrowConfig.class);

  private static final String CONFIG_ROOT_PATH = "data.root";

  public static final TruffleArrowConfig INSTANCE = new TruffleArrowConfig();

  private Prepare.CatalogReader catalogReader;
  private JavaTypeFactory typeFactory;
  private SqlValidatorImpl validator;

  private TruffleArrowConfig() {
    reload();
  }

  public Prepare.CatalogReader catalogReader() {
    return this.catalogReader;
  }

  public JavaTypeFactory typeFactory() {
    return this.typeFactory;
  }

  public SqlValidator sqlValidator() {
    return this.validator;
  }

  void reload() {
    Config config = ConfigFactory.load();
    SqlOperatorTable operatorTable = SqlStdOperatorTable.instance();
    String rootPath = config.getString(CONFIG_ROOT_PATH);
    if (rootPath == null) {
      log.warn(String.format("'%s' is not found. Set it to '.'", CONFIG_ROOT_PATH));
      rootPath = ".";
    }
    ArrowSchema schema = new ArrowColumnarSchema(new File(rootPath));
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, true, "SAMPLES", schema);
    this.typeFactory = new JavaTypeFactoryImpl();
    this.catalogReader =
      new ArrowCatalogReader(schema, rootSchema, ImmutableList.of("SAMPLES"), typeFactory);
    this.validator = new TruffleArrowMeta.ArrowValidatorImpl(operatorTable,
      catalogReader,
      this.typeFactory,
      SqlConformance.PRAGMATIC_2003);
  }
}
