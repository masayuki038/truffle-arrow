package net.wrap_trap.truffle_arrow;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import java.io.File;

public class TruffleArrowConfig {

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
    SqlOperatorTable operatorTable = SqlStdOperatorTable.instance();
    // TODO
    ArrowSchema schema = new ArrowSchema(new File("target/classes/samples/files"));
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
