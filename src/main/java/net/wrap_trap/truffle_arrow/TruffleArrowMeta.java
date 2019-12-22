package net.wrap_trap.truffle_arrow;

import com.google.common.collect.ImmutableList;
import com.oracle.truffle.api.CallTarget;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Programs;

import java.io.File;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TruffleArrowMeta extends  MetaImpl {

  private  JavaTypeFactory typeFactory;
  private Prepare.CatalogReader catalogReader;
  private SqlValidatorImpl validator;

  public TruffleArrowMeta(AvaticaConnection connection) {
    super(connection);
    SqlOperatorTable operatorTable = SqlStdOperatorTable.instance();
    ArrowSchema schema = new ArrowSchema(new File("target/classes/samples/files"));
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, true, "SAMPLES", schema);
    this.typeFactory = new JavaTypeFactoryImpl();
    this.catalogReader =
      new ArrowCatalogReader(schema, rootSchema, ImmutableList.of("SAMPLES"), typeFactory);
    this.validator = new ArrowValidatorImpl(operatorTable,
                                             catalogReader,
                                             this.typeFactory,
                                             SqlConformance.PRAGMATIC_2003);
  }

  @Override
  public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
    StatementHandle statement = createStatement(ch);
    statement.signature = createSignature(sql, parse(sql));
    return statement;
  }

  private Signature createSignature(String sql, SqlNode sqlNode) {

    this.validator.validate(sqlNode);
    RelDataType type = this.validator.getValidatedNodeType(sqlNode);
    List<List<String>> fieldOrigins = this.validator.getFieldOrigins(sqlNode);

    List<RelDataTypeField> fieldList = type.getFieldList();
    List<ColumnMetaData> columns = new ArrayList<>();
    for (int i = 0; i < fieldList.size(); i++) {
      RelDataTypeField field = fieldList.get(i);
      List<String> origins = fieldOrigins.get(i);

      SqlTypeName sqlTypeName = type.getSqlTypeName();
      ColumnMetaData.AvaticaType avaticaType =  ColumnMetaData.scalar(
        sqlTypeName.getJdbcOrdinal(),
        sqlTypeName.getName(),
        ColumnMetaData.Rep.of(this.typeFactory.getJavaClass(field.getType()))
      );

      ColumnMetaData metadata =  new ColumnMetaData(
         i,
         false,
         true,
         false,
         false,
         type.isNullable()
           ? DatabaseMetaData.columnNullable
           : DatabaseMetaData.columnNoNulls,
         true,
         type.getPrecision(),
         field.getName(),
         origins.get(2),
         origins.get(0),
         type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
           ? 0
           : type.getPrecision(),
         type.getScale() == RelDataType.SCALE_NOT_SPECIFIED
           ? 0
           :  type.getScale(),
         origins.get(1),
         null,
         avaticaType,
         true,
         false,
         false,
         avaticaType.columnClassName());
      columns.add(metadata);
    }

    return new Signature(
      columns,
      sql,
      Collections.emptyList(),
      Collections.emptyMap(),
      CursorFactory.ARRAY,
      StatementType.SELECT
    );
  }

  private SqlNode parse(String sql) {
    try {
      SqlParser.Config config = SqlParser.configBuilder().setLex(Lex.JAVA).build();
      SqlParser parser = SqlParser.create(sql, config);
      return parser.parseStmt();
    } catch (SqlParseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, PrepareCallback callback) throws NoSuchStatementException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback) throws NoSuchStatementException {
    SqlNode sqlNode = parse(sql);
    Signature signature = createSignature(sql, sqlNode);
    RelRoot root = createPlan(sqlNode);
    start(h, root);

    try {
      synchronized (callback.getMonitor()) {
        callback.clear();
        callback.assign(signature, null, -1);
      }
      callback.execute();
      MetaResultSet metaResultSet =
        MetaResultSet.create(h.connectionId, h.id, false, signature, null);
      return new ExecuteResult(Collections.singletonList(metaResultSet));
    } catch(SQLException e){
      throw new RuntimeException(e);
    }
  }

  @Override
  public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle h, List<String> sqlCommands) throws NoSuchStatementException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExecuteBatchResult executeBatch(StatementHandle h, List<List<TypedValue>> parameterValues) throws NoSuchStatementException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) throws NoSuchStatementException, MissingResultsException {
    return null;
  }

  @Override
  public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, long maxRowCount) throws NoSuchStatementException {
    return null;
  }

  @Override
  public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame) throws NoSuchStatementException {
    MetaResultSet metaResultSet =
      MetaResultSet.create(h.connectionId, h.id, false, h.signature, null);
    return new ExecuteResult(Collections.singletonList(metaResultSet));
  }

  @Override
  public void closeStatement(StatementHandle h) {

  }

  @Override
  public boolean syncResults(StatementHandle sh, QueryState state, long offset) throws NoSuchStatementException {
    return false;
  }

  @Override
  public void commit(ConnectionHandle ch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollback(ConnectionHandle ch) {
    throw new UnsupportedOperationException();
  }

  private void start(StatementHandle handle, RelRoot plan) {
    List<Object[]> results = new ArrayList<>();
    CallTarget program = TruffleArrowLanguage.INSTANCE.compileInteractiveQuery(plan, results::add);
    TruffleArrowLanguage.callWithRootContext(program);
  }

  static class ArrowValidatorImpl extends SqlValidatorImpl {
    public ArrowValidatorImpl(SqlOperatorTable opTab,
                               SqlValidatorCatalogReader catalogReader,
                               RelDataTypeFactory typeFactory,
                               SqlConformance conformance) {
      super(opTab, catalogReader, typeFactory, conformance);
    }
  }

  private static RelRoot expandView(
                                     RelDataType rowType,
                                     String queryString,
                                     List<String> schemaPath,
                                     List<String> viewPath) {
    throw new UnsupportedOperationException();
  }

  private RelRoot createPlan(SqlNode sqlNode) {
    try {
      VolcanoPlanner planner = new VolcanoPlanner(null, new PlannerContext());
      catalogReader.registerRules(planner);

      RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(this.typeFactory));
      SqlToRelConverter.Config config =
        SqlToRelConverter.configBuilder().withTrimUnusedFields(true).build();
      SqlToRelConverter converter = new SqlToRelConverter(
                                                           TruffleArrowMeta::expandView,
                                                           this.validator,
                                                           this.catalogReader,
                                                           cluster,
                                                           StandardConvertletTable.INSTANCE,
                                                           config);

      RelRoot root = converter.convertQuery(sqlNode, true, true);
      RelTraitSet traits = root.rel.getTraitSet()
                             .replace(ArrowRel.CONVENTION)
                             .replace(root.collation)
                             .simplify();

      RelNode optimized = Programs.standard().run(
        planner, root.rel, traits, ImmutableList.of(), ImmutableList.of());
      return root.withRel(optimized);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
