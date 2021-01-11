package net.wrap_trap.truffle_arrow;

import com.google.common.collect.Lists;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.graalvm.polyglot.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TruffleArrowMeta extends MetaImpl {

  private static final Logger log = LoggerFactory.getLogger(TruffleArrowMeta.class);

  private final Map<Integer, Running> runningQueries = new ConcurrentHashMap<>();

  public TruffleArrowMeta(AvaticaConnection connection) {
    super(connection);
    log.info("Instanciated TruffleArrowMeta");
  }

  @Override
  public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
    log.info(String.format("prepare, ch: %s, sql: %s, maxRowCount: %d", ch.id, sql, maxRowCount));
    Context context = Context.newBuilder("ta").build();
    Value value = context.eval("ta", sql);
    List result = value.as(List.class);

    StatementHandle statement = createStatement(ch);
    this.runningQueries.put(statement.id, new Running(result, null));
    statement.signature = createSignature(sql);
    return statement;
  }

  @Override
  public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, PrepareCallback callback) {
    log.info(String.format("prepareAndExecute, h: %s, sql: %s, maxRowCount: %d, callback: %s", h.id, sql, maxRowCount, callback));
    throw new UnsupportedOperationException();
  }

  @Override
  public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback) throws NoSuchStatementException {
    log.info(String.format("prepareAndExecute, h: %s, sql: %s, maxRowCount: %d, maxRowsInFirstFrame: %d, callback: %s", h.id, sql, maxRowCount, maxRowsInFirstFrame, callback));
    Context context = Context.newBuilder("ta").build();
    Value value = context.eval("ta", sql);
    List result = value.as(List.class);
    this.runningQueries.put(h.id, new Running(result, null));

    try {
      h.signature = createSignature(sql);
      int rows =  (maxRowsInFirstFrame < 0) ? Integer.MAX_VALUE: maxRowsInFirstFrame;
      Frame firstFrame = fetch(h, 0, rows);

      synchronized (callback.getMonitor()) {
        callback.clear();
        callback.assign(h.signature, firstFrame, -1);
        callback.execute();
      }

      MetaResultSet metaResultSet =
        MetaResultSet.create(h.connectionId, h.id, false, h.signature, firstFrame);
      return new ExecuteResult(Collections.singletonList(metaResultSet));
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle h, List<String> sqlCommands) {
    log.info(String.format("prepareAndExecuteBatch, h: %s, sqlCommands: %s", h.id, sqlCommands));
    throw new UnsupportedOperationException();
  }

  @Override
  public ExecuteBatchResult executeBatch(StatementHandle h, List<List<TypedValue>> parameterValues) {
    log.info(String.format("executeBatch, h: %s, parameterValues: %s", h.id, parameterValues));
    throw new UnsupportedOperationException();
  }

  @Override
  public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) {
    log.info(String.format("fetch, h: %s, offset: %d, fetchMaxRowCount: %d", h.id, offset, fetchMaxRowCount));
    Running running = runningQueries.get(h.id);
    List<Object> slice = running.rows
                           .stream()
                           .skip(offset)
                           .limit(fetchMaxRowCount)
                           .collect(Collectors.toList());

    return new Frame(offset, slice.isEmpty(), slice);
  }

  @Override
  public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, long maxRowCount) {
    log.info(String.format("execute, h: %s, parameterValues: %s, maxRowCount: %d", h.id, parameterValues, maxRowCount));
    return null;
  }

  @Override
  public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame) {
    log.info(String.format("execute, h: %s, parameterValues: %s, maxRowsInFirstFrame: %d", h.id, parameterValues, maxRowsInFirstFrame));
    MetaResultSet metaResultSet =
      MetaResultSet.create(h.connectionId, h.id, false, h.signature, null);
    return new ExecuteResult(Collections.singletonList(metaResultSet));
  }

  @Override
  public void closeStatement(StatementHandle h) {
    log.info(String.format("closeStatement, h: %s", h.id));
  }

  @Override
  public boolean syncResults(StatementHandle sh, QueryState state, long offset) {
    log.info(String.format("syncResults, sh: %s, state: %s, offset: %d", sh.id, state, offset));
    return false;
  }

  @Override
  public void commit(ConnectionHandle ch) {
    log.info(String.format("commit, ch: %s", ch.id));
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollback(ConnectionHandle ch) {
    log.info(String.format("rollback, ch: %s", ch.id));
    throw new UnsupportedOperationException();
  }

  public static class ArrowValidatorImpl extends SqlValidatorImpl {
    public ArrowValidatorImpl(SqlOperatorTable opTab,
                               SqlValidatorCatalogReader catalogReader,
                               RelDataTypeFactory typeFactory,
                               SqlConformance conformance) {
      super(opTab, catalogReader, typeFactory, conformance);
    }
  }

  private Meta.Signature createSignature(String sql) {
    SqlValidator validator = TruffleArrowConfig.INSTANCE.sqlValidator();
    JavaTypeFactory typeFactory = TruffleArrowConfig.INSTANCE.typeFactory();
    SqlNode sqlNode = SqlParser.parse(sql);
    validator.validate(sqlNode);
    RelDataType type = validator.getValidatedNodeType(sqlNode);
    List<List<String>> fieldOrigins = validator.getFieldOrigins(sqlNode);

    List<RelDataTypeField> fieldList = type.getFieldList();
    List<ColumnMetaData> columns = new ArrayList<>();
    for (int i = 0; i < fieldList.size(); i++) {
      RelDataTypeField field = fieldList.get(i);
      List<String> origins = fieldOrigins.get(i);

      SqlTypeName sqlTypeName = type.getFieldList().get(i).getType().getSqlTypeName();
      ColumnMetaData.AvaticaType avaticaType =  ColumnMetaData.scalar(
        sqlTypeName.getJdbcOrdinal(),
        sqlTypeName.getName(),
        ColumnMetaData.Rep.of(typeFactory.getJavaClass(field.getType()))
      );

      String columnName = null;
      String schemaName = null;
      String tableName = null;
      if (origins != null) {
        columnName = origins.get(2);
        schemaName = origins.get(0);
        tableName = origins.get(1);
      }

      ColumnMetaData metadata =  new ColumnMetaData(
         i,
         false,
         true,
         false,
         false,
        type.getFieldList().get(i).getType().isNullable()
           ? DatabaseMetaData.columnNullable
           : DatabaseMetaData.columnNoNulls,
         true,
         type.getPrecision(),
         field.getName(),
         columnName,
         schemaName,
         type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
           ? 0
           : type.getPrecision(),
         type.getScale() == RelDataType.SCALE_NOT_SPECIFIED
           ? 0
           :  type.getScale(),
         tableName,
         null,
         avaticaType,
         true,
         false,
         false,
         avaticaType.columnClassName());
      columns.add(metadata);
    }

    return new Meta.Signature(
                               columns,
                               sql,
                               Collections.emptyList(),
                               Collections.emptyMap(),
                               Meta.CursorFactory.ARRAY,
                               Meta.StatementType.SELECT
    );
  }

  @Override
  public Map<DatabaseProperty, Object> getDatabaseProperties(ConnectionHandle ch) {
    Map<DatabaseProperty, Object> map = new HashMap<>();
    map.put(DatabaseProperty.AVATICA_VERSION, "1.15.0");
    map.put(DatabaseProperty.GET_DATABASE_MAJOR_VERSION, "0");
    map.put(DatabaseProperty.GET_DATABASE_MINOR_VERSION, "1");
    map.put(DatabaseProperty.GET_DRIVER_NAME, "truffle-arrow JDBC Driver");
    map.put(DatabaseProperty.GET_DRIVER_MAJOR_VERSION, "0");
    map.put(DatabaseProperty.GET_DRIVER_MINOR_VERSION, "1");
    map.put(DatabaseProperty.GET_DATABASE_PRODUCT_NAME, "truffle-arrow");
    map.put(DatabaseProperty.GET_DATABASE_PRODUCT_VERSION, "0.1");
    return map;
  }

  @Override
  public MetaResultSet getTables(ConnectionHandle ch,
                                 String catalog,
                                 Pat schemaPattern,
                                 Pat tableNamePattern,
                                 List<String> typeList) {
    return createResultSet(
      Lists.newArrayList(
        new MetaTable(
          "SIMPLE",
          "SIMPLE",
          "NATIONSSF",
          "TABLE")),
      MetaTable.class,
      "TABLE_CAT",
      "TABLE_SCHEM",
      "TABLE_NAME",
      "TABLE_TYPE");
  }

  @Override
  public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
    return createResultSet(Lists.newArrayList(new MetaSchema("SIMPLE", "SIMPLE")),
      MetaSchema.class,
      "TABLE_SCHEM",
      "TABLE_CATALOG");
  }

  @Override
  public MetaResultSet getCatalogs(ConnectionHandle ch) {
    return createResultSet(Lists.newArrayList(new MetaCatalog("SIMPLE")),
      MetaCatalog.class,
      "TABLE_CAT");
  }

  @Override
  public MetaResultSet getTableTypes(ConnectionHandle ch) {
    return createResultSet(Lists.newArrayList(new MetaTableType("TABLE")),
      MetaTableType.class,
      "TABLE_TYPE");
  }

  // From CalciteMetaImpl
  private MetaResultSet createResultSet(List<Object> results,
                                            Class clazz, String... names) {
    List<ColumnMetaData> columns = new ArrayList<>();
    List<Field> fields = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();

    try {
      for (int i = 0; i < names.length; i ++) {
        String fieldName = AvaticaUtils.toCamelCase(names[i]);
        Field field = clazz.getField(fieldName);
        columns.add(columnMetaData(names[i], i, field.getType(), false));
        fields.add(field);
        fieldNames.add(fieldName);
      }
      return createResultSet(Collections.emptyMap(),
        columns, CursorFactory.record(clazz, fields, fieldNames),
        new Frame(0, true, results));
    } catch (NoSuchFieldException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static class Running {
    public final List<Object[]> rows;
    public final RelDataType type;

    private Running(List<Object[]> rows, RelDataType type) {
      this.rows = rows;
      this.type = type;
    }
  }
}
