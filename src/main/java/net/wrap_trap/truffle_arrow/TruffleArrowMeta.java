package net.wrap_trap.truffle_arrow;

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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TruffleArrowMeta extends  MetaImpl {

  private Context context;

  public TruffleArrowMeta(AvaticaConnection connection) {
    super(connection);
    this.context = Context.newBuilder("ta").build();
  }

  @Override
  public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {

    context.eval("ta", sql);

    StatementHandle statement = createStatement(ch);
    statement.signature = createSignature(sql);
    return statement;
  }

  @Override
  public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, PrepareCallback callback) throws NoSuchStatementException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback) throws NoSuchStatementException {
    context.eval("ta", sql);
    Signature signature = createSignature(sql);

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

      SqlTypeName sqlTypeName = type.getSqlTypeName();
      ColumnMetaData.AvaticaType avaticaType =  ColumnMetaData.scalar(
        sqlTypeName.getJdbcOrdinal(),
        sqlTypeName.getName(),
        ColumnMetaData.Rep.of(typeFactory.getJavaClass(field.getType()))
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

    return new Meta.Signature(
                               columns,
                               sql,
                               Collections.emptyList(),
                               Collections.emptyMap(),
                               Meta.CursorFactory.ARRAY,
                               Meta.StatementType.SELECT
    );
  }
}
