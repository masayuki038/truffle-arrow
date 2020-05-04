package net.wrap_trap.truffle_arrow;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.List;

public class ArrowCatalogReader implements Prepare.CatalogReader {

  private CalciteSchema calciteSchema ;
  private List<List<String>> schemaPaths;

  private ArrowSchema arrowSchema;
  private RelDataTypeFactory typeFactory;
  private SqlNameMatcher nameMatcher;

  public ArrowCatalogReader(ArrowSchema schema, CalciteSchema calciteSchema, List<String> defaultSchema,
                             RelDataTypeFactory typeFactory) {
    this.calciteSchema = calciteSchema;
    this.typeFactory = typeFactory;
    this.arrowSchema = schema;
    this.schemaPaths = ImmutableList.of(defaultSchema);
    this.nameMatcher =  SqlNameMatchers.withCaseSensitive(true);
  }

  @Override
  public Prepare.PreparingTable getTableForMember(List<String> names) {
    return getTable(names);
  }

  @Override
  public Prepare.CatalogReader withSchemaPath(List<String> schemaPath) {
    return null;
  }

  @Override
  public Prepare.PreparingTable getTable(List<String> names) {
    ArrowTable table = (ArrowTable) this.arrowSchema.getTableMap().get(names.get(1));
    return new ArrowPreparingTable(names, table, this.typeFactory);
  }

  @Override
  public RelDataTypeFactory getTypeFactory() {
    return this.typeFactory;
  }

  @Override
  public void registerRules(RelOptPlanner planner) throws Exception {
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRule(ArrowProjectTableScanRule.INSTANCE);
    planner.addRule(ArrowFilterTableScanRule.INSTANCE);
    planner.addRule(ArrowProjectRule.INSTANCE);
  }

  @Override
  public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax, List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return null;
  }

  @Override
  public RelDataType getNamedType(SqlIdentifier typeName) {
    return null;
  }

  @Override
  public List<SqlMoniker> getAllSchemaObjectNames(List<String> names) {
    return null;
  }

  @Override
  public List<List<String>> getSchemaPaths() {
    return this.schemaPaths;
  }

  @Override
  public RelDataTypeField field(RelDataType rowType, String alias) {
    return null;
  }

  @Override
  public SqlNameMatcher nameMatcher() {
    return this.nameMatcher;
  }

  @Override
  public boolean matches(String string, String name) {
    return nameMatcher().matches(string, name);
  }

  @Override
  public RelDataType createTypeFromProjection(RelDataType type, List<String> columnNameList) {
    return null;
  }

  @Override
  public boolean isCaseSensitive() {
    return true;
  }

  @Override
  public CalciteSchema getRootSchema() {
    return this.calciteSchema;
  }

  @Override
  public CalciteConnectionConfig getConfig() {
    return null;
  }

  @Override
  public <C> C unwrap(Class<C> aClass) {
    return null;
  }
}
