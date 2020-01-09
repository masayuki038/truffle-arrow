package net.wrap_trap.truffle_arrow;

import com.google.common.collect.ImmutableList;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExecutableNode;
import com.oracle.truffle.api.source.Source;
import com.oracle.truffle.api.source.SourceSection;
import com.oracle.truffle.api.source.TruffleArrowSource;
import net.wrap_trap.truffle_arrow.truffle.RelRootNode;
import net.wrap_trap.truffle_arrow.truffle.RowSink;
import net.wrap_trap.truffle_arrow.truffle.RowSource;
import net.wrap_trap.truffle_arrow.truffle.ThenRowSink;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Programs;

import java.io.File;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

@TruffleLanguage.Registration(id="ta", name = "TruffleArrow", version = "0.1", mimeType = TruffleArrowLanguage.MIME_TYPE)
public class TruffleArrowLanguage extends TruffleLanguage<TruffleArrowContext> {
  public static final String MIME_TYPE = "application/x-truffle-arrow-sql";

  private Prepare.CatalogReader catalogReader;
  private JavaTypeFactory typeFactory;
  private SqlValidatorImpl validator;

  public TruffleArrowLanguage() {
    SqlOperatorTable operatorTable = SqlStdOperatorTable.instance();
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

  @Override
  protected TruffleArrowContext createContext(Env env) {
    return TruffleArrowContext.from(env);
  }

  @Override
  protected ExecutableNode parse(TruffleLanguage.InlineParsingRequest request) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Object findExportedSymbol(TruffleArrowContext context, String globalName, boolean onlyExplicit) {
    return null;
  }

  @Override
  protected Object getLanguageGlobal(TruffleArrowContext context) {
    return context;
  }

  @Override
  protected boolean isObjectOfLanguage(Object object) {
    return false;
  }

  @Override
  protected CallTarget parse(ParsingRequest request) throws Exception {
    String sql = request.getSource().getCharacters().toString();
    SqlNode sqlNode = parse(sql);
    Meta.Signature signature = createSignature(sql, sqlNode);
    RelRoot root = createPlan(sqlNode);
    List<Object[]> results = new ArrayList<>();

    return compileInteractiveQuery(root, signature, results::add);
  }

  private CallTarget compileInteractiveQuery(RelRoot plan, Meta.Signature signature, Consumer<Object[]> then) {
    ThenRowSink sink = resultFrame -> new RowSink() {
      @Override
      public void executeVoid(VirtualFrame frame) {
        Object[] values = new Object[resultFrame.size()];

        for (int i = 0; i < resultFrame.size(); i++) {
          FrameSlot slot = resultFrame.findFrameSlot(i);
          Object truffleValue = frame.getValue(slot);
          RelDataType type = plan.validatedRowType.getFieldList().get(i).getType();
          Object resultSetValue = truffleValue; // TODO Convert type to match ResultSet interface

          values[i] = resultSetValue;
        }
        then.accept(values);
      }
    };

    CallTarget callTarget = compile(plan, signature, sink);
    //this.callWithRootContext(callTarget);
    return callTarget;
  }

  private CallTarget compile(RelRoot plan, Meta.Signature signature, ThenRowSink sink) {
    ArrowRel rel = (ArrowRel) plan.rel;
    RowSource compiled = rel.compile(sink);
    RelRootNode root = new RelRootNode(this, compiled, signature);

    return Truffle.getRuntime().createCallTarget(root);
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

  private Meta.Signature createSignature(String sql, SqlNode sqlNode) {

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

    return new Meta.Signature(
                               columns,
                               sql,
                               Collections.emptyList(),
                               Collections.emptyMap(),
                               Meta.CursorFactory.ARRAY,
                               Meta.StatementType.SELECT
    );
  }

  private void callWithRootContext(CallTarget main) {
    Objects.requireNonNull(main, "Program is null");
    TruffleArrowContext arrowContext = new TruffleArrowContext(
      System.in,
      System.out,
      System.err
    );
    main.call(arrowContext);
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
                                                           TruffleArrowLanguage::expandView,
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
