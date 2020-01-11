package net.wrap_trap.truffle_arrow;

import com.google.common.collect.ImmutableList;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExecutableNode;
import net.wrap_trap.truffle_arrow.truffle.RelRootNode;
import net.wrap_trap.truffle_arrow.truffle.RowSink;
import net.wrap_trap.truffle_arrow.truffle.RowSource;
import net.wrap_trap.truffle_arrow.truffle.ThenRowSink;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Programs;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

@TruffleLanguage.Registration(id="ta", name = "TruffleArrow", version = "0.1", mimeType = TruffleArrowLanguage.MIME_TYPE)
public class TruffleArrowLanguage extends TruffleLanguage<TruffleArrowContext> {
  public static final String MIME_TYPE = "application/x-truffle-arrow-sql";

  public TruffleArrowLanguage() {
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
    SqlNode sqlNode = SqlParser.parse(sql);
    RelRoot root = createPlan(sqlNode);
    List<Object[]> results = new ArrayList<>();

    return compileInteractiveQuery(root, results::add);
  }

  private CallTarget compileInteractiveQuery(RelRoot plan, Consumer<Object[]> then) {
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

    CallTarget callTarget = compile(plan, sink);
    //this.callWithRootContext(callTarget);
    return callTarget;
  }

  private CallTarget compile(RelRoot plan, ThenRowSink sink) {
    ArrowRel rel = (ArrowRel) plan.rel;
    RowSource compiled = rel.compile(sink);
    RelRootNode root = new RelRootNode(this, compiled);

    return Truffle.getRuntime().createCallTarget(root);
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
    SqlValidator validator = TruffleArrowConfig.INSTANCE.sqlValidator();
    JavaTypeFactory typeFactory = TruffleArrowConfig.INSTANCE.typeFactory();
    Prepare.CatalogReader catalogReader = TruffleArrowConfig.INSTANCE.catalogReader();

    try {

      VolcanoPlanner planner = new VolcanoPlanner(null, new PlannerContext());
      catalogReader.registerRules(planner);

      RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
      SqlToRelConverter.Config config =
        SqlToRelConverter.configBuilder().withTrimUnusedFields(true).build();
      SqlToRelConverter converter = new SqlToRelConverter(
                                                           TruffleArrowLanguage::expandView,
                                                           validator,
                                                           catalogReader,
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
