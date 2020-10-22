package net.wrap_trap.truffle_arrow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExecutableNode;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.truffle.*;
import net.wrap_trap.truffle_arrow.type.ArrowTimeSec;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.util.Text;
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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
  protected ExecutableNode parse(TruffleLanguage.InlineParsingRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected CallTarget parse(ParsingRequest request) {
    String sql = request.getSource().getCharacters().toString();
    SqlNode sqlNode = SqlParser.parse(sql);
    RelRoot root = createPlan(sqlNode);
    LastPlan.INSTANCE.set(root);
    return compileInteractiveQuery(root);
  }

  private CallTarget compileInteractiveQuery(RelRoot plan) {
    ThenRowSink sink = resultFrame -> new RowSink() {
      @Override
      public void executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context) {
        List<Object> row = framePart.getFrameSlots().stream()
                               .map(slot -> getValue(frame.getValue(slot)))
                               .collect(Collectors.toList());
        context.addRow(new Row(row));
      }
    };

    return compile(plan, sink);
  }

  private Object getValue(Object o) {
    if (o == null || o == SqlNull.INSTANCE) {
      return SqlNull.INSTANCE;
    } else if (o instanceof Text) {
      return o.toString();
    } else if (o instanceof ArrowTimeSec) {
      return ((ArrowTimeSec) o).timeSec() * 1000;
    }
    return o;
  }

  private CallTarget compile(RelRoot plan, ThenRowSink sink) {
    ArrowRel rel = (ArrowRel) plan.rel;
    RowSource compiled = rel.compile(sink, new CompileContext());
    RelRootNode root = new RelRootNode(this, compiled);

    return Truffle.getRuntime().createCallTarget(root);
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

    VolcanoPlanner planner = new VolcanoPlanner(null, new PlannerContext());
    try {
      catalogReader.registerRules(planner);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    SqlToRelConverter.Config config =
      SqlToRelConverter.configBuilder().withTrimUnusedFields(true).withConvertTableAccess(false).build();
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
  }
}