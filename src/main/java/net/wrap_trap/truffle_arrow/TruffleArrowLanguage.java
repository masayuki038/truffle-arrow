package net.wrap_trap.truffle_arrow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExecutableNode;
import net.wrap_trap.truffle_arrow.truffle.*;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.FieldVector;
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
import java.util.stream.IntStream;

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
    return compileInteractiveQuery(root);
  }

  private CallTarget compileInteractiveQuery(RelRoot plan) {
    final List<Row> results = new ArrayList<>();

    ThenRowSink sink = resultFrame -> new RowSink() {
      @Override
      public void executeVoid(VirtualFrame frame, FrameDescriptorPart sourceFrame) {
        FrameSlot slot = resultFrame.findFrameSlot(0);
        List<FieldVector> fieldVectors = (List<FieldVector>) frame.getValue(slot);
        Object[] vectors = new Object[fieldVectors.size()];
        fieldVectors.toArray(vectors);
        results.addAll(convertVectorsToRows(vectors));
      }
    };

    CallTarget callTarget = compile(plan, results, sink);
    return callTarget;
  }

  private List<Row> convertVectorsToRows(Object[] vectors) {
    if (vectors.length > 0) {
      return IntStream.range(0, ((ValueVector) vectors[0]).getValueCount()).mapToObj(rowIndex -> {
        List<Object> row = Arrays.stream(vectors).map(v -> {
          Object o = ((ValueVector) v).getObject(rowIndex);
          return (o instanceof Text) ? o.toString() : o;
        }).collect(Collectors.toList());
        return new Row(row);
      }).collect(Collectors.toList());
    }
    return Lists.newArrayList();
  }

  private CallTarget compile(RelRoot plan, List<Row> results, ThenRowSink sink) {
    ArrowRel rel = (ArrowRel) plan.rel;
    RowSource compiled = rel.compile(sink);
    RelRootNode root = new RelRootNode(this, compiled, results);

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