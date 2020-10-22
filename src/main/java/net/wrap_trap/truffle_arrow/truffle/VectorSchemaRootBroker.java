package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


public class VectorSchemaRootBroker extends RelRowSink {

  private FrameDescriptorPart framePart;
  private Schema schema;
  private File dir;
  private int[] fields;

  public static VectorSchemaRootBroker compile(
      FrameDescriptorPart framePart,
      RelDataType relType,
      File dir,
      Schema schema,
      List<? extends RexNode> projects,
      int[] fields,
      CompileContext compileContext,
      ThenRowSink then) {
    if (projects != null && projects.size() > 0) {
      for (RexNode child : projects) {
        compile(framePart, child, compileContext);
      }
    } else {
      for (int field : fields) {
        compileContext.addInputRefSlotMap(field, field);
      }
    }
    RowSink sink = then.apply(framePart);
    return new VectorSchemaRootBroker(framePart, relType, dir, schema, projects, fields, sink);
  }

  private static ExprBase compile(FrameDescriptorPart framePart, RexNode child, CompileContext compileContext) {
    return ScanCompileExpr.compile(framePart, child, compileContext);
  }

  private VectorSchemaRootBroker(
      FrameDescriptorPart framePart,
      RelDataType relType,
      File dir,
      Schema schema,
      List<? extends RexNode> projects,
      int[] fields, RowSink then) {
    super(then);
    this.framePart = framePart;
    this.dir = dir;
    this.schema = schema;
    this.fields = fields;
    this.then = then;
  }

  @Override
  public void executeVoid(VirtualFrame frame, SinkContext context) {
    VectorSchemaRoot[] vectorSchemaRoots = this.loadVectorSchemaRoots(context);
    Map<Integer, Integer> indexesMap = createFieldIndexesMap();
    for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
      List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
      Map<Integer, FieldVector> selected = new HashMap<>();
      for (InputRefSlotMap inputRefSlotMap : context.getInputRefSlotMaps()) {
        int fieldVectorIndex = indexesMap.get(inputRefSlotMap.getIndex());
        selected.put(inputRefSlotMap.getSlot(), fieldVectors.get(fieldVectorIndex));
      }

      SinkContext newContext = new SinkContext(selected, context.getInputRefSlotMaps(), context.getPartition(), context.getRows());
      this.vectorEach(frame, this.framePart, newContext, i -> {
        try {
          then.executeByRow(frame, this.framePart, newContext);
        } catch (UnexpectedResultException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  private Map<Integer, Integer> createFieldIndexesMap() {
    Map<Integer, Integer> map = new HashMap<>();
    for (int i = 0; i < this.fields.length; i ++) {
      map.put(this.fields[i], i);
    }
    return map;
  }


  private VectorSchemaRoot[] loadVectorSchemaRoots(SinkContext context) {
    List<VectorSchemaRoot[]> list = loadArrowFiles(context);
    assert list.size() > 0;
    int size = list.get(0).length;
    List<List<FieldVector>> container = new ArrayList<>();
    for (int i = 0; i < size; i ++) {
      container.add(new ArrayList<>());
    }

    for (VectorSchemaRoot[] roots: list) {
      if (roots.length != size) {
        throw new IllegalStateException(String.format(
            "Unexpected array size of VectorSchemaRoot[]. expect: %d, actual: %d", size, roots.length));
      }
      for (int j = 0; j < roots.length; j ++) {
        List<FieldVector> vectors = roots[j].getFieldVectors();
        assert vectors.size() == 1;
        container.get(j).add(vectors.get(0));
      }
    }

    VectorSchemaRoot[] ret = new VectorSchemaRoot[size];
    for (int i = 0; i < size; i ++) {
      ret[i] = new VectorSchemaRoot(container.get(i));
    }
    return ret;
  }

  private List<VectorSchemaRoot[]> loadArrowFiles(SinkContext context) {
    return Arrays.stream(this.fields).mapToObj(index -> {
      Field field = this.schema.getFields().get(index);
      File arrowFile = new File(context.getPartition(), field.getName().toUpperCase() + ".arrow");
      String arrowFilePath = arrowFile.getAbsolutePath();
      if (!arrowFile.exists()) {
        throw new IllegalStateException("Failed to read arrow file: " + arrowFilePath);
      }

      try {
        return ArrowUtils.load(arrowFilePath);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }).collect(Collectors.toList());
  }
}
