package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class VectorSchemaRootBroker extends RelRowSink {

  private FrameDescriptorPart framePart;
  private VectorSchemaRoot[] vectorSchemaRoots;
  private int[] fields;

  public static VectorSchemaRootBroker compile(
      FrameDescriptorPart framePart,
      RelDataType relType,
      VectorSchemaRoot[] vectorSchemaRoots,
      List<? extends RexNode> projects,
      int[] fields,
      SinkContext context,
      ThenRowSink then) {
    if (projects != null && projects.size() > 0) {
      for (RexNode child : projects) {
        compile(framePart, child, context);
      }
    } else {
      for (int field : fields) {
        context.addInputRefSlotMap(field, field);
      }
    }
    RowSink sink = then.apply(framePart);
    return new VectorSchemaRootBroker(framePart, relType, vectorSchemaRoots, projects, fields, sink);
  }

  private static ExprBase compile(FrameDescriptorPart framePart, RexNode child, SinkContext context) {
    return ScanCompileExpr.compile(framePart, child, context);
  }

  private VectorSchemaRootBroker(
      FrameDescriptorPart framePart,
      RelDataType relType,
      VectorSchemaRoot[] vectorSchemaRoots,
      List<? extends RexNode> projects,
      int[] fields, RowSink then) {
    super(then);
    this.framePart = framePart;
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.fields = fields;
    this.then = then;
  }

  @Override
  public void executeVoid(VirtualFrame frame, SinkContext context) {
    Map<Integer, Integer> indexesMap = createFieldIndexesMap();
    for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
      List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
      Map<Integer, FieldVector> selected = new HashMap<>();
      for (InputRefSlotMap inputRefSlotMap : context.getInputRefSlotMaps()) {
        int fieldVectorIndex = indexesMap.get(inputRefSlotMap.getIndex());
        selected.put(inputRefSlotMap.getSlot(), fieldVectors.get(fieldVectorIndex));
      }

      context.setVectors(selected);
      this.vectorEach(frame, this.framePart, context, i -> {
        try {
          then.executeByRow(frame, this.framePart, context);
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
}
