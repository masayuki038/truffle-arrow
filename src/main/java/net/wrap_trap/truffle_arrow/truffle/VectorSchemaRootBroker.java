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


public class VectorSchemaRootBroker extends RowSink {

  private FrameDescriptorPart framePart;
  private VectorSchemaRoot[] vectorSchemaRoots;
  private RowSink then;

  public static VectorSchemaRootBroker compile(
      FrameDescriptorPart framePart,
      RelDataType relType,
      VectorSchemaRoot[] vectorSchemaRoots,
      List<? extends RexNode> projects,
      int[] fields,
      SinkContext context,
      ThenRowSink then) {
    if (projects.size() > 0) {
      for (int i = 0; i < projects.size(); i ++) {
        RexNode child = projects.get(i);
        compile(framePart, child, context);
      }
    } else {
      for (int i = 0; i < fields.length; i ++ ) {
        context.addInputRefSlotMap(fields[i], fields[i]);
      }
    }
    RowSink sink = then.apply(framePart);
    return new VectorSchemaRootBroker(framePart, relType, vectorSchemaRoots, projects, sink);
  }

  private static ExprBase compile(FrameDescriptorPart framePart, RexNode child, SinkContext context) {
    return ScanCompileExpr.compile(framePart, child, context);
  }

  private VectorSchemaRootBroker(
      FrameDescriptorPart framePart,
      RelDataType relType,
      VectorSchemaRoot[] vectorSchemaRoots,
      List<? extends RexNode> projects, RowSink then) {
    this.framePart = framePart;
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.then = then;
  }

  @Override
  public void executeVoid(VirtualFrame frame, SinkContext context) throws UnexpectedResultException {
    for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
      List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
      Map<Integer, FieldVector> selected = new HashMap<>();
      for (InputRefSlotMap inputRefSlotMap : context.getInputRefSlotMaps()) {
        selected.put(inputRefSlotMap.getSlot(), fieldVectors.get(inputRefSlotMap.getIndex()));
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

  @Override
  public void afterExecute(VirtualFrame frame, SinkContext context) throws UnexpectedResultException {
    then.afterExecute(frame, context);
  }
}
