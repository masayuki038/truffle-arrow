package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class VectorSchemaRootBroker extends RowSink {

  public static final int SLOT_OFFSET = 1;

  private FrameDescriptorPart framePart;
  private final RelDataType relType;
  private VectorSchemaRoot[] vectorSchemaRoots;
  private int[] fields;
  private RowSink then;

  public static VectorSchemaRootBroker compile(
      FrameDescriptorPart framePart,
      RelDataType relType,
      VectorSchemaRoot[] vectorSchemaRoots,
      int[] fields,
      ThenRowSink then) {
    RowSink sink = then.apply(framePart);
    return new VectorSchemaRootBroker(framePart, relType, vectorSchemaRoots, fields, sink);
  }

  private VectorSchemaRootBroker(
      FrameDescriptorPart framePart,
      RelDataType relType,
      VectorSchemaRoot[] vectorSchemaRoots,
      int[] fields, RowSink then) {
    this.framePart = framePart;
    this.relType = relType;
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.fields = fields;
    this.then = then;

    assert relType.getFieldCount() == fields.length;
  }

  @Override
  public void executeVoid(VirtualFrame frame, SinkContext context) throws UnexpectedResultException {
    for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
      List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
      Map<Integer, FieldVector> selected = new HashMap<>();
      for (int i = 0; i < this.fields.length; i ++) {
        if (context.getInputRefIndices().contains(i)) {
          selected.put(i, fieldVectors.get(i));
        }
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
}
