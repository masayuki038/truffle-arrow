package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.type.RelDataType;


public class VectorSchemaRootBroker extends RowSink {

  public static final int SLOT_OFFSET = 1;

  private final RelDataType relType;
  private VectorSchemaRoot[] vectorSchemaRoots;
  private UInt4Vector selectionVector;
  private int[] fields;
  private RowSink then;

  public static VectorSchemaRootBroker compile(
      FrameDescriptor frameDescriptor,
      RelDataType relType,
      VectorSchemaRoot[] vectorSchemaRoots,
      UInt4Vector selectionVector,
      int[] fields,
      ThenRowSink then) {
    frameDescriptor.addFrameSlot(0, FrameSlotKind.Object);
    RowSink sink = then.apply(frameDescriptor);
    return new VectorSchemaRootBroker(relType, vectorSchemaRoots, selectionVector, fields, sink);
  }

  private VectorSchemaRootBroker(
      RelDataType relType,
      VectorSchemaRoot[] vectorSchemaRoots,
      UInt4Vector selectionVector,
      int[] fields, RowSink then) {
    this.relType = relType;
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.selectionVector = selectionVector;
    this.fields = fields;
    this.then = then;

    assert relType.getFieldCount() == fields.length;
  }

  @Override
  public void executeVoid(VirtualFrame frame, FrameDescriptor frameDescriptor) throws UnexpectedResultException {
    for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
      FrameSlot slot0 = frameDescriptor.findFrameSlot(0);
      frame.setObject(slot0, vectorSchemaRoot.getFieldVectors());
      then.executeVoid(frame, frameDescriptor);
    }
  }
}
