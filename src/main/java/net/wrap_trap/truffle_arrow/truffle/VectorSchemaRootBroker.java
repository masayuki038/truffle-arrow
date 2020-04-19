package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.rel.type.RelDataType;


public class VectorSchemaRootBroker extends RowSink {

  private final RelDataType relType;
  private VectorSchemaRoot[] vectorSchemaRoots;
  private int[] fields;
  private FrameDescriptorPart sourceFrame;
  private RowSink then;

  public static VectorSchemaRootBroker compile(
      FrameDescriptorPart sourceFrame,
      RelDataType relType,
      VectorSchemaRoot[] vectorSchemaRoots,
      int[] fields,
      ThenRowSink then) {
    FrameDescriptorPart frame = sourceFrame.push(1, FrameSlotKind.Object);
    RowSink sink = then.apply(frame);
    return new VectorSchemaRootBroker(relType, frame, vectorSchemaRoots, fields, sink);
  }

  private VectorSchemaRootBroker(
      RelDataType relType,
      FrameDescriptorPart sourceFrame,
      VectorSchemaRoot[] vectorSchemaRoots,
      int[] fields, RowSink then) {
    this.relType = relType;
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.fields = fields;
    this.sourceFrame = sourceFrame;
    this.then = then;

    assert relType.getFieldCount() == fields.length;
  }

  @Override
  public void executeVoid(VirtualFrame frame, FrameDescriptorPart sourceFrame) throws UnexpectedResultException {
    for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
      FrameSlot slot = sourceFrame.findFrameSlot(0);
      frame.setObject(slot, vectorSchemaRoot.getFieldVectors());
      then.executeVoid(frame, sourceFrame);
    }
  }
}
