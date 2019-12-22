package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.rel.type.RelDataType;


public class VectorSchemaRootBroker extends RowSourceSimple {

  private final RelDataType relType;
  private VectorSchemaRoot[] vectorSchemaRoots;
  private int[] fields;

  public static VectorSchemaRootBroker compile(RelDataType relType, VectorSchemaRoot[] vectorSchemaRoots, int[] fields, ThenRowSink then) {
    FrameDescriptorPart frame = FrameDescriptorPart.root(relType.getFieldCount());
    RowSink sink = then.apply(frame);

    return new VectorSchemaRootBroker(relType, frame, vectorSchemaRoots, fields, sink);
  }

  private VectorSchemaRootBroker(RelDataType relType, FrameDescriptorPart frame, VectorSchemaRoot[] vectorSchemaRoots, int[] fields, RowSink then) {
    super(frame, then);

    assert relType.getFieldCount() == frame.size();

    this.relType = relType;
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.fields = fields;
  }

  @Override
  protected void executeVoid() {
    VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(new Object[]{}, sourceFrame.frame());

    for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
      for (int i = 0; i < this.fields.length; i++) {
        FrameSlot slot = sourceFrame.findFrameSlot(i);
        FieldVector fieldVector = vectorSchemaRoot.getFieldVectors().get(this.fields[i]);
        frame.setObject(slot, fieldVector);
      }

      then.executeVoid(frame);
    }
  }
}
