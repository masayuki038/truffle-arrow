package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowFieldType;
import net.wrap_trap.truffle_arrow.type.ArrowTimeSec;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class RelRowSink extends RowSink {

  protected RowSink then;

  protected RelRowSink(RowSink then) {
    this.then = then;
    if (this != null) {
      this.insert(then);
    }
  }

  abstract protected FrameDescriptorPart getFrameDescriptorPart();

  public SinkContext executeVoid(VirtualFrame frame, VectorSchemaRoot[] vectorSchemaRoots, SinkContext initialContext) {
    SinkContext sinkContext = initialContext;
    for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
      List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
      Map<Integer, FieldVector> selected = new HashMap<>();
      for (InputRefSlotMap inputRefSlotMap : sinkContext.getInputRefSlotMaps()) {
        selected.put(inputRefSlotMap.getSlot(), fieldVectors.get(inputRefSlotMap.getIndex()));
      }
      sinkContext = this.vectorEach(frame, getFrameDescriptorPart(), selected, sinkContext);
    }
    return sinkContext;
  }

  @Override
  public SinkContext afterExecute(VirtualFrame frame, SinkContext context) throws UnexpectedResultException {
    return then.afterExecute(frame, context);
  }

  protected SinkContext vectorEach(VirtualFrame frame, FrameDescriptorPart framePart, Map<Integer, FieldVector> vectors,
                            SinkContext initialContext) {
    Set<InputRefSlotMap> inputRefMaps = initialContext.getInputRefSlotMaps();

    SinkContext context = initialContext;
    for (int i = 0; i < vectors.get(0).getValueCount(); i ++) {
      for (InputRefSlotMap inputRefSlotMap: inputRefMaps) {
        int slotId = inputRefSlotMap.getSlot();
        FrameSlot slot = framePart.findFrameSlot(slotId);
        FieldVector fieldVector = vectors.get(slotId);
        Object value = fieldVector.getObject(i);
        if (value == null) {
          framePart.setFrameSlotKind(slot, FrameSlotKind.Object);
          frame.setObject(slot, SqlNull.INSTANCE);
        } else {
          ArrowFieldType type = ArrowFieldType.of(fieldVector.getField().getFieldType().getType());
          switch (type) {
            case INT:
            case DATE:
              framePart.setFrameSlotKind(slot, FrameSlotKind.Int);
              frame.setInt(slot, (int) value);
              break;
            case TIME:
              framePart.setFrameSlotKind(slot, FrameSlotKind.Object);
              frame.setObject(slot, new ArrowTimeSec((int) value));
              break;
            case LONG:
            case TIMESTAMP:
              framePart.setFrameSlotKind(slot, FrameSlotKind.Long);
              frame.setLong(slot, (long) value);
              break;
            case DOUBLE:
              framePart.setFrameSlotKind(slot, FrameSlotKind.Double);
              frame.setDouble(slot, (double) value);
              break;
            case STRING:
              framePart.setFrameSlotKind(slot, FrameSlotKind.Object);
              frame.setObject(slot, value);
              break;
            default:
              throw new IllegalArgumentException("Unexpected ArrowFieldType:" + type);
          }
        }
      }

      try {
        context = this.executeByRow(frame, framePart, context);
      } catch (UnexpectedResultException e) {
        throw new RuntimeException(e);
      }
    }

    return context;
  }
}
