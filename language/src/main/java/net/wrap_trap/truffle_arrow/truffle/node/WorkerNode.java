package net.wrap_trap.truffle_arrow.truffle.node;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowFieldType;
import net.wrap_trap.truffle_arrow.truffle.FrameDescriptorPart;
import net.wrap_trap.truffle_arrow.truffle.InputRefSlotMap;
import net.wrap_trap.truffle_arrow.truffle.SinkContext;
import net.wrap_trap.truffle_arrow.truffle.SqlNull;
import net.wrap_trap.truffle_arrow.type.ArrowTimeSec;
import org.apache.arrow.vector.FieldVector;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public abstract class WorkerNode extends Node {

  public void executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context)
    throws UnexpectedResultException {
    throw new UnsupportedOperationException();
  }

  /**
   * Do something with one row. Called once per row of the relational expression.
   */
  public void executeVoid(VirtualFrame frame, SinkContext context)
    throws UnexpectedResultException {
    throw new UnsupportedOperationException();
  }

  public void afterExecute(VirtualFrame frame, SinkContext context)
    throws UnexpectedResultException { }

  protected void vectorEach(VirtualFrame frame, FrameDescriptorPart framePart, Map<Integer, FieldVector> vectors,
                            SinkContext context, Consumer<Integer> action) {
    Set<InputRefSlotMap> inputRefMaps = context.getInputRefSlotMaps();

    for (int i = 0; i < vectors.get(0).getValueCount(); i++) {
      for (InputRefSlotMap inputRefSlotMap : inputRefMaps) {
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
      action.accept(i);
    }
  }
}
