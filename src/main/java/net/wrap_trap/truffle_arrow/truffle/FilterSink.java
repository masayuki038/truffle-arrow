package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowFieldType;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.calcite.rex.RexNode;

import java.util.List;


public class FilterSink extends RowSink {

  public static FilterSink createSink(
    FrameDescriptor frameDescriptor,
    RexNode condition,
    SinkContext context,
    ThenRowSink next) {
    RowSink rowSink = next.apply(frameDescriptor);
    return new FilterSink(CompileExpr.compile(frameDescriptor, condition, context), rowSink);
  }

  RowSink then;
  ExprBase conditionExpr;

  private FilterSink(ExprBase conditionExpr, RowSink then) {
    this.conditionExpr = conditionExpr;
    this.then = then;
  }

  @Override
  public void executeVoid(VirtualFrame frame, FrameDescriptor frameDescriptor, SinkContext context) throws UnexpectedResultException {
    List<Integer> indices = context.getInputRefIndices();
    List<FieldVector> vectors = context.vectors();
    UInt4Vector selectionVector = ArrowUtils.createSelectionVector();
    selectionVector.setValueCount(vectors.get(0).getValueCount());
    int s = 0;

    for (int i = 0; i < vectors.get(0).getValueCount(); i ++) {
      for (int j = 0; j < indices.size(); j ++) {
        int index = indices.get(j);
        FrameSlot slot = frameDescriptor.findFrameSlot(index);
        Object value = vectors.get(index).getObject(i);
        if (value == null) {
          frameDescriptor.setFrameSlotKind(slot, FrameSlotKind.Object);
          frame.setObject(slot, SqlNull.INSTANCE);
        } else {
          ArrowFieldType type = ArrowFieldType.of(vectors.get(index).getField().getFieldType().getType());
          switch (type) {
            case INT:
            case TIME:
            case DATE:
              frameDescriptor.setFrameSlotKind(slot, FrameSlotKind.Int);
              frame.setInt(slot, (int) value);
              break;
            case LONG:
            case TIMESTAMP:
              frameDescriptor.setFrameSlotKind(slot, FrameSlotKind.Long);
              frame.setLong(slot, (long) value);
              break;
            case DOUBLE:
              frameDescriptor.setFrameSlotKind(slot, FrameSlotKind.Double);
              frame.setDouble(slot, (double) value);
              break;
            case STRING:
              frameDescriptor.setFrameSlotKind(slot, FrameSlotKind.Object);
              frame.setObject(slot, value);
              break;
            default:
              throw new IllegalArgumentException("Unexpected ArrowFieldType:" + type);
          }
        }
      }
      if (this.conditionExpr.executeBoolean(frame)) {
        selectionVector.set(s ++, i);
      }
    }
    selectionVector.setValueCount(s);
    context.setSelectionVector(selectionVector);

    then.executeVoid(frame, frameDescriptor, context);
  }
}
