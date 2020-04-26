package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;

import java.util.List;

public class InputRefSink extends RowSink {

  RowSink then;
  ExprBase conditionExpr;
  int index;

  public static InputRefSink createSink(ExprBase conditionExpr, int index, ThenRowSink next) {
    FrameDescriptorPart empty = FrameDescriptorPart.root(0);

    return new InputRefSink(conditionExpr, index, next.apply(empty));
  }

  private InputRefSink(ExprBase conditionExpr, int index, RowSink then) {
    this.conditionExpr = conditionExpr;
    this.index = index;
    this.then = then;
  }

  @Override
  public void executeVoid(VirtualFrame frame, FrameDescriptorPart sourceFrame) throws UnexpectedResultException {
    FrameSlot slot0 = sourceFrame.findFrameSlot(0);
    List<FieldVector> fieldVectors = (List<FieldVector>) frame.getValue(slot0);
    assert fieldVectors.size() > 0;

    FrameSlot slot1 = sourceFrame.findFrameSlot(0);
    UInt4Vector selectionVector = (UInt4Vector) frame.getValue(slot1);

    FrameDescriptorPart newFrame = sourceFrame.push(1, FrameSlotKind.Object);
    FrameSlot slot2 = newFrame.findFrameSlot(VectorSchemaRootBroker.SLOT_OFFSET);

    int selectionIndex = 0;
    for (int i = 0; i < fieldVectors.get(index).getValueCount(); i++) {
      frame.setObject(slot2, fieldVectors.get(index).getObject(i));
      if (conditionExpr.executeBoolean(frame)) {
        selectionVector.set(selectionIndex ++, i);
      }
    }
  }
}
