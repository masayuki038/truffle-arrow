package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;

import java.util.List;
import java.util.function.Function;

public class Functions {

  public static ExprBase count(FrameDescriptorPart aggregateFramePart, List<FrameSlot> receiverFrameSlots, FrameSlot keyFrameSlot, InsertToNode insertToNode) {
    FrameSlot receiverFrameSlot = aggregateFramePart.frame().addFrameSlot(
      "receiver" + receiverFrameSlots.size(), FrameSlotKind.Object);
    receiverFrameSlots.add(receiverFrameSlot);

    ExprBase key = ExprReadLocalNodeGen.create(keyFrameSlot);
    ExprBase receiver = ExprReadLocalNodeGen.create(receiverFrameSlot);
    ExprBase hasMember = ExprHasMemberNodeGen.create(receiver, key);

    ExprBase readProperty = ExprReadPropertyNodeGen.create(receiver, key);
    ExprBase inc = ExprPlusNodeGen.create(readProperty, ExprLiteral.Long(1));
    ExprBase writeProperty = ExprWritePropertyNodeGen.create(receiver, key, inc);
    ExprBase initProperty = ExprWritePropertyNodeGen.create(receiver, key, ExprLiteral.Long(1));

    insertToNode.apply(hasMember);
    insertToNode.apply(initProperty);
    insertToNode.apply(writeProperty);

    return new ExprIf(hasMember, writeProperty, initProperty);
  }
}
