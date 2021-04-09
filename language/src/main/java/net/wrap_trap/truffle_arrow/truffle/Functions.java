package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameSlot;

public class Functions {

  public static ExprBase count(FrameSlot receiverFrameSlot, FrameSlot keyFrameSlot, InsertToNode insertToNode) {
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

  public static ExprBase sum(FrameSlot receiverFrameSlot, FrameSlot keyFrameSlot, FrameSlot toBeAddedSlot, InsertToNode insertToNode) {
    ExprBase key = ExprReadLocalNodeGen.create(keyFrameSlot);
    ExprBase toBeAdded = ExprReadLocalNodeGen.create(toBeAddedSlot);
    ExprBase receiver = ExprReadLocalNodeGen.create(receiverFrameSlot);
    ExprBase hasMember = ExprHasMemberNodeGen.create(receiver, key);

    ExprBase readProperty = ExprReadPropertyNodeGen.create(receiver, key);
    ExprBase inc = ExprPlusNodeGen.create(readProperty, toBeAdded);
    ExprBase writeProperty = ExprWritePropertyNodeGen.create(receiver, key, inc);
    ExprBase initProperty = ExprWritePropertyNodeGen.create(receiver, key, toBeAdded);

    insertToNode.apply(hasMember);
    insertToNode.apply(initProperty);
    insertToNode.apply(writeProperty);

    return new ExprIf(hasMember, writeProperty, initProperty);
  }
}
