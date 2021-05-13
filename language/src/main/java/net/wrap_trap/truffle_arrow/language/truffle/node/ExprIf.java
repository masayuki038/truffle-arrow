package net.wrap_trap.truffle_arrow.language.truffle.node;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import com.oracle.truffle.api.profiles.ConditionProfile;

@NodeInfo(shortName = "if")
public final class ExprIf extends ExprBase {

  @Node.Child
  private ExprBase conditionNode;

  @Node.Child
  private Statements thenPart;

  @Node.Child
  private Statements elsePart;

  private final ConditionProfile condition = ConditionProfile.createCountingProfile();

  public ExprIf(ExprBase conditionNode, Statements thenPart, Statements elsePart) {
    this.conditionNode = conditionNode;
    this.thenPart = thenPart;
    this.elsePart = elsePart;
  }

  @Override
  public Object executeGeneric(VirtualFrame frame) {
    if (condition.profile(evaluateCondition(frame))) {
      this.thenPart.executeVoid(frame);
    } else if (this.elsePart != null) {
      this.elsePart.executeVoid(frame);
    }
    return null;
  }

  private boolean evaluateCondition(VirtualFrame frame) {
    try {
      return conditionNode.executeBoolean(frame);
    } catch (UnexpectedResultException ex) {
      throw new IllegalStateException(ex);
    }
  }

  @Override
  void executeVoid(VirtualFrame frame) {
    executeGeneric(frame);
  }
}
