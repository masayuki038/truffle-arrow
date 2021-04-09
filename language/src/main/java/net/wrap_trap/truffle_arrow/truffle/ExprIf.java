package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import com.oracle.truffle.api.profiles.ConditionProfile;

@NodeInfo(shortName = "if")
public final class ExprIf extends ExprBase {

  @Child private ExprBase conditionNode;

  @Child private ExprBase thenPartNode;

  @Child private ExprBase elsePartNode;

  private final ConditionProfile condition = ConditionProfile.createCountingProfile();

  public ExprIf(ExprBase conditionNode, ExprBase thenPartNode, ExprBase elsePartNode) {
    this.conditionNode = conditionNode;
    this.thenPartNode = thenPartNode;
    this.elsePartNode = elsePartNode;
  }

  @Override
  public Object executeGeneric(VirtualFrame frame) {
    if (condition.profile(evaluateCondition(frame))) {
      return thenPartNode.executeGeneric(frame);
    } else if (elsePartNode != null) {
      return elsePartNode.executeGeneric(frame);
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
}