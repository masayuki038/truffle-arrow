package net.wrap_trap.truffle_arrow.language.truffle.node;

import com.oracle.truffle.api.dsl.TypeSystemReference;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.language.truffle.node.type.ArrowTimeSec;

/**
 * Base of all SQL expressions.
 */
@TypeSystemReference(SqlTypes.class)
@NodeInfo(description = "The abstract base node for all expressions")
public abstract class ExprBase extends StatementBase {
  /**
   * Compute the value of the expression. Called once for each expression in each row of a query.
   *
   * @param frame One row of data. Each FrameSlot corresponds to one column.
   * @return Result of evaluating the expression
   */
  public abstract Object executeGeneric(VirtualFrame frame);

  boolean executeBoolean(VirtualFrame frame) throws UnexpectedResultException {
    return SqlTypesGen.expectBoolean(executeGeneric(frame));
  }

  long executeLong(VirtualFrame frame) throws UnexpectedResultException {
    return SqlTypesGen.expectLong(executeGeneric(frame));
  }

  int executeInteger(VirtualFrame frame) throws UnexpectedResultException {
    return SqlTypesGen.expectInteger(executeGeneric(frame));
  }

  double executeDouble(VirtualFrame frame) throws UnexpectedResultException {
    return SqlTypesGen.expectDouble(executeGeneric(frame));
  }

  String executeString(VirtualFrame frame) throws UnexpectedResultException {
    return SqlTypesGen.expectString(executeGeneric(frame));
  }

  ArrowTimeSec executeArrowTimeSec(VirtualFrame frame) throws UnexpectedResultException {
    return SqlTypesGen.expectArrowTimeSec(executeGeneric(frame));
  }
}
