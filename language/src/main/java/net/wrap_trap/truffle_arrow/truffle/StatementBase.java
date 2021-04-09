package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.TypeSystemReference;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;

/**
 * Base of all statements.
 */
@TypeSystemReference(SqlTypes.class)
@NodeInfo(description = "The abstract base node for all statements")
abstract class StatementBase extends Node {

  abstract void executeVoid(VirtualFrame frame);

}
