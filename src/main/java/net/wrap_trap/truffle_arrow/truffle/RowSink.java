package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

/**
 * An expression that receives rows.
 *
 * Could transform rows, send them back to the user, or write them to a file somewhere.
 */
public abstract class RowSink extends Node {
    // TODO implment array node to handle FieldVector[] for optimization
    /**
     * Do something with one row. Called once per row of the relational expression.
     */
    public abstract void executeVoid(VirtualFrame frame, FrameDescriptorPart sourceFrame)
      throws UnexpectedResultException;
}
