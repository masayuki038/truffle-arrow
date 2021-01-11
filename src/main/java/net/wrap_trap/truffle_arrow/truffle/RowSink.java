package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowFieldType;
import net.wrap_trap.truffle_arrow.type.ArrowTimeSec;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * An expression that receives rows.
 *
 * Could transform rows, send them back to the user, or write them to a file somewhere.
 */
public abstract class RowSink extends Node {

  public SinkContext executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context)
    throws UnexpectedResultException {
    throw new UnsupportedOperationException();
  }

  /**
   * Do something with one row. Called once per row of the relational expression.
   */
  public SinkContext executeVoid(VirtualFrame frame, VectorSchemaRoot[] vectorSchemaRoots, SinkContext context)
    throws UnexpectedResultException {
    return context;
  }

  public SinkContext afterExecute(VirtualFrame frame, SinkContext context)
      throws UnexpectedResultException {
    return context;
  }
}
