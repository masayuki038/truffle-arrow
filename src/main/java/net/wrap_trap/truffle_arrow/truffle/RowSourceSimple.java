package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.nodes.Node;

abstract class RowSourceSimple extends RowSource {
  protected final FrameDescriptor frameDescriptor;

  /**
   * What to do with each record
   */
  @Node.Child
  protected RowSink then;

  protected RowSourceSimple(FrameDescriptor frameDescriptor, RowSink then) {
    this.frameDescriptor = frameDescriptor;
    this.then = then;
  }
}
