package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.nodes.Node;

abstract class RowSourceSimple extends RowSource {
  protected final FrameDescriptorPart sourceFrame;

  /**
   * What to do with each record
   */
  @Node.Child
  protected RowSink then;

  protected RowSourceSimple(FrameDescriptorPart sourceFrame, RowSink then) {
    this.sourceFrame = sourceFrame;
    this.then = then;
  }
}
