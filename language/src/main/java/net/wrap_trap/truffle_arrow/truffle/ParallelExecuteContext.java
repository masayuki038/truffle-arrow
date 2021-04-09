package net.wrap_trap.truffle_arrow.truffle;

public class ParallelExecuteContext {
  private FrameDescriptorPart framePart;
  private SinkContext sinkContext;
  private RowSink rowSink;

  public ParallelExecuteContext(FrameDescriptorPart framePart, SinkContext sinkContext, RowSink rowSink) {
    this.framePart = framePart;
    this.sinkContext = sinkContext;
    this.rowSink = rowSink;
  }

  public FrameDescriptorPart framePart() {
    return this.framePart;
  }

  public SinkContext sinkContext() {
    return this.sinkContext;
  }

  public RowSink rowSink() {
    return this.rowSink;
  }
}