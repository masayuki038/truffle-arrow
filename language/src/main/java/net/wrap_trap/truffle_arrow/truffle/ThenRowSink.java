package net.wrap_trap.truffle_arrow.truffle;

@FunctionalInterface
public interface ThenRowSink {
    RowSink apply(FrameDescriptorPart frame);
}
