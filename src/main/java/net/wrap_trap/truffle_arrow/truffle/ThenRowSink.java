package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;

@FunctionalInterface
public interface ThenRowSink {
    RowSink apply(FrameDescriptorPart frame);
}
