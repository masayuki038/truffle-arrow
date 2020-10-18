package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;


public class TerminalSink extends RowSource {

  private SinkContext context;
  private FrameDescriptorPart framePart;

  public static RowSource compile(CompileContext compileContext, ThenRowSink next) {
    FrameDescriptorPart framePart = FrameDescriptorPart.root(0);
    SinkContext sinkContext = new SinkContext(null, compileContext.getInputRefSlotMaps(), null);
    return new TerminalSink(framePart, sinkContext, next.apply(framePart));
  }

  private TerminalSink(FrameDescriptorPart framePart, SinkContext context, RowSink then) {
    super(then);
    this.framePart = framePart;
    this.context = context;
  }

  @Override
  protected void executeVoid() {
    ForkJoinPool pool = new ForkJoinPool(2);

    List<String> partitions = Arrays.asList("202010", "202011");
    partitions.forEach(p -> {
      SinkContext newContext = new SinkContext(null, this.context.getInputRefSlotMaps(), p);
      pool.submit(new ParallelSink(newContext));
    });

    if (!pool.awaitQuiescence(1, TimeUnit.MINUTES)) {
      throw new IllegalStateException("Timeout while running ParallelSink");
    }
  }

  class ParallelSink extends RecursiveAction {

    private SinkContext sinkContext;

    ParallelSink(SinkContext sinkContext) {
      this.sinkContext = sinkContext;
    }

    @Override
    protected void compute() {
      try {
        VirtualFrame frame = Truffle.getRuntime()
                                 .createVirtualFrame(new Object[] { }, framePart.frame());
        then.executeVoid(frame, this.sinkContext);
        then.afterExecute(frame, this.sinkContext);
      } catch (UnexpectedResultException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
