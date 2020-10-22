package net.wrap_trap.truffle_arrow.truffle;

import com.google.common.collect.Lists;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class TerminalSink extends RowSource {

  private CompileContext compileContext;
  private SinkContext sinkContext;
  private FrameDescriptorPart framePart;

  public static RowSource compile(CompileContext compileContext, ThenRowSink next) {
    FrameDescriptorPart framePart = FrameDescriptorPart.root(0);
    SinkContext sinkContext = new SinkContext(null, compileContext.getInputRefSlotMaps(), null, null);
    return new TerminalSink(framePart, compileContext, sinkContext, next.apply(framePart));
  }

  private TerminalSink(FrameDescriptorPart framePart, CompileContext compileContext,
                       SinkContext sinkContext, RowSink then) {
    super(then);
    this.framePart = framePart;
    this.compileContext = compileContext;
    this.sinkContext = sinkContext;
  }

  @Override
  protected List<Row> execute() {
    List<SinkContext> contexts = getPartitions()
                                     .stream()
                                     .map(f -> new SinkContext(null, this.sinkContext.getInputRefSlotMaps(), f, new ArrayList<Row>()))
                                     .collect(Collectors.toList());

    ForkJoinPool pool = new ForkJoinPool();
    contexts.forEach(newContext -> {
      pool.submit(new ParallelSink(newContext));
    });

    if (!pool.awaitQuiescence(1, TimeUnit.MINUTES)) {
      throw new IllegalStateException("Timeout while running ParallelSink");
    }

    List<Row> results = Lists.newArrayList();
    contexts.forEach(newContext -> results.addAll(newContext.getRows()));
    return results;
  }

  private List<File> getPartitions() {
    return Arrays.stream(this.compileContext.getDir().listFiles(f -> f.isDirectory()))
               .collect(Collectors.toList());
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
      } catch (UnexpectedResultException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
