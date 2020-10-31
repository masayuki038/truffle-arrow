package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;


public class TerminalSink extends RowSource {

  private static final Logger log = LoggerFactory.getLogger(TerminalSink.class);

  private CompileContext compileContext;
  private SinkContext sinkContext;
  private FrameDescriptorPart framePart;

  public static RowSource compile(CompileContext compileContext, ThenRowSink next) {
    FrameDescriptorPart framePart = FrameDescriptorPart.root(0);
    SinkContext sinkContext = new SinkContext(compileContext.getInputRefSlotMaps(), null, null);
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
    ForkJoinPool pool = new ForkJoinPool();

    List<ParallelSink> tasks = getPartitions().stream().map(f -> {
      SinkContext newContext = new SinkContext(this.sinkContext.getInputRefSlotMaps(), f, new ArrayList<Row>());
      ParallelSink task = new ParallelSink(newContext);
      pool.submit(task);
      return task;
    }).collect(Collectors.toList());

    return tasks.stream()
               .flatMap(p -> {
                 p.join();
                 return p.sinkContext().getRows().stream();
               }).collect(Collectors.toList());
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

    public SinkContext sinkContext() {
      return this.sinkContext;
    }

    @Override
    protected void compute() {
      try {
        VirtualFrame frame = Truffle.getRuntime()
                                 .createVirtualFrame(new Object[] { }, framePart.frame().copy());
        then.executeVoid(frame, this.sinkContext);
        then.afterExecute(frame, this.sinkContext);
      } catch (UnexpectedResultException e) {
        log.error("ParallelSink", e);
        throw new RuntimeException(e);
      } catch (RuntimeException e) {
        log.error("ParallelSink", e);
        throw e;
      }
    }
  }
}
