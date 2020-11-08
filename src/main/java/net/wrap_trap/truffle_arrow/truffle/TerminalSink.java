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

  private List<ParallelExecuteContext> sinks;

  public static RowSource compile(CompileContext compileContext, ThenRowSink next) {

    List<ParallelExecuteContext> sinks = getPartitions(compileContext)
     .stream().map(f -> {
       FrameDescriptorPart framePart = FrameDescriptorPart.root(0);
       RowSink sink = next.apply(framePart);
       SinkContext sinkContext = new SinkContext(compileContext.getInputRefSlotMaps(), f, new ArrayList<Row>());
       return new ParallelExecuteContext(framePart, sinkContext, sink);
     }).collect(Collectors.toList());
    return new TerminalSink(sinks);
  }

  private TerminalSink(List<ParallelExecuteContext> sinks) {
    this.sinks = sinks;
    sinks.forEach(s -> this.insert(s.rowSink()));
  }

  @Override
  protected List<Row> execute() {
    ForkJoinPool pool = new ForkJoinPool();

    List<ParallelSink> tasks = sinks.stream().map(parallelExecuteContext -> {
      ParallelSink task = new ParallelSink(parallelExecuteContext);
      pool.submit(task);
      return task;
    }).collect(Collectors.toList());

    return tasks.stream()
               .flatMap(p -> {
                 p.join();
                 return p.sinkContext().getRows().stream();
               }).collect(Collectors.toList());
  }

  private static List<File> getPartitions(CompileContext compileContext) {
    return Arrays.stream(compileContext.getDir().listFiles(f -> f.isDirectory()))
               .collect(Collectors.toList());
  }

  class ParallelSink extends RecursiveAction {

    private ParallelExecuteContext p;

    ParallelSink(ParallelExecuteContext p) {
      this.p = p;
    }

    SinkContext sinkContext() {
      return this.p.sinkContext();
    }

    @Override
    protected void compute() {
      try {
        VirtualFrame frame = Truffle.getRuntime()
                                 .createVirtualFrame(new Object[] { }, p.framePart().frame());
        p.rowSink().executeVoid(frame, p.sinkContext());
        p.rowSink().afterExecute(frame, p.sinkContext());
      } catch (UnexpectedResultException e) {
        log.error("ParallelSink", e);
        throw new RuntimeException(e);
      } catch (RuntimeException e) {
        log.error("ParallelSink", e);
        throw e;
      }
    }
  }

  static class ParallelExecuteContext {
    private FrameDescriptorPart framePart;
    private SinkContext sinkContext;
    private RowSink rowSink;

    ParallelExecuteContext(FrameDescriptorPart framePart, SinkContext sinkContext, RowSink rowSink) {
      this.framePart = framePart;
      this.sinkContext = sinkContext;
      this.rowSink = rowSink;
    }

    FrameDescriptorPart framePart() {
      return this.framePart;
    }

    SinkContext sinkContext() {
      return this.sinkContext;
    }

    RowSink rowSink() {
      return this.rowSink;
    }
  }
}
