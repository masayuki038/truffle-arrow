package net.wrap_trap.truffle_arrow.truffle.node;

import net.wrap_trap.truffle_arrow.truffle.*;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

public class FirstGroupLeaderNode extends AbstractLeaderNode {

  private List<ParallelExecuteContext> parallelExecuteContexts;

  public static ThenLeader getFactory(ThenRowSink next, CompileContext compileContext) {
    return () -> {
      List<ParallelExecuteContext> sinks = compileContext.getPartitions().stream().map(f -> {
        FrameDescriptorPart framePart = FrameDescriptorPart.root(0);
        RowSink sink = next.apply(framePart);
        SinkContext sinkContext = new SinkContext(compileContext.getInputRefSlotMaps(), f, new ArrayList<Row>());
        return new ParallelExecuteContext(framePart, sinkContext, sink);
      }).collect(Collectors.toList());
      return new FirstGroupLeaderNode(sinks);
    };
  }

  private FirstGroupLeaderNode(List<ParallelExecuteContext> parallelExecuteContexts) {
    this.parallelExecuteContexts = parallelExecuteContexts;
    parallelExecuteContexts.forEach(p -> this.insert(p.rowSink()));
  }

  @Override
  public VectorSchemaRoot[] execute(VectorSchemaRoot[] vectorSchemaRoots) {
    throw new UnsupportedOperationException();
  }

  public VectorSchemaRoot[] execute() {
    // call from TerminalSink#execute
    ForkJoinPool pool = new ForkJoinPool();

    List<VectorSchemaRoot> collected = parallelExecuteContexts.stream().map(parallelExecuteContext -> {
      ParallelSink task = new ParallelSink(parallelExecuteContext);
      pool.submit(task);
      return task;
    }).flatMap(p -> {
      p.join();
      return Arrays.stream(p.getResults());
    }).collect(Collectors.toList());

    VectorSchemaRoot[] result = new VectorSchemaRoot[collected.size()];
    collected.toArray(result);
    return result;
  }
}
