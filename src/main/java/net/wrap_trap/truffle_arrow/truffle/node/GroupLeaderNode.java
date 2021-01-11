package net.wrap_trap.truffle_arrow.truffle.node;

import net.wrap_trap.truffle_arrow.truffle.*;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

public class GroupLeaderNode extends AbstractLeaderNode {

  private  List<ParallelExecuteContext> parallelExecuteContexts;

  public static ThenLeader getFactory(ThenRowSink next, CompileContext compileContext) {
    return () -> {
      List<ParallelExecuteContext> sinks = getPartitions(compileContext).stream().map(f -> {
          FrameDescriptorPart framePart = FrameDescriptorPart.root(0);
          RowSink sink = next.apply(framePart);
          SinkContext sinkContext = new SinkContext(compileContext.getInputRefSlotMaps(), f, new ArrayList<Row>());
          return new ParallelExecuteContext(framePart, sinkContext, sink);
        }).collect(Collectors.toList());
      return new GroupLeaderNode(sinks);
    };
  }

  private static List<File> getPartitions(CompileContext compileContext) {
    return Arrays.stream(compileContext.getDir().listFiles(f -> f.isDirectory()))
             .collect(Collectors.toList());
  }

  private GroupLeaderNode(List<ParallelExecuteContext> parallelExecuteContexts) {
    this.parallelExecuteContexts = parallelExecuteContexts;
    parallelExecuteContexts.forEach(p -> this.insert(p.rowSink()));
  }

  @Override
  public VectorSchemaRoot[] execute(VectorSchemaRoot[] vectorSchemaRoots) {
    // call from TerminalSink#execute
    ForkJoinPool pool = new ForkJoinPool();

    List<VectorSchemaRoot> collected = parallelExecuteContexts.stream().map(parallelExecuteContext -> {
      ParallelSink task = new ParallelSink(parallelExecuteContext, vectorSchemaRoots);
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
