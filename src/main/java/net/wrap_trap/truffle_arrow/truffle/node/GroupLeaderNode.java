package net.wrap_trap.truffle_arrow.truffle.node;

import net.wrap_trap.truffle_arrow.truffle.*;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GroupLeaderNode extends AbstractLeaderNode {

  private static final Logger log = LoggerFactory.getLogger(GroupLeaderNode.class);

  private  List<ParallelExecuteContext> parallelExecuteContexts;

  public static ThenLeader getFactory(ThenRowSink next, RelDataType relDataType, CompileContext compileContext) {
    log.debug("create ThenLeader");
    return () -> {
      List<ParallelExecuteContext> sinks = compileContext.getPartitions().stream().map(f -> {
          FrameDescriptorPart framePart = FrameDescriptorPart.root(0);
          RowSink sink = next.apply(framePart);
          Set<InputRefSlotMap> inputRefSlotMaps = new HashSet<>();
          for (int i = 0; i < relDataType.getFieldList().size(); i ++) {
            inputRefSlotMaps.add(new InputRefSlotMap(i, i));
          }
          SinkContext sinkContext = new SinkContext(inputRefSlotMaps, f, new ArrayList<Row>());
          return new ParallelExecuteContext(framePart, sinkContext, sink);
        }).collect(Collectors.toList());
      return new GroupLeaderNode(sinks);
    };
  }

  private GroupLeaderNode(List<ParallelExecuteContext> parallelExecuteContexts) {
    log.debug("create GroupLeaderNode");
    this.parallelExecuteContexts = parallelExecuteContexts;
    parallelExecuteContexts.forEach(p -> this.insert(p.rowSink()));
  }

  @Override
  public VectorSchemaRoot[] execute(VectorSchemaRoot[] vectorSchemaRoots) {
    int size = parallelExecuteContexts.size();
    assert(size == vectorSchemaRoots.length);
    ForkJoinPool pool = new ForkJoinPool();

    log.debug("start querying in parallel, parallelExecuteContexts.size: " + parallelExecuteContexts.size());
    List<VectorSchemaRoot> collected = IntStream.range(0, size).mapToObj(i -> {
      ParallelSink task = new ParallelSink(
        this.parallelExecuteContexts.get(i),
        new VectorSchemaRoot[]{vectorSchemaRoots[i]});
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
