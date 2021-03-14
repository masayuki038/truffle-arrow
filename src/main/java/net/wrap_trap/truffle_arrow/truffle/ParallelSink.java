package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.VirtualFrame;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RecursiveAction;

public class ParallelSink extends RecursiveAction {

  private static final Logger log = LoggerFactory.getLogger(ParallelSink.class);

  private ParallelExecuteContext p;
  private VectorSchemaRoot[] inputs;
  private VectorSchemaRoot[] results;

  public ParallelSink(ParallelExecuteContext p) {
    this(p, null);
  }

  public ParallelSink(ParallelExecuteContext p, VectorSchemaRoot[] inputs) {
    this.p = p;
    this.inputs = inputs;
  }

  public VectorSchemaRoot[] getResults() {
    return this.results;
  }

  @Override
  public void compute() {
    log.debug("start, rowSink: " + this.p.rowSink());
    try {
      VirtualFrame frame = Truffle.getRuntime()
                             .createVirtualFrame(new Object[] { }, p.framePart().frame());
      SinkContext context = p.rowSink().executeVoid(frame, inputs, p.sinkContext());
      context = p.rowSink().afterExecute(frame, context);
      this.results = context.getVectorSchemaRoots();
    } catch (Throwable e) {
      log.error("ParallelSink", e);
      throw new RuntimeException(e);
    } finally {
      log.debug("end, rowSink: " + this.p.rowSink());
    }
  }
}
