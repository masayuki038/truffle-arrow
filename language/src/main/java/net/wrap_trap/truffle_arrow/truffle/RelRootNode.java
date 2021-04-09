package net.wrap_trap.truffle_arrow.truffle;

import net.wrap_trap.truffle_arrow.ArrowUtils;
import net.wrap_trap.truffle_arrow.TruffleArrowLanguage;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelRootNode extends RootNode {

  private static final Logger log = LoggerFactory.getLogger(RelRootNode.class);

  @Child
  private RowSource delegate;

  public RelRootNode(TruffleArrowLanguage language, RowSource delegate) {
    // TODO Cross reference by TruffleArrowLanguage
    super(language, new FrameDescriptor());
    this.delegate = delegate;
  }

  @Override
  public Object execute(VirtualFrame frame) {
    try {
      VectorSchemaRoot[] results = this.delegate.execute();
      return new Result(results);
    } catch (Exception e) {
      log.error("Failed to RelRRootNode#execute", e);
      throw new RuntimeException(e);
    }
  }
}
