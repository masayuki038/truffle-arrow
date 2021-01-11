package net.wrap_trap.truffle_arrow.truffle;

import net.wrap_trap.truffle_arrow.TruffleArrowLanguage;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.arrow.vector.VectorSchemaRoot;

public class RelRootNode extends RootNode {

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
    } catch (UnexpectedResultException e) {
      throw new RuntimeException(e);
    }
  }
}
