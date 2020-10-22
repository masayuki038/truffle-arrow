package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.TruffleArrowLanguage;

import java.util.List;

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
      List<Row> results = this.delegate.execute();
      return new Result(results);
    } catch (UnexpectedResultException e) {
      throw new RuntimeException(e);
    }
  }
}
