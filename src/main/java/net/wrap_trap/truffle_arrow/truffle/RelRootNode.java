package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.SourceSection;
import net.wrap_trap.truffle_arrow.TruffleArrowLanguage;

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
    this.delegate.executeVoid();
    return null;
  }
}
