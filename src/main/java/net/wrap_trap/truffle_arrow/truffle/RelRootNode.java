package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import net.wrap_trap.truffle_arrow.TruffleArrowLanguage;

import java.util.List;

public class RelRootNode extends RootNode {

  @Child
  private RowSource delegate;
  private List<Row> results;

  public RelRootNode(TruffleArrowLanguage language, RowSource delegate, List<Row> results) {
    // TODO Cross reference by TruffleArrowLanguage
    super(language, new FrameDescriptor());
    this.delegate = delegate;
    this.results = results;
  }

  @Override
  public Object execute(VirtualFrame frame) {
    this.delegate.executeVoid();
    return new Result(this.results);
  }
}
