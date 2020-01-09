package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.SourceSection;
import net.wrap_trap.truffle_arrow.TruffleArrowLanguage;
import org.apache.calcite.avatica.Meta;

public class RelRootNode extends RootNode {

  @Child
  private RowSource delegate;
  private  Meta.Signature signature;

  public RelRootNode(TruffleArrowLanguage language, RowSource delegate, Meta.Signature signature) {
    // TODO Cross reference by TruffleArrowLanguage
    super(language, new FrameDescriptor());
    this.delegate = delegate;
    this.signature = signature;
  }

  @Override
  public Object execute(VirtualFrame frame) {
    this.delegate.executeVoid();
    return this.signature;
  }
}
