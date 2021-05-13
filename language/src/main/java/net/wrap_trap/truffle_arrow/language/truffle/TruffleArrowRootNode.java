package net.wrap_trap.truffle_arrow.language.truffle;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import net.wrap_trap.truffle_arrow.language.truffle.node.ReturnException;
import net.wrap_trap.truffle_arrow.language.truffle.node.Statements;


public class TruffleArrowRootNode extends RootNode {

  @Child
  private Statements statements;

  public TruffleArrowRootNode(TruffleLanguage<?> language, FrameDescriptor frameDescriptor, Statements statements) {
    super(language, frameDescriptor);
    this.statements = statements;
  }

  @Override
  public Object execute(VirtualFrame frame) {
    try {
      statements.executeVoid(frame);
    } catch (ReturnException e) {
      return e.getResult();
    }
    return true;
  }
}
