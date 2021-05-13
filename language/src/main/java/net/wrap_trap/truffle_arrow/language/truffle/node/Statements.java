package net.wrap_trap.truffle_arrow.language.truffle.node;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo
public final class Statements extends StatementBase {

  private StatementBase[] statements;

  public Statements(StatementBase[] statements) {
    this.statements = statements;
  }

  @Override
  public void executeVoid(VirtualFrame frame) {
    for (StatementBase statement: this.statements) {
      statement.executeVoid(frame);
    }
  }
}