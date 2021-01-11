package net.wrap_trap.truffle_arrow.truffle;

import net.wrap_trap.truffle_arrow.truffle.node.AbstractLeaderNode;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


public class TerminalSink extends RowSource {

  private static final Logger log = LoggerFactory.getLogger(TerminalSink.class);

  private List<AbstractLeaderNode> leaders;

  public static RowSource compile(CompileContext compileContext) {
    List<AbstractLeaderNode> leaders =
      compileContext.getLeaders().stream().map(thenLeader -> thenLeader.apply())
        .collect(Collectors.toList());
    Collections.reverse(leaders);
    return new TerminalSink(leaders);
  }

  private TerminalSink(List<AbstractLeaderNode> leaders) {
    this.leaders = leaders;
    leaders.forEach(l -> this.insert(l));
  }

  @Override
  protected VectorSchemaRoot[] execute() {
    VectorSchemaRoot[] vectorSchemaRoots = null;
    for (AbstractLeaderNode leader : leaders) {
      vectorSchemaRoots = leader.execute(vectorSchemaRoots);
    };
    return vectorSchemaRoots;
  }
}
