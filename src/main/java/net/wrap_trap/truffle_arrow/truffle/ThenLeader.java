package net.wrap_trap.truffle_arrow.truffle;

import net.wrap_trap.truffle_arrow.truffle.node.AbstractLeaderNode;

@FunctionalInterface
public interface ThenLeader {
  AbstractLeaderNode apply();
}
