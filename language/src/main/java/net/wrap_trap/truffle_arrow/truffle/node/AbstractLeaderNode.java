package net.wrap_trap.truffle_arrow.truffle.node;


import com.oracle.truffle.api.nodes.Node;
import org.apache.arrow.vector.VectorSchemaRoot;

public abstract class AbstractLeaderNode extends Node {
  abstract public VectorSchemaRoot[] execute(VectorSchemaRoot[] vectorSchemaRoots);
}
