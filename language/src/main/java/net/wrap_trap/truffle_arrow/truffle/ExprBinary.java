package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.NodeChildren;

@NodeChildren({ @NodeChild("leftNode"), @NodeChild("righrNode") })
public abstract class ExprBinary extends ExprBase {
}
