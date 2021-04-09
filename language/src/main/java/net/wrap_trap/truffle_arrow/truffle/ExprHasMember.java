package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo
@NodeChild("receiverNode")
@NodeChild("nameNode")
public abstract class ExprHasMember extends ExprBase {
  @Specialization(limit = "3")
  boolean hasMember(Object receiver, String name,
                    @CachedLibrary("receiver") InteropLibrary objects) {
    return objects.hasMemberReadSideEffects(receiver, name);
  }
}
