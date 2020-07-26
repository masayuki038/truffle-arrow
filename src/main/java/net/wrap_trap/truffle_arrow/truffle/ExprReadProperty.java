package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.NodeChildren;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.NodeInfo;

/**
 * Extract a field from a object type.
 *
 * Based on SLReadPropertyNode and SLReadPropertyCacheNode
 */
@NodeInfo(shortName = ".")
@NodeChildren({@NodeChild("receiverNode"), @NodeChild("nameNode")})
abstract class ExprReadProperty extends ExprBase {
  @Specialization(guards = "objects.hasMembers(receiver)", limit = "3")
  protected Object readObject(Object receiver, String name,
                               @CachedLibrary("receiver") InteropLibrary objects) {
    try {
      return objects.readMember(receiver, name);
    } catch (UnsupportedMessageException | UnknownIdentifierException e) {
      throw new UnsupportedOperationException();
    }
  }
}