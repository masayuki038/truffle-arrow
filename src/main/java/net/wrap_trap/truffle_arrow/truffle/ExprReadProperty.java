package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.CompilerAsserts;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.NodeChildren;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.object.DynamicObject;
import com.oracle.truffle.api.object.Location;
import com.oracle.truffle.api.object.Property;
import com.oracle.truffle.api.object.Shape;

/**
 * Extract a field from a object type.
 *
 * Based on SLReadPropertyNode and SLReadPropertyCacheNode
 */
@NodeInfo(shortName = ".")
@NodeChildren({@NodeChild("receiverNode")})
abstract class ExprReadProperty extends ExprBase {
  protected static final int CACHE_LIMIT = 3;

  protected final String name;

  protected ExprReadProperty(String name) {
    this.name = name;
  }

  @Specialization
  protected static SqlNull readNull(SqlNull receiver) {
    return SqlNull.INSTANCE;
  }

  /**
   * Polymorphic inline cache for a limited number of distinct property names and shapes.
   */
  @Specialization(
    limit = "CACHE_LIMIT",
    guards = {
               "shapeCheck(shape, receiver)"
    },
    assumptions = {
                    "shape.getValidAssumption()"
    }
  )
  protected static Object readCached(DynamicObject receiver,
                                     @Cached("lookupShape(receiver)") Shape shape,
                                     @Cached("lookupLocation(shape, name)") Location location) {
    return location.get(receiver, shape);
  }

  protected static boolean shapeCheck(Shape shape, DynamicObject receiver) {
    return shape != null && shape.check(receiver);
  }

  protected static Location lookupLocation(Shape shape, Object name) {
    // Initialization of cached values always happens in a slow path
    CompilerAsserts.neverPartOfCompilation();

    Property property = shape.getProperty(name);

    if (property == null) {
      // Property does not exist
      throw new RuntimeException("No such property " + name);
    }

    return property.getLocation();
  }

  protected static Shape lookupShape(DynamicObject receiver) {
    CompilerAsserts.neverPartOfCompilation();

//    if (!TruffleSqlContext.isSqlObject(receiver)) {
//      // TODO The specialization doForeignObject handles this case
//      return null;
//    }
    return receiver.getShape();
  }
}