package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.object.DynamicObject;
import com.oracle.truffle.api.object.Layout;
import com.oracle.truffle.api.object.ObjectType;
import com.oracle.truffle.api.object.Shape;

@NodeInfo(shortName = "new")
public abstract class TANewObject extends ExprBase {

  static final Layout LAYOUT = Layout.createLayout();
  static final Shape emptyShape = LAYOUT.createShape(TAObjectType.SINGLETON);

  @Specialization
  public Object newObject() {
    DynamicObject object = null;
    object = emptyShape.newInstance();
    return object;
  }

  public static class TAObjectType extends ObjectType {
    public static final TAObjectType SINGLETON = new TAObjectType();
  }
}


