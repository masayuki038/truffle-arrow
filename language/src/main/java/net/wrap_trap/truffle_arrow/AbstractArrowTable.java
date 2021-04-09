package net.wrap_trap.truffle_arrow;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.commons.lang3.NotImplementedException;

import java.io.File;

public abstract class AbstractArrowTable extends AbstractTable  {

  abstract public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable);

  public File getTableDirectory() {
    throw new NotImplementedException("not implemented");
  }

  public Schema getSchema() {
    throw new NotImplementedException("not implemented");
  }
}
