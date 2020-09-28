package net.wrap_trap.truffle_arrow.storage.columnar;

import net.wrap_trap.truffle_arrow.ArrowRel;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import net.wrap_trap.truffle_arrow.truffle.SinkContext;
import net.wrap_trap.truffle_arrow.truffle.ThenRowSink;
import net.wrap_trap.truffle_arrow.truffle.VectorSchemaRootBroker;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ArrowColumnarTableScan extends TableScan implements ArrowRel {

  private RelOptTable relOptTable;
  private File dir;
  private Schema schema;
  private List<? extends RexNode> projects;
  private int[] fields;

  public ArrowColumnarTableScan(RelOptCluster cluster, RelOptTable relOptTable,
                                File dir, Schema schema, List<? extends RexNode> projects) {
    super(cluster, cluster.traitSetOf(ArrowRel.CONVENTION), relOptTable);
    this.relOptTable = relOptTable;
    this.dir = dir;
    this.schema = schema;
    if (projects != null) {
      this.projects = projects;
      this.fields = getProjectFields(projects);
    } else {
      this.fields = this.relOptTable.getRowType().getFieldList().stream()
                      .mapToInt(f -> f.getIndex()).toArray();
    }
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ArrowColumnarTableScan(
      getCluster(), this.relOptTable, this.dir, this.schema, this.projects);
  }

  @Override
  public RelWriter explainTerms(RelWriter rw) {
    return super.explainTerms(rw).item("fields", this.projects);
  }

  @Override
  public RelDataType deriveRowType() {
    List<RelDataTypeField> fieldList = this.relOptTable.getRowType().getFieldList();
    RelDataTypeFactory.FieldInfoBuilder builder = getCluster().getTypeFactory().builder();

    if (this.fields == null) {
      for (RelDataTypeField relDataTypeField: fieldList) {
        builder.add(relDataTypeField);
      }
    } else {
      Arrays.stream(this.fields).forEach(i -> builder.add(fieldList.get(i)));
    }
    return builder.build();
  }

  public RelNode getInput() {
    return null;
  }

  public ThenRowSink createRowSink(ThenRowSink next, SinkContext context) {

    VectorSchemaRoot[] vectorSchemaRoots = this.loadVectorSchemaRoots();

    return
      frameDescriptor -> VectorSchemaRootBroker.compile(
        frameDescriptor,
        getRowType(),
        vectorSchemaRoots,
        this.projects,
        this.fields,
        context,
        next);
  }

  private int[] getProjectFields(List<? extends RexNode> exps) {
    final int[] fields = new int[exps.size()];
    for (int i = 0; i < exps.size(); i++) {
      final RexNode exp = exps.get(i);
      if (exp instanceof RexInputRef) {
        fields[i] = ((RexInputRef) exp).getIndex();
      } else {
        return null; // not a simple projection
      }
    }
    return fields;
  }

  private VectorSchemaRoot[] loadVectorSchemaRoots() {
    List<VectorSchemaRoot[]> list = loadArrowFiles();
    assert list.size() > 0;
    int size = list.get(0).length;
    List<List<FieldVector>> container = new ArrayList<>();
    for (int i = 0; i < size; i ++) {
      container.add(new ArrayList<>());
    }

    for (VectorSchemaRoot[] roots: list) {
      if (roots.length != size) {
        throw new IllegalStateException(String.format(
          "Unexpected array size of VectorSchemaRoot[]. expect: %d, actual: %d", size, roots.length));
      }
      for (int j = 0; j < roots.length; j ++) {
        List<FieldVector> vectors = roots[j].getFieldVectors();
        assert vectors.size() == 1;
        container.get(j).add(vectors.get(0));
      }
    }

    VectorSchemaRoot[] ret = new VectorSchemaRoot[size];
    for (int i = 0; i < size; i ++) {
      ret[i] = new VectorSchemaRoot(container.get(i));
    }
    return ret;
  }

  private List<VectorSchemaRoot[]> loadArrowFiles() {
    return Arrays.stream(this.fields).mapToObj(index -> {
      Field field = this.schema.getFields().get(index);
      File arrowFile = new File(dir, field.getName().toUpperCase() + ".arrow");
      String arrowFilePath = arrowFile.getAbsolutePath();
      if (!arrowFile.exists()) {
        throw new IllegalStateException("Failed to read arrow file: " + arrowFilePath);
      }

      try {
        return ArrowUtils.load(arrowFilePath);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }).collect(Collectors.toList());
  }
}