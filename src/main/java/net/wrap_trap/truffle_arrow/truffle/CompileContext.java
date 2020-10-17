package net.wrap_trap.truffle_arrow.truffle;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CompileContext {
    private Set<InputRefSlotMap> inputRefSlotMaps;

    public CompileContext() {
        this.inputRefSlotMaps = new HashSet<>();
    }

    public void addInputRefSlotMap(int index, int slot) {
        this.inputRefSlotMaps.add(new InputRefSlotMap(index, slot));
    }

    public Set<InputRefSlotMap> getInputRefSlotMaps() {
        return this.inputRefSlotMaps;
    }
}
