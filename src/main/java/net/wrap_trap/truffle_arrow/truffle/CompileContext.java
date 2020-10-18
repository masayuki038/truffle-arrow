package net.wrap_trap.truffle_arrow.truffle;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class CompileContext {
    private Set<InputRefSlotMap> inputRefSlotMaps;
    private File dir;

    public CompileContext() {
        this.inputRefSlotMaps = new HashSet<>();
    }

    public void addInputRefSlotMap(int index, int slot) {
        this.inputRefSlotMaps.add(new InputRefSlotMap(index, slot));
    }

    public Set<InputRefSlotMap> getInputRefSlotMaps() {
        return this.inputRefSlotMaps;
    }

    public void setDir(File dir) {
        this.dir = dir;
    }

    public File getDir() {
        return this.dir;
    }
}
