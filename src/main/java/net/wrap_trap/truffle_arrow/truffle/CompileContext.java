package net.wrap_trap.truffle_arrow.truffle;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CompileContext {
    private Set<InputRefSlotMap> inputRefSlotMaps;
    private List<ThenLeader> leaders;
    private File dir;

    public CompileContext() {
        this.inputRefSlotMaps = new HashSet<>();
        this.leaders = new ArrayList<>();
    }

    public void addInputRefSlotMap(int index, int slot) {
        this.inputRefSlotMaps.add(new InputRefSlotMap(index, slot));
    }

    public void addLeader(ThenLeader thenLeader) {
        this.leaders.add(thenLeader);
    }

    public Set<InputRefSlotMap> getInputRefSlotMaps() {
        return this.inputRefSlotMaps;
    }

    public List<ThenLeader> getLeaders() {
        return this.leaders;
    }

    public void setDir(File dir) {
        this.dir = dir;
    }

    public File getDir() {
        return this.dir;
    }
}
