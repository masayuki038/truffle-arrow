package net.wrap_trap.truffle_arrow.truffle;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class CompileContext {
  private Set<InputRefSlotMap> inputRefSlotMaps;
  private List<ThenLeader> leaders;
  private ThenLeader firstLeader;
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

  public void setFirstLeader(ThenLeader firstLeader) {
    this.firstLeader = firstLeader;
  }

  public Set<InputRefSlotMap> getInputRefSlotMaps() {
    return this.inputRefSlotMaps;
  }

  public List<ThenLeader> getLeaders() {
    return this.leaders;
  }

  public ThenLeader getFirstLeader() {
    return this.firstLeader;
  }

  public void setDir(File dir) {
    this.dir = dir;
  }

  public List<File> getPartitions() {
    return Arrays.stream(this.dir.listFiles(f -> f.isDirectory()))
             .collect(Collectors.toList());
  }
}
