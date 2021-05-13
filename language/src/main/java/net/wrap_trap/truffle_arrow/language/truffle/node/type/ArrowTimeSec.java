package net.wrap_trap.truffle_arrow.language.truffle.node.type;

import org.jetbrains.annotations.NotNull;

public class ArrowTimeSec implements Comparable<ArrowTimeSec> {
  private Integer timeSec;

  public ArrowTimeSec(Integer timeSec) {
    this.timeSec = timeSec;
  }

  public Integer timeSec() {
    return timeSec;
  }

  @Override
  public int hashCode() {
    return this.timeSec.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ArrowTimeSec) {
      return this.timeSec.equals(((ArrowTimeSec) obj).timeSec());
    }
    return false;
  }

  @Override
  public int compareTo(@NotNull ArrowTimeSec arrowTimeSec) {
    return this.timeSec.compareTo(arrowTimeSec.timeSec());
  }
}
