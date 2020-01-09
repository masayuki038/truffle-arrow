package com.oracle.truffle.api.source;

import org.apache.calcite.rel.RelRoot;
import org.graalvm.polyglot.io.ByteSequence;

import java.net.URI;
import java.net.URL;
import java.util.function.Consumer;

public class TruffleArrowSource extends Source {
  private RelRoot plan;
  private Consumer<Object[]> then;

  public TruffleArrowSource(RelRoot plan,  Consumer<Object[]> then) {
    super();
    this.plan = plan;
    this.then = then;
  }

  public RelRoot getPlan() {
    return this.plan;
  }

  public Consumer<Object[]> getThen() {
    return this.then;
  }

  @Override
  Object getSourceId() {
    return this;
  }

  @Override
  Source copy() {
    return new TruffleArrowSource(this.plan, this.then);
  }

  @Override
  URI getOriginalURI() {
    return null;
  }

  @Override
  boolean isLegacy() {
    return false;
  }

  @Override
  public String getLanguage() {
    return null;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public String getPath() {
    return null;
  }

  @Override
  public boolean isInternal() {
    return false;
  }

  @Override
  public boolean isCached() {
    return false;
  }

  @Override
  public boolean isInteractive() {
    return false;
  }

  @Override
  public CharSequence getCharacters() {
    return null;
  }

  @Override
  public boolean hasBytes() {
    return false;
  }

  @Override
  public boolean hasCharacters() {
    return false;
  }

  @Override
  public ByteSequence getBytes() {
    return null;
  }

  @Override
  public URL getURL() {
    return null;
  }

  @Override
  public String getMimeType() {
    return null;
  }
}
