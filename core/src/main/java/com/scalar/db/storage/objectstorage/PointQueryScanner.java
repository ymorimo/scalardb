package com.scalar.db.storage.objectstorage;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class PointQueryScanner implements Scanner {
  private final ResultInterpreter resultInterpreter;
  @Nullable ObjectStorageRecord record;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public PointQueryScanner(
      @Nullable ObjectStorageRecord record, ResultInterpreter resultInterpreter) {
    this.record = record;
    this.resultInterpreter = resultInterpreter;
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    if (record != null) {
      Optional<Result> ret = Optional.of(resultInterpreter.interpret(record));
      record = null;
      return ret;
    } else {
      return Optional.empty();
    }
  }

  @Override
  @Nonnull
  public List<Result> all() {
    if (record != null) {
      List<Result> ret = ImmutableList.of(resultInterpreter.interpret(record));
      record = null;
      return ret;
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  @Nonnull
  public Iterator<Result> iterator() {
    return all().iterator();
  }

  @Override
  public void close() {}
}
