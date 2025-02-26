package com.scalar.db.storage.blob;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.common.ScannerIterator;
import com.scalar.db.exception.storage.ExecutionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
@Nonnull
public class RangeQueryScanner implements Scanner {
  private final Iterator<BlobRecord> recordIterator;
  private final ResultInterpreter resultInterpreter;
  private final int recordCountLimit;
  private int recordCount;

  private ScannerIterator scannerIterator;

  public RangeQueryScanner(
      Iterator<BlobRecord> recordIterator,
      ResultInterpreter resultInterpreter,
      int recordCountLimit) {
    this.recordIterator = recordIterator;
    this.resultInterpreter = resultInterpreter;
    this.recordCountLimit = recordCountLimit;
    this.recordCount = 0;
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    if (!recordIterator.hasNext()) {
      return Optional.empty();
    }
    return Optional.of(resultInterpreter.interpret(recordIterator.next()));
  }

  @Override
  @Nonnull
  public List<Result> all() throws ExecutionException {
    List<Result> results = new ArrayList<>();
    while (true) {
      if (recordCountLimit > 0 && recordCountLimit <= recordCount) {
        break;
      }
      Optional<Result> result = one();
      if (!result.isPresent()) {
        break;
      }
      results.add(result.get());
      recordCount++;
    }
    return results;
  }

  @Override
  public void close() throws IOException {}

  @Override
  @Nonnull
  public Iterator<Result> iterator() {
    if (scannerIterator == null) {
      scannerIterator = new ScannerIterator(this);
    }
    return scannerIterator;
  }
}
