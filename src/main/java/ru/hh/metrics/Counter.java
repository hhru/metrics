package ru.hh.metrics;

import java.util.concurrent.atomic.LongAdder;

class Counter {

  private final LongAdder adders = new LongAdder();
  private final LongAdder value = new LongAdder();
  private volatile boolean lockedForSnapshot = false;

  boolean add(int value) {
    adders.increment();
    try {
      if (lockedForSnapshot) {
        return false;
      }
      this.value.add(value);
    } finally {
      adders.decrement();
    }
    return true;
  }

  int getConsistentAndReset() {
    lockedForSnapshot = true;
    try {
      while (adders.intValue() > 0) {
        try {
          Thread.sleep(0L, 1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
      return (int) this.value.sumThenReset();
    } finally {
      lockedForSnapshot = false;
    }
  }

  int getUnconsistent() {
    return value.intValue();
  }

}
