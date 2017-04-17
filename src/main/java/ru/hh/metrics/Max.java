package ru.hh.metrics;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class Max {

  private static final AtomicIntegerFieldUpdater<Max> maxUpdater = AtomicIntegerFieldUpdater.newUpdater(Max.class, "max");

  private final int defaultValue;
  private volatile int max;

  public Max(int defaultValue) {
    this.defaultValue = defaultValue;
    max = defaultValue;
  }

  public void save(int value) {
    int currentMax = maxUpdater.get(this);
    while (value > currentMax) {
      boolean set = maxUpdater.compareAndSet(this, currentMax, value);
      if (!set) {
        currentMax = maxUpdater.get(this);
      }
    }
  }

  public int getAndReset() {
    return maxUpdater.getAndSet(this, defaultValue);
  }

  public int get() {
    return maxUpdater.get(this);
  }

}
