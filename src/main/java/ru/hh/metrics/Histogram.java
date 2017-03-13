package ru.hh.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Histogram {
  private static final Logger logger = LoggerFactory.getLogger(Histogram.class);

  private final int maxHistogramSize;
  private final Map<Integer, AtomicInteger> valueToCounter;

  public Histogram(int maxHistogramSize) {
    this.maxHistogramSize = maxHistogramSize;
    this.valueToCounter = new ConcurrentHashMap<>(maxHistogramSize);
  }

  public void save(int value) {
    AtomicInteger counter = valueToCounter.get(value);
    if (counter == null) {
      if (valueToCounter.size() >= maxHistogramSize) {
        logger.error("Max number of different values reached, dropping observation");
        return;
      }
      counter = new AtomicInteger(1);
      counter = valueToCounter.putIfAbsent(value, counter);
      if (counter == null) {
        return;
      }
    }
    counter.incrementAndGet();
  }

  public Map<Integer, Integer> getValueToCountAndReset() {
    Map<Integer, Integer> valueToCount = new HashMap<>(valueToCounter.size());
    for (Integer value : valueToCounter.keySet()) {
      AtomicInteger counter = valueToCounter.get(value);
      int count = counter.getAndSet(0);
      if (count > 0) {
        valueToCount.put(value, count);
      } else {
        valueToCounter.remove(value);
      }
    }
    return valueToCount;
  }
}
