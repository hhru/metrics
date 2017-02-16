package ru.hh.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterAggregator {
  private static final Logger logger = LoggerFactory.getLogger(CounterAggregator.class);

  private final Map<Tags, AtomicInteger> tagsToCounter = new ConcurrentHashMap<>();
  private final int maxNumOfDifferentTags;

  public CounterAggregator(int maxNumOfDifferentTags) {
    this.maxNumOfDifferentTags = maxNumOfDifferentTags;
  }

  public void add(int value, Tag... tagsArr) {
    Tags tags = new Tags(tagsArr);
    AtomicInteger counter = tagsToCounter.get(tags);
    if (counter == null) {
      if (tagsToCounter.size() >= maxNumOfDifferentTags) {
        removeSomeCounter();
      }
      counter = tagsToCounter.putIfAbsent(tags, new AtomicInteger(value));
      if (counter == null) {
        return;
      }
    }
    counter.addAndGet(value);
  }

  private void removeSomeCounter() {
    int minValue = Integer.MAX_VALUE;
    Tags tagsToRemove = null;

    for (Map.Entry<Tags, AtomicInteger> entry : tagsToCounter.entrySet()) {
      int curValue = entry.getValue().intValue();
      if (curValue < minValue) {
        tagsToRemove = entry.getKey();
        minValue = curValue;
      }
    }

    tagsToCounter.remove(tagsToRemove);
    logger.error("Max num ({}) of different tags reached, removed {} counter", maxNumOfDifferentTags, tagsToRemove);
  }

  public Map<Tags, Integer> getSnapshotAndReset() {
    Map<Tags, Integer> tagsToCountSnapshot = new HashMap<>();
    for (Tags tags : tagsToCounter.keySet()) {
      tagsToCounter.compute(tags, (key, atomicInteger) -> {
        int value = atomicInteger.getAndSet(0);
        tagsToCountSnapshot.put(key, value);
        if (value > 0) {
          return atomicInteger;
        } else {
          return null;
        }
      });
    }
    return tagsToCountSnapshot;
  }
}
