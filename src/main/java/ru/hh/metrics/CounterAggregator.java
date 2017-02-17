package ru.hh.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterAggregator {
  private static final Logger logger = LoggerFactory.getLogger(CounterAggregator.class);

  private final Map<Tags, Counter> tagsToCounter = new ConcurrentHashMap<>();
  private final int maxNumOfDifferentTags;

  public CounterAggregator(int maxNumOfDifferentTags) {
    this.maxNumOfDifferentTags = maxNumOfDifferentTags;
  }

  public void add(int value, Tag tag) {
    addInner(value, tag);
  }

  public void add(int value, Tag... tagsArr) {
    addInner(value, new MultiTags(tagsArr));
  }

  private void addInner(int value, Tags tags) {
    while(true) {
      Counter counter = tagsToCounter.get(tags);
      if (counter == null) {
        if (tagsToCounter.size() >= maxNumOfDifferentTags) {
          removeSomeCounter();
        }
        counter = new Counter();
        counter.add(value);
        counter = tagsToCounter.putIfAbsent(tags, counter);
        if (counter == null) {
          return;
        }
      }
      boolean added = counter.add(value);
      if (added) {
        return;
      }
    }
  }

  private void removeSomeCounter() {
    int minValue = Integer.MAX_VALUE;
    Tags tagsToRemove = null;

    for (Map.Entry<Tags, Counter> entry : tagsToCounter.entrySet()) {
      int curValue = entry.getValue().getUnconsistent();
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
      Counter counter = tagsToCounter.remove(tags);
      int value = counter.getConsistentAndReset();
      if (value > 0) {
        tagsToCounter.putIfAbsent(tags, counter);
      }
      tagsToCountSnapshot.put(tags, value);
    }
    return tagsToCountSnapshot;
  }
}
