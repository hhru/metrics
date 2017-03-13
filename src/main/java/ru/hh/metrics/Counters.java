package ru.hh.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Counters {
  private static final Logger logger = LoggerFactory.getLogger(Counters.class);

  private final Map<Tags, AtomicInteger> tagsToCounter = new ConcurrentHashMap<>();
  private final int maxNumOfDifferentTags;

  public Counters(int maxNumOfDifferentTags) {
    this.maxNumOfDifferentTags = maxNumOfDifferentTags;
  }

  public void add(int value, Tag tag) {
    addInner(value, tag);
  }

  public void add(int value, Tag... tagsArr) {
    addInner(value, new MultiTags(tagsArr));
  }

  private void addInner(int value, Tags tags) {
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
      AtomicInteger counter = tagsToCounter.get(tags);
      int count = counter.getAndSet(0);
      if (count == 0) {
        tagsToCounter.remove(tags);
      }
      tagsToCountSnapshot.put(tags, count);
    }
    return tagsToCountSnapshot;
  }
}
