package ru.hh.metrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterAggregator {
  private static final Logger logger = LoggerFactory.getLogger(CounterAggregator.class);

  private final Map<List<Tag>, AtomicInteger> tagListToCounter = new ConcurrentHashMap<>();
  private final int maxNumOfDifferentTags;

  public CounterAggregator(int maxNumOfDifferentTags) {
    this.maxNumOfDifferentTags = maxNumOfDifferentTags;
  }

  public void add(int value, Tag... tags) {
    List<Tag> tagList = Arrays.asList(tags);
    Collections.sort(tagList);
    AtomicInteger counter = tagListToCounter.get(tagList);
    if (counter == null) {
      if (tagListToCounter.size() >= maxNumOfDifferentTags) {
        removeSomeCounter();
      }
      counter = tagListToCounter.putIfAbsent(tagList, new AtomicInteger(value));
      if (counter == null) {
        return;
      }
    }
    counter.addAndGet(value);
  }

  private void removeSomeCounter() {
    int minValue = Integer.MAX_VALUE;
    List<Tag> tagListToRemove = null;

    for (Map.Entry<List<Tag>, AtomicInteger> entry : tagListToCounter.entrySet()) {
      int curValue = entry.getValue().intValue();
      if (curValue < minValue) {
        tagListToRemove = entry.getKey();
        minValue = curValue;
      }
    }

    tagListToCounter.remove(tagListToRemove);
    logger.error("Max num ({}) of different tags reached, removed {} counter", maxNumOfDifferentTags, tagListToRemove);
  }

  public Map<List<Tag>, Integer> getSnapshotAndReset() {
    Map<List<Tag>, Integer> tagListToCountSnapshot = new HashMap<>();
    for (List<Tag> tagList : tagListToCounter.keySet()) {
      tagListToCounter.compute(tagList, (key, atomicInteger) -> {
        int value = atomicInteger.getAndSet(0);
        tagListToCountSnapshot.put(key, value);
        if (value > 0) {
          return atomicInteger;
        } else {
          return null;
        }
      });
    }
    return tagListToCountSnapshot;
  }
}
