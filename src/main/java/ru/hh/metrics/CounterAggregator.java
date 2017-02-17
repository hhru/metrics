package ru.hh.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterAggregator {
  private static final Logger logger = LoggerFactory.getLogger(CounterAggregator.class);

  private final Map<Tags, LongAdder> tagsToCounter = new ConcurrentHashMap<>();
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
    LongAdder counter = tagsToCounter.get(tags);
    if (counter == null) {
      if (tagsToCounter.size() >= maxNumOfDifferentTags) {
        removeSomeCounter();
      }
      counter = new LongAdder();
      counter.add(value);
      counter = tagsToCounter.putIfAbsent(tags, counter);
      if (counter == null) {
        return;
      }
    }
    counter.add(value);
  }

  private void removeSomeCounter() {
    int minValue = Integer.MAX_VALUE;
    Tags tagsToRemove = null;

    for (Map.Entry<Tags, LongAdder> entry : tagsToCounter.entrySet()) {
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
      tagsToCounter.compute(tags, (key, counter) -> {
        int value = (int) counter.sumThenReset();
        tagsToCountSnapshot.put(key, value);
        if (value > 0) {
          return counter;
        } else {
          return null;
        }
      });
    }
    return tagsToCountSnapshot;
  }
}
