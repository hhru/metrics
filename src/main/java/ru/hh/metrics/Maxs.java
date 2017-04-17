package ru.hh.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Integer.MIN_VALUE;

public class Maxs {
  private static final Logger logger = LoggerFactory.getLogger(Maxs.class);

  private final Map<Tags, Max> tagsToMax = new ConcurrentHashMap<>();
  private final int maxNumOfDifferentTags;

  public Maxs(int maxNumOfDifferentTags) {
    this.maxNumOfDifferentTags = maxNumOfDifferentTags;
  }

  public void set(int value, Tag tag) {
    setInner(value, tag);
  }

  public void set(int value, Tag... tagsArr) {
    setInner(value, new MultiTags(tagsArr));
  }

  private void setInner(int value, Tags tags) {
    Max max = tagsToMax.get(tags);
    if (max == null) {
      if (tagsToMax.size() >= maxNumOfDifferentTags) {
        removeSomeMax();
      }
      final Max newMax = new Max(MIN_VALUE);
      newMax.save(value);
      max = tagsToMax.putIfAbsent(tags, newMax);
      if (max == null) {
        return;
      }
    }
    max.save(value);
  }

  private void removeSomeMax() {
    int minValue = Integer.MAX_VALUE;
    Tags tagsToRemove = null;

    for (Map.Entry<Tags, Max> entry : tagsToMax.entrySet()) {
      int curValue = entry.getValue().get();
      if (curValue < minValue) {
        tagsToRemove = entry.getKey();
        minValue = curValue;
      }
    }

    tagsToMax.remove(tagsToRemove);
    logger.error("Max num ({}) of different tags reached, removed {} max", maxNumOfDifferentTags, tagsToRemove);
  }

  public Map<Tags, Integer> getSnapshotAndReset() {
    Map<Tags, Integer> tagsToMaxSnapshot = new HashMap<>();
    for (Tags tags : tagsToMax.keySet()) {
      Max max = tagsToMax.get(tags);
      int snapshotMax = max.getAndReset();
      if (snapshotMax == 0) {
        tagsToMax.remove(tags);
      }
      tagsToMaxSnapshot.put(tags, snapshotMax);
    }
    return tagsToMaxSnapshot;
  }
}
