package ru.hh.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class PercentileAggregator {
  private static final Logger logger = LoggerFactory.getLogger(PercentileAggregator.class);

  private final int maxHistogramSize;
  private final Map<Tags, Map<Integer, LongAdder>> tagsToHistogram = new ConcurrentHashMap<>();
  private final Double[] chances;
  private final int maxNumOfDifferentTags;

  public PercentileAggregator() {
    this(1000, new Double[]{0.5, 0.97, 0.99, 1.0}, 20);
  }

  public PercentileAggregator(int maxHistogramSize, Double[] chances, int maxNumOfDifferentTags) {
    this.maxHistogramSize = maxHistogramSize;
    this.chances = chances;
    this.maxNumOfDifferentTags = maxNumOfDifferentTags;
  }

  public void save(int value, Tag tag) {
    saveInner(value, tag);
  }

  public void save(int value, Tag... tags) {
    saveInner(value, new MultiTags(tags));
  }

  private void saveInner(int value, Tags tags) {
    Map<Integer, LongAdder> histogram = tagsToHistogram.get(tags);
    if (histogram == null) {
      if (tagsToHistogram.size() >= maxNumOfDifferentTags) {
        logger.error("Max number of different tags reached, dropping observation");
        return;
      }
      histogram = new ConcurrentHashMap<>();
      Map<Integer, LongAdder> currentHistogram = tagsToHistogram.putIfAbsent(tags, histogram);
      if (currentHistogram != null) {
        histogram = currentHistogram;
      }
    }

    LongAdder counter = histogram.get(value);
    if (counter == null) {
      if (histogram.size() >= maxHistogramSize) {
        logger.error("Max number of different histogram values reached, dropping observation for {}", tags);
        return;
      }
      counter = new LongAdder();
      LongAdder currentCounter = histogram.putIfAbsent(value, counter);
      if (currentCounter != null) {
        counter = currentCounter;
      }
    }
    counter.increment();
  }

  public Map<Tags, Integer> getSnapshotAndReset() {
    Map<Tags, Integer> tagsToValueSnapshot = new HashMap<>();
    for (Tags tags : tagsToHistogram.keySet()) {
      Map<Integer, LongAdder> histogram = tagsToHistogram.remove(tags);
      // TODO: wait until all threads stop writing to the histogram
      int totalObservations = histogram.values().stream()
          .mapToInt(LongAdder::intValue)
          .sum();

      int currentObservations = 0;
      int currentChanceIndex = 0;

      Iterator<Map.Entry<Integer, LongAdder>> it = histogram.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .iterator();
      while (it.hasNext()) {
        Map.Entry<Integer, LongAdder> metricValueToCountEntry = it.next();

        currentObservations += metricValueToCountEntry.getValue().intValue();
        for (; currentChanceIndex < chances.length && totalObservations * chances[currentChanceIndex] <= currentObservations;
             currentChanceIndex++) {
          Tag[] tagsWithPercentile = Arrays.copyOf(tags.getTags(), tags.getTags().length + 1);
          tagsWithPercentile[tagsWithPercentile.length-1] =
            new Tag("percentile", String.valueOf((int) (100 * chances[currentChanceIndex])));
          tagsToValueSnapshot.put(new MultiTags(tagsWithPercentile), metricValueToCountEntry.getKey());
        }
      }
    }

    return tagsToValueSnapshot;
  }
}
