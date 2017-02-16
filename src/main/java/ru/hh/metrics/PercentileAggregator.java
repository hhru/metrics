package ru.hh.metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PercentileAggregator {
  private static final Logger logger = LoggerFactory.getLogger(PercentileAggregator.class);

  private final int maxHistogramSize;
  private final Map<List<Tag>, Map<Integer, LongAdder>> tagListToMetricValuesHistogram = new ConcurrentHashMap<>();
  private final List<Double> chances;
  private final int maxNumOfDifferentTags;

  public PercentileAggregator() {
    this(1000, Arrays.asList(0.5, 0.97, 0.99, 1.0), 20);
  }

  public PercentileAggregator(int maxHistogramSize, List<Double> chances, int maxNumOfDifferentTags) {
    this.maxHistogramSize = maxHistogramSize;
    this.chances = chances;
    this.maxNumOfDifferentTags = maxNumOfDifferentTags;
  }

  public void increaseMetric(int metricValue, Tag... tags) {
    List<Tag> tagList = Arrays.asList(tags);
    Collections.sort(tagList);
    Map<Integer, LongAdder> metricValuesHistogram = tagListToMetricValuesHistogram.computeIfAbsent(
            tagList, key -> {
              if (tagListToMetricValuesHistogram.size() >= maxNumOfDifferentTags) {
                logger.error("Max number of different tags reached, dropping observation");
                return null;
              } else {
                return new ConcurrentHashMap<>();
              }
            });

    if (metricValuesHistogram != null) {
      LongAdder metricValueCounter = metricValuesHistogram.computeIfAbsent(metricValue, key -> {
        if (metricValuesHistogram.size() >= maxHistogramSize) {
          logger.error("Max number of different duration values reached, dropping observation for tagList {}", tagList.toString());
          return null;
        }
        return new LongAdder();
      });
      if (metricValueCounter != null) {
        metricValueCounter.increment();
      }
    }
  }

  public Map<List<Tag>, Integer> getSnapshotAndReset() {
    Map<List<Tag>, Integer> tagListToMetricValueSnapshot = new HashMap<>();
    for (List<Tag> tagList : tagListToMetricValuesHistogram.keySet()) {
      Map<Integer, LongAdder> metricValuesHistogram = tagListToMetricValuesHistogram.remove(tagList);
      int totalObservations = metricValuesHistogram.values().stream()
          .mapToInt(LongAdder::intValue)
          .sum();

      int currentObservations = 0;
      int currentChanceIndex = 0;

      Iterator<Map.Entry<Integer, LongAdder>> it = metricValuesHistogram.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .iterator();
      while (it.hasNext()) {
        Map.Entry<Integer, LongAdder> metricValueToCountEntry = it.next();

        currentObservations += metricValueToCountEntry.getValue().intValue();
        for (; currentChanceIndex < chances.size() && totalObservations * chances.get(currentChanceIndex) <= currentObservations;
             currentChanceIndex++) {
          List<Tag> tagListWithPercentile = new ArrayList<>(tagList);
          tagListWithPercentile.add(new Tag("percentile", String.valueOf((int) (100 * chances.get(currentChanceIndex)))));
          tagListToMetricValueSnapshot.put(tagListWithPercentile, metricValueToCountEntry.getKey());
        }
      }
    }

    return tagListToMetricValueSnapshot;
  }
}
