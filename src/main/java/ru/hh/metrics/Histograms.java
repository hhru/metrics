package ru.hh.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Histograms {
  private static final Logger logger = LoggerFactory.getLogger(Histograms.class);

  private final int maxHistogramSize;
  private final Map<Tags, Histogram> tagsToHistogram = new ConcurrentHashMap<>();
  private final int maxNumOfDifferentTags;

  public Histograms(int maxHistogramSize, int maxNumOfDifferentTags) {
    this.maxHistogramSize = maxHistogramSize;
    this.maxNumOfDifferentTags = maxNumOfDifferentTags;
  }

  public void save(int value, Tag tag) {
    saveInner(value, tag);
  }

  public void save(int value, Tag... tags) {
    saveInner(value, new MultiTags(tags));
  }

  private void saveInner(int value, Tags tags) {
    Histogram histogram = tagsToHistogram.get(tags);
    if (histogram == null) {
      if (tagsToHistogram.size() >= maxNumOfDifferentTags) {
        logger.error("Max number of different tags reached, dropping observation");
        return;
      }
      histogram = new Histogram(maxHistogramSize);
      Histogram currentHistogram = tagsToHistogram.putIfAbsent(tags, histogram);
      if (currentHistogram != null) {
        histogram = currentHistogram;
      }
    }
    histogram.save(value);
  }

  public Map<Tags, Map<Integer, Integer>> getTagsToHistogramAndReset() {
    Map<Tags, Map<Integer, Integer>> tagsToHistogramSnapshot = new HashMap<>(tagsToHistogram.size());
    for (Tags tags : tagsToHistogram.keySet()) {
      Histogram histogram = tagsToHistogram.get(tags);
      Map<Integer, Integer> histSnapshot = histogram.getValueToCountAndReset();
      if (!histSnapshot.isEmpty()) {
        tagsToHistogramSnapshot.put(tags, histSnapshot);
      } else {
        tagsToHistogram.remove(tags);
      }
    }
    return tagsToHistogramSnapshot;
  }
}
