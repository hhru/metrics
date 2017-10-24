package ru.hh.metrics;

import com.timgroup.statsd.StatsDClient;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A glue between aggregators ({@link Counters}, {@link Histogram}, etc.) and StatsDClient.<br/>
 * For each aggregator there is a corresponding method that registers a periodic task.<br/>
 * This task sends snapshot of the aggregator to a monitoring system and resets the aggregator.
 */
public class StatsDSender {
  private static final int PERIOD_OF_TRANSMISSION_STATS_SECONDS = 60;

  private final StatsDClient statsDClient;
  private final ScheduledExecutorService scheduledExecutorService;

  public StatsDSender(StatsDClient statsDClient, ScheduledExecutorService scheduledExecutorService) {
    this.statsDClient = statsDClient;
    this.scheduledExecutorService = scheduledExecutorService;
  }

  public void sendPercentilesPeriodically(String metricName, Histogram histogram, int... percentiles) {
    Percentiles percentilesCalculator = new Percentiles(percentiles);
    scheduledExecutorService.scheduleAtFixedRate(
        () -> {
          Map<Integer, Integer> valueToCount = histogram.getValueToCountAndReset();
          computeAndSendPercentiles(metricName, null, valueToCount, percentilesCalculator);
        },
        PERIOD_OF_TRANSMISSION_STATS_SECONDS, PERIOD_OF_TRANSMISSION_STATS_SECONDS, TimeUnit.SECONDS);
  }

  public void sendPercentilesPeriodically(String metricName, Histograms histograms, int... percentiles) {
    Percentiles percentilesСalculator = new Percentiles(percentiles);
    scheduledExecutorService.scheduleAtFixedRate(
        () -> {
          Map<Tags, Map<Integer, Integer>> tagsToHistogram = histograms.getTagsToHistogramAndReset();
          for (Map.Entry<Tags, Map<Integer, Integer>> tagsAndHistogram : tagsToHistogram.entrySet()) {
            computeAndSendPercentiles(
                metricName,
                tagsAndHistogram.getKey().getTags(),
                tagsAndHistogram.getValue(),
                percentilesСalculator
            );
          }
        },
        PERIOD_OF_TRANSMISSION_STATS_SECONDS,
        PERIOD_OF_TRANSMISSION_STATS_SECONDS, TimeUnit.SECONDS);
  }

  public void sendTiming(String metricName, long value, Tag... tags) {
    statsDClient.time(getFullMetricName(metricName, tags), value);
  }

  public void sendCounter(String metricName, long delta, Tag... tags) {
    statsDClient.count(getFullMetricName(metricName, tags), delta);
  }

  private void computeAndSendPercentiles(String metricName,
                                         @Nullable Tag[] tags,
                                         Map<Integer, Integer> valueToCount,
                                         Percentiles percentiles) {
    Map<Integer, Integer> percentileToValue = percentiles.compute(valueToCount);
    for (Map.Entry<Integer, Integer> percentileAndValue : percentileToValue.entrySet()) {
      statsDClient.gauge(
          getFullMetricName(metricName, tags) + ".percentile_is_" + percentileAndValue.getKey(),
          percentileAndValue.getValue()
      );
    }
  }

  public void sendCountersPeriodically(String metricName, Counters counters) {
    scheduledExecutorService.scheduleAtFixedRate(
        () -> sendCountMetric(metricName, counters),
        PERIOD_OF_TRANSMISSION_STATS_SECONDS,
        PERIOD_OF_TRANSMISSION_STATS_SECONDS,
        TimeUnit.SECONDS
    );
  }

  private void sendCountMetric(String metricName, Counters counters) {
    Map<Tags, Integer> counterAggregatorSnapshot = counters.getSnapshotAndReset();
    counterAggregatorSnapshot.forEach((tags, count) -> statsDClient.count(getFullMetricName(metricName, tags.getTags()), count));
  }

  public void sendMaxPeriodically(String metricName, Max max, Tag... tags) {
    String fullName = getFullMetricName(metricName, tags);
    scheduledExecutorService.scheduleAtFixedRate(
        () -> statsDClient.gauge(fullName, max.getAndReset()),
        PERIOD_OF_TRANSMISSION_STATS_SECONDS,
        PERIOD_OF_TRANSMISSION_STATS_SECONDS,
        TimeUnit.SECONDS
    );
  }

  static String getFullMetricName(String metricName, @Nullable Tag[] tags) {
    if (tags == null) {
      return metricName;
    }

    int tagsLength = tags.length;
    if (tagsLength == 0) {
      return metricName;
    }

    StringBuilder stringBuilder = new StringBuilder(metricName + '.');

    for (int i = 0; i < tagsLength; i++) {
      stringBuilder.append(tags[i].name.replace('.', '-'))
              .append("_is_")
              .append(tags[i].value.replace('.', '-'));
      if (i != tagsLength - 1) {
        stringBuilder.append('.');
      }
    }
    return stringBuilder.toString();
  }
}
