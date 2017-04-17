package ru.hh.metrics;

import com.timgroup.statsd.StatsDClient;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StatsDSender {
  private static final int PERIOD_OF_TRANSMISSION_STATS_SECONDS = 60;

  private final StatsDClient statsDClient;
  private final ScheduledExecutorService scheduledExecutorService;
  private final int periodOfTransmissionStatsSeconds;

  public StatsDSender(StatsDClient statsDClient, ScheduledExecutorService scheduledExecutorService) {
    this(statsDClient, scheduledExecutorService, PERIOD_OF_TRANSMISSION_STATS_SECONDS);
  }

  public StatsDSender(StatsDClient statsDClient, ScheduledExecutorService scheduledExecutorService, int periodOfTransmissionStatsSeconds) {
    this.statsDClient = statsDClient;
    this.scheduledExecutorService = scheduledExecutorService;
    this.periodOfTransmissionStatsSeconds = periodOfTransmissionStatsSeconds;
  }

  public void sendPercentilesPeriodically(String metricName, Histogram histogram, int... percentiles) {
    Percentiles percentilesCalculator = new Percentiles(percentiles);
    scheduledExecutorService.scheduleAtFixedRate(
            () -> {
              Map<Integer, Integer> valueToCount = histogram.getValueToCountAndReset();
              computeAndSendPercentiles(metricName, null, valueToCount, percentilesCalculator);
            },
            periodOfTransmissionStatsSeconds,
            periodOfTransmissionStatsSeconds,
            TimeUnit.SECONDS
    );
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
            periodOfTransmissionStatsSeconds,
            periodOfTransmissionStatsSeconds,
            TimeUnit.SECONDS
    );
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
            periodOfTransmissionStatsSeconds,
            periodOfTransmissionStatsSeconds,
            TimeUnit.SECONDS
    );
  }

  private void sendCountMetric(String metricName, Counters counters) {
    Map<Tags, Integer> counterAggregatorSnapshot = counters.getSnapshotAndReset();
    counterAggregatorSnapshot.forEach((tags, count) -> statsDClient.count(getFullMetricName(metricName, tags.getTags()), count));
  }

  public void sendMaxsPeriodically(String metricName, Maxs maxs) {
    scheduledExecutorService.scheduleAtFixedRate(
            () -> sendMaxMetric(metricName, maxs),
            periodOfTransmissionStatsSeconds,
            periodOfTransmissionStatsSeconds,
            TimeUnit.SECONDS
    );
  }

  private void sendMaxMetric(String metricName, Maxs maxs) {
    Map<Tags, Integer> counterAggregatorSnapshot = maxs.getSnapshotAndReset();
    counterAggregatorSnapshot.forEach((tags, max) -> {
      final String fullMetricName = getFullMetricName(metricName, tags.getTags());
      if (max < 0) {
        statsDClient.gauge(fullMetricName, 0);
      }
      statsDClient.gauge(fullMetricName, max);
    });
  }

  public void sendMaxPeriodically(String metricName, Max max, Tag... tags) {
    String fullName = getFullMetricName(metricName, tags);
    scheduledExecutorService.scheduleAtFixedRate(
            () -> statsDClient.gauge(fullName, max.getAndReset()),
            periodOfTransmissionStatsSeconds,
            periodOfTransmissionStatsSeconds,
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
