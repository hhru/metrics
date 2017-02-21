package ru.hh.metrics;

import com.timgroup.statsd.StatsDClient;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
          computeAndSendPercentiles(metricName, "", valueToCount, percentilesCalculator);
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
                getTagString(tagsAndHistogram.getKey().getTags()),
                tagsAndHistogram.getValue(),
                percentilesСalculator
            );
          }
        },
        PERIOD_OF_TRANSMISSION_STATS_SECONDS,
        PERIOD_OF_TRANSMISSION_STATS_SECONDS, TimeUnit.SECONDS);
  }

  private void computeAndSendPercentiles(String metricName,
                                         String tagsString,
                                         Map<Integer, Integer> valueToCount,
                                         Percentiles percentiles) {
    Map<Integer, Integer> percentileToValue = percentiles.compute(valueToCount);
    for (Map.Entry<Integer, Integer> percentileAndValue : percentileToValue.entrySet()) {
      statsDClient.gauge(
          metricName + '.' + tagsString + ".percentile_is_" + percentileAndValue.getKey(),
          percentileAndValue.getValue()
      );
    }
  }

  public void sendCounterPeriodically(String metricName, CounterAggregator counterAggregator) {
    scheduledExecutorService.scheduleAtFixedRate(() -> sendCountMetric(metricName, counterAggregator), PERIOD_OF_TRANSMISSION_STATS_SECONDS,
        PERIOD_OF_TRANSMISSION_STATS_SECONDS, TimeUnit.SECONDS);
  }

  private void sendCountMetric(String metricName, CounterAggregator counterAggregator) {
    Map<Tags, Integer> counterAggregatorSnapshot = counterAggregator.getSnapshotAndReset();
    counterAggregatorSnapshot.forEach((tags, count) -> statsDClient.gauge(metricName + "." + getTagString(tags.getTags()), (double) count / PERIOD_OF_TRANSMISSION_STATS_SECONDS));
  }

  static String getTagString(Tag[] tags) {
    StringBuilder stringBuilder = new StringBuilder();

    for (int i = 0; i < tags.length; i++) {
      stringBuilder.append(tags[i].name.replace('.', '-'))
              .append("_is_")
              .append(tags[i].value.replace('.', '-'));
      if (i != tags.length - 1) {
        stringBuilder.append(".");
      }
    }
    return stringBuilder.toString();
  }
}
