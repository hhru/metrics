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

  public void sendPercentilesPeriodically(String metricName, PercentileAggregator percentileAggregator) {
    scheduledExecutorService.scheduleAtFixedRate(() -> sendPercentilesMetric(metricName, percentileAggregator), PERIOD_OF_TRANSMISSION_STATS_SECONDS,
            PERIOD_OF_TRANSMISSION_STATS_SECONDS, TimeUnit.SECONDS);
  }

  public void sendCounterPeriodically(String metricName, CounterAggregator counterAggregator) {
    scheduledExecutorService.scheduleAtFixedRate(() -> sendCountMetric(metricName, counterAggregator), PERIOD_OF_TRANSMISSION_STATS_SECONDS,
            PERIOD_OF_TRANSMISSION_STATS_SECONDS, TimeUnit.SECONDS);
  }

  private void sendPercentilesMetric(String metricName, PercentileAggregator percentileAggregator) {
    Map<Tags, Integer> percentileAggregatorSnapshot = percentileAggregator.getSnapshotAndReset();
    percentileAggregatorSnapshot.forEach((tags, percentileValue) -> {
      String tagsString = getTagString(tags.getTags());
      statsDClient.gauge(metricName + "." + tagsString, percentileValue);
    });
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
