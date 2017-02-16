package ru.hh.metrics;

import com.timgroup.statsd.StatsDClient;
import java.util.List;
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
    Map<List<Tag>, Integer> percentileAggregatorSnapshot = percentileAggregator.getSnapshotAndReset();
    percentileAggregatorSnapshot.forEach((tags, percentileValue) -> statsDClient.gauge(metricName + "." + getTagString(tags), percentileValue));
  }

  private void sendCountMetric(String metricName, CounterAggregator counterAggregator) {
    Map<List<Tag>, Integer> counterAggregatorSnapshot = counterAggregator.getSnapshotAndReset();
    counterAggregatorSnapshot.forEach((tags, count) -> statsDClient.gauge(metricName + "." + getTagString(tags), (double) count / PERIOD_OF_TRANSMISSION_STATS_SECONDS));
  }

  static String getTagString(List<Tag> tagList) {
    StringBuilder stringBuilder = new StringBuilder();

    for (int i = 0; i < tagList.size(); i++) {
      stringBuilder.append(tagList.get(i).name.replace('.', '-'))
              .append("_is_")
              .append(tagList.get(i).value.replace('.', '-'));
      if (i != tagList.size() - 1) {
        stringBuilder.append(".");
      }
    }
    return stringBuilder.toString();
  }
}
