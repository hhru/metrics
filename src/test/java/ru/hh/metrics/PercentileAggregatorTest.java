package ru.hh.metrics;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class PercentileAggregatorTest {

  private static PercentileAggregator percentileAggregator = new PercentileAggregator();;

  @Before
  public void setUp() {
    percentileAggregator = new PercentileAggregator();
  }

  @Test
  public void increaseMetricValueStatsShouldBeSend() throws InterruptedException {
    for (int i = 0; i < 50; i++) {
      percentileAggregator.increaseMetric(100, new Tag("targetServer", "localhost"));
    }
    for (int i = 0; i < 45; i++) {
      percentileAggregator.increaseMetric(200, new Tag("targetServer", "localhost"));
    }
    for (int i = 0; i < 2; i++) {
      percentileAggregator.increaseMetric(300, new Tag("targetServer", "localhost"));
    }
    for (int i = 0; i < 2; i++) {
      percentileAggregator.increaseMetric(400, new Tag("targetServer", "localhost"));
    }

    Map<List<Tag>, Integer> expectedSnapshot = new HashMap<>();
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "50")), 100);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "97")), 300);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "99")), 400);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "100")), 400);

    assertEquals(expectedSnapshot, percentileAggregator.getSnapshotAndReset());
  }

  @Test
  public void sendPercentilesWithoutValues() {
    assertEquals(emptyMap(), percentileAggregator.getSnapshotAndReset());
  }

  @Test
  public void sendPercentilesOneValue() {
    percentileAggregator.increaseMetric(1, new Tag("targetServer", "localhost"));

    Map<List<Tag>, Integer> expectedSnapshot = new HashMap<>();
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "50")), 1);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "97")), 1);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "99")), 1);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "100")), 1);

    assertEquals(expectedSnapshot, percentileAggregator.getSnapshotAndReset());
  }

  @Test
  public void sendPercentilesTwoValues() {
    percentileAggregator.increaseMetric(1, new Tag("targetServer", "localhost"));
    percentileAggregator.increaseMetric(2, new Tag("targetServer", "localhost"));

    Map<List<Tag>, Integer> expectedSnapshot = new HashMap<>();
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "50")), 1);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "97")), 2);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "99")), 2);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "100")), 2);

    assertEquals(expectedSnapshot, percentileAggregator.getSnapshotAndReset());
  }

  @Test
  public void sendPercentilesTwoTargetServers() {
    percentileAggregator.increaseMetric(1, new Tag("targetServer", "localhost"));
    percentileAggregator.increaseMetric(2, new Tag("targetServer", "google"));

    Map<List<Tag>, Integer> expectedSnapshot = new HashMap<>();
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "50")), 1);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "97")), 1);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "99")), 1);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "100")), 1);
    expectedSnapshot.put(asList(new Tag("targetServer", "google"), new Tag("percentile", "50")), 2);
    expectedSnapshot.put(asList(new Tag("targetServer", "google"), new Tag("percentile", "97")), 2);
    expectedSnapshot.put(asList(new Tag("targetServer", "google"), new Tag("percentile", "99")), 2);
    expectedSnapshot.put(asList(new Tag("targetServer", "google"), new Tag("percentile", "100")), 2);

    assertEquals(expectedSnapshot, percentileAggregator.getSnapshotAndReset());
  }

  @Test
  public void sendPercentilesTwoTags() {
    percentileAggregator.increaseMetric(1, new Tag("targetServer", "localhost"), new Tag("key", "one"));
    percentileAggregator.increaseMetric(2, new Tag("targetServer", "google"), new Tag("key", "two"));
    percentileAggregator.increaseMetric(3, new Tag("targetServer", "localhost"), new Tag("key", "two"));
    percentileAggregator.increaseMetric(4, new Tag("targetServer", "google"), new Tag("key", "three"));

    Map<List<Tag>, Integer> expectedSnapshot = new HashMap<>();
    expectedSnapshot.put(asList(new Tag("key", "one"), new Tag("targetServer", "localhost"), new Tag("percentile", "50")), 1);
    expectedSnapshot.put(asList(new Tag("key", "one"), new Tag("targetServer", "localhost"), new Tag("percentile", "97")), 1);
    expectedSnapshot.put(asList(new Tag("key", "one"), new Tag("targetServer", "localhost"), new Tag("percentile", "99")), 1);
    expectedSnapshot.put(asList(new Tag("key", "one"), new Tag("targetServer", "localhost"), new Tag("percentile", "100")), 1);
    expectedSnapshot.put(asList(new Tag("key", "two"), new Tag("targetServer", "localhost"), new Tag("percentile", "50")), 3);
    expectedSnapshot.put(asList(new Tag("key", "two"), new Tag("targetServer", "localhost"), new Tag("percentile", "97")), 3);
    expectedSnapshot.put(asList(new Tag("key", "two"), new Tag("targetServer", "localhost"), new Tag("percentile", "99")), 3);
    expectedSnapshot.put(asList(new Tag("key", "two"), new Tag("targetServer", "localhost"), new Tag("percentile", "100")), 3);
    expectedSnapshot.put(asList(new Tag("key", "two"), new Tag("targetServer", "google"), new Tag("percentile", "50")), 2);
    expectedSnapshot.put(asList(new Tag("key", "two"), new Tag("targetServer", "google"), new Tag("percentile", "97")), 2);
    expectedSnapshot.put(asList(new Tag("key", "two"), new Tag("targetServer", "google"), new Tag("percentile", "99")), 2);
    expectedSnapshot.put(asList(new Tag("key", "two"), new Tag("targetServer", "google"), new Tag("percentile", "100")), 2);
    expectedSnapshot.put(asList(new Tag("key", "three"), new Tag("targetServer", "google"), new Tag("percentile", "50")), 4);
    expectedSnapshot.put(asList(new Tag("key", "three"), new Tag("targetServer", "google"), new Tag("percentile", "97")), 4);
    expectedSnapshot.put(asList(new Tag("key", "three"), new Tag("targetServer", "google"), new Tag("percentile", "99")), 4);
    expectedSnapshot.put(asList(new Tag("key", "three"), new Tag("targetServer", "google"), new Tag("percentile", "100")), 4);

    assertEquals(expectedSnapshot, percentileAggregator.getSnapshotAndReset());
  }

  @Test
  public void sendPercentilesTwoTargetServerDifferentOrder() {
    percentileAggregator.increaseMetric(1, new Tag("targetServer", "localhost"), new Tag("key", "one"));
    percentileAggregator.increaseMetric(1, new Tag("targetServer", "localhost"), new Tag("key", "one"));
    percentileAggregator.increaseMetric(3, new Tag("key", "one"), new Tag("targetServer", "localhost"));

    Map<List<Tag>, Integer> expectedSnapshot = new HashMap<>();
    expectedSnapshot.put(asList(new Tag("key", "one"), new Tag("targetServer", "localhost"), new Tag("percentile", "50")), 1);
    expectedSnapshot.put(asList(new Tag("key", "one"), new Tag("targetServer", "localhost"), new Tag("percentile", "97")), 3);
    expectedSnapshot.put(asList(new Tag("key", "one"), new Tag("targetServer", "localhost"), new Tag("percentile", "99")), 3);
    expectedSnapshot.put(asList(new Tag("key", "one"), new Tag("targetServer", "localhost"), new Tag("percentile", "100")), 3);

    assertEquals(expectedSnapshot, percentileAggregator.getSnapshotAndReset());
  }

  @Test
  public void sendMetricsTwoThreads() throws InterruptedException {

    IncreaseMetricInThread increaseMetricInThread =
            new IncreaseMetricInThread(1, new Tag("targetServer", "localhost"));

    increaseMetricInThread.start();

    for (int i = 0; i < 495000; i++) {
      percentileAggregator.increaseMetric(1, new Tag("targetServer", "localhost"));
    }
    increaseMetricInThread.join();
    for (int i = 0; i < 10000; i++) {
      percentileAggregator.increaseMetric(2, new Tag("targetServer", "localhost"));
    }

    Map<List<Tag>, Integer> expectedSnapshot = new HashMap<>();
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "50")), 1);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "97")), 1);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "99")), 1);
    expectedSnapshot.put(asList(new Tag("targetServer", "localhost"), new Tag("percentile", "100")), 2);

    assertEquals(expectedSnapshot, percentileAggregator.getSnapshotAndReset());
  }

  class IncreaseMetricInThread extends Thread {
    private final Tag[] tags;
    private final int metricValue;

    IncreaseMetricInThread(int metricValue, Tag... tags) {
      this.tags = tags;
      this.metricValue = metricValue;
    }

    @Override
    public void run() {
      for (int i = 0; i < 495000; i++) {
        percentileAggregator.increaseMetric(metricValue, tags);
      }
    }
  }
}
