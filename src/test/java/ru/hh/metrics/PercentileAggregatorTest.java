package ru.hh.metrics;

import org.junit.Test;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static ru.hh.metrics.TestUtils.tagsOf;

public class PercentileAggregatorTest {

  private final PercentileAggregator percentileAggregator = new PercentileAggregator();

  @Test
  public void differentValues() throws InterruptedException {
    for (int i = 0; i < 50; i++) {
      percentileAggregator.save(100, new Tag("targetServer", "localhost"));
    }
    for (int i = 0; i < 45; i++) {
      percentileAggregator.save(200, new Tag("targetServer", "localhost"));
    }
    for (int i = 0; i < 2; i++) {
      percentileAggregator.save(300, new Tag("targetServer", "localhost"));
    }
    for (int i = 0; i < 2; i++) {
      percentileAggregator.save(400, new Tag("targetServer", "localhost"));
    }

    Map<Tags, Integer> tagsToValue = percentileAggregator.getSnapshotAndReset();

    assertEquals(4, tagsToValue.size());
    assertEquals(
        100,
        tagsToValue.get(tagsOf(new Tag("targetServer", "localhost"), new Tag("percentile", "50"))).intValue()
    );
    assertEquals(
        300,
        tagsToValue.get(tagsOf(new Tag("targetServer", "localhost"), new Tag("percentile", "97"))).intValue()
    );
    assertEquals(
        400,
        tagsToValue.get(tagsOf(new Tag("targetServer", "localhost"), new Tag("percentile", "99"))).intValue()
    );
    assertEquals(
        400,
        tagsToValue.get(tagsOf(new Tag("targetServer", "localhost"), new Tag("percentile", "100"))).intValue()
    );
  }

  @Test
  public void noValue() {
    assertEquals(emptyMap(), percentileAggregator.getSnapshotAndReset());
  }

  @Test
  public void oneValue() {
    percentileAggregator.save(1, new Tag("targetServer", "localhost"));

    Map<Tags, Integer> tagsToValue = percentileAggregator.getSnapshotAndReset();

    assertEquals(4, tagsToValue.size());
    assertEquals(
        1,
        tagsToValue.get(tagsOf(new Tag("targetServer", "localhost"), new Tag("percentile", "50"))).intValue()
    );
    assertEquals(
        1,
        tagsToValue.get(tagsOf(new Tag("targetServer", "localhost"), new Tag("percentile", "97"))).intValue()
    );
    assertEquals(
        1,
        tagsToValue.get(tagsOf(new Tag("targetServer", "localhost"), new Tag("percentile", "99"))).intValue()
    );
    assertEquals(
        1,
        tagsToValue.get(tagsOf(new Tag("targetServer", "localhost"), new Tag("percentile", "100"))).intValue()
    );
  }

  @Test
  public void twoTags() {
    percentileAggregator.save(1, new Tag("targetServer", "localhost"), new Tag("key", "one"));
    percentileAggregator.save(2, new Tag("targetServer", "google"), new Tag("key", "two"));
    percentileAggregator.save(3, new Tag("targetServer", "localhost"), new Tag("key", "two"));
    percentileAggregator.save(4, new Tag("targetServer", "google"), new Tag("key", "three"));

    Map<Tags, Integer> tagsToValue = percentileAggregator.getSnapshotAndReset();

    assertEquals(
        1,
        tagsToValue.get(tagsOf(new Tag("key", "one"), new Tag("targetServer", "localhost"), new Tag("percentile", "50"))).intValue()
    );
    assertEquals(
        1,
        tagsToValue.get(tagsOf(new Tag("key", "one"), new Tag("targetServer", "localhost"), new Tag("percentile", "100"))).intValue()
    );
    assertEquals(
        3,
        tagsToValue.get(tagsOf(new Tag("key", "two"), new Tag("targetServer", "localhost"), new Tag("percentile", "50"))).intValue()
    );
    assertEquals(
        3,
        tagsToValue.get(tagsOf(new Tag("key", "two"), new Tag("targetServer", "localhost"), new Tag("percentile", "100"))).intValue()
    );
    assertEquals(
        2,
        tagsToValue.get(tagsOf(new Tag("key", "two"), new Tag("targetServer", "google"), new Tag("percentile", "50"))).intValue()
    );
    assertEquals(
        2,
        tagsToValue.get(tagsOf(new Tag("key", "two"), new Tag("targetServer", "google"), new Tag("percentile", "100"))).intValue()
    );
    assertEquals(
        4,
        tagsToValue.get(tagsOf(new Tag("key", "three"), new Tag("targetServer", "google"), new Tag("percentile", "50"))).intValue()
    );
    assertEquals(
        4,
        tagsToValue.get(tagsOf(new Tag("key", "three"), new Tag("targetServer", "google"), new Tag("percentile", "100"))).intValue()
    );
  }

  @Test
  public void tagsInDifferentOrder() {
    percentileAggregator.save(1, new Tag("targetServer", "localhost"), new Tag("key", "one"));
    percentileAggregator.save(1, new Tag("targetServer", "localhost"), new Tag("key", "one"));
    percentileAggregator.save(3, new Tag("key", "one"), new Tag("targetServer", "localhost"));

    Map<Tags, Integer> tagsToValue = percentileAggregator.getSnapshotAndReset();

    assertEquals(
        1,
        tagsToValue.get(tagsOf(new Tag("key", "one"), new Tag("targetServer", "localhost"), new Tag("percentile", "50"))).intValue()
    );
    assertEquals(
        3,
        tagsToValue.get(tagsOf(new Tag("key", "one"), new Tag("targetServer", "localhost"), new Tag("percentile", "97"))).intValue()
    );
    assertEquals(
        3,
        tagsToValue.get(tagsOf(new Tag("key", "one"), new Tag("targetServer", "localhost"), new Tag("percentile", "99"))).intValue()
    );
    assertEquals(
        3,
        tagsToValue.get(tagsOf(new Tag("key", "one"), new Tag("targetServer", "localhost"), new Tag("percentile", "100"))).intValue()
    );
  }

  @Test
  public void twoThreads() throws InterruptedException {

    IncreaseMetricInThread increaseMetricInThread =
            new IncreaseMetricInThread(1, new Tag("targetServer", "localhost"));

    increaseMetricInThread.start();

    for (int i = 0; i < 495000; i++) {
      percentileAggregator.save(1, new Tag("targetServer", "localhost"));
    }
    percentileAggregator.save(2, new Tag("targetServer", "localhost"));
    increaseMetricInThread.join();

    Map<Tags, Integer> tagsToValue = percentileAggregator.getSnapshotAndReset();

    assertEquals(1, tagsToValue.get(tagsOf(new Tag("targetServer", "localhost"), new Tag("percentile", "50"))).intValue());
    assertEquals(1, tagsToValue.get(tagsOf(new Tag("targetServer", "localhost"), new Tag("percentile", "97"))).intValue());
    assertEquals(1, tagsToValue.get(tagsOf(new Tag("targetServer", "localhost"), new Tag("percentile", "99"))).intValue());
    assertEquals(2, tagsToValue.get(tagsOf(new Tag("targetServer", "localhost"), new Tag("percentile", "100"))).intValue());
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
        percentileAggregator.save(metricValue, tags);
      }
    }
  }
}
