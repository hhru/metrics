package ru.hh.metrics;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CounterAggregatorTest {
  private final CounterAggregator counterAggregator = new CounterAggregator(300);

  @Test
  public void sendMetricsWithoutValues() {
    assertTrue(counterAggregator.getSnapshotAndReset().isEmpty());
  }

  @Test
  public void sendMetricsOneValue() {
    Tag tag = new Tag("label", "first");
    counterAggregator.increaseMetric(5, tag);
    Map<List<Tag>, Integer> tagsToValue;
    List<Tag> tags = singletonList(tag);

    tagsToValue = counterAggregator.getSnapshotAndReset();
    assertEquals(1, tagsToValue.size());
    assertEquals(5, tagsToValue.get(tags).intValue());

    tagsToValue = counterAggregator.getSnapshotAndReset();
    assertEquals(1, tagsToValue.size());
    assertEquals(0,  tagsToValue.get(tags).intValue());

    tagsToValue = counterAggregator.getSnapshotAndReset();
    assertTrue(tagsToValue.isEmpty());
  }

  @Test
  public void sendMetricsDifferentOrderOfTags() {
    counterAggregator.increaseMetric(5, new Tag("label", "first"), new Tag("notlabel", "a"));
    counterAggregator.increaseMetric(3, new Tag("notlabel", "a"), new Tag("label", "first"));

    Map<List<Tag>, Integer> expectedSnapshot = new HashMap<>();
    expectedSnapshot.put(asList(new Tag("label", "first"), new Tag("notlabel", "a")), 8);

    assertEquals(expectedSnapshot, counterAggregator.getSnapshotAndReset());
  }

  @Test
  public void sendMetricsTwoTags() {
    counterAggregator.increaseMetric(5, new Tag("label", "first"), new Tag("region", "vacancy"));
    counterAggregator.increaseMetric(2, new Tag("label", "first"), new Tag("region", "resume"));
    counterAggregator.increaseMetric(6, new Tag("label", "second"), new Tag("region", "resume"));
    counterAggregator.increaseMetric(11, new Tag("label", "third"), new Tag("region", "resume"));

    Map<List<Tag>, Integer> expectedSnapshot = new HashMap<>();
    expectedSnapshot.put(asList(new Tag("label", "first"), new Tag("region", "vacancy")), 5);
    expectedSnapshot.put(asList(new Tag("label", "first"), new Tag("region", "resume")), 2);
    expectedSnapshot.put(asList(new Tag("label", "second"), new Tag("region", "resume")), 6);
    expectedSnapshot.put(asList(new Tag("label", "third"), new Tag("region", "resume")), 11);

    assertEquals(expectedSnapshot, counterAggregator.getSnapshotAndReset());
  }

  @Test
  public void sendMetricsTwoThreads() throws InterruptedException {
    int increases = 1_000_000;
    Tag tag = new Tag("label", "first");
    List<Tag> tags = singletonList(tag);
    Runnable increaseMetricTask = () -> {
      for (int i = 0; i < increases; i++) {
        counterAggregator.increaseMetric(1, tag);
      }
    };

    int tests = 100;
    for (int t = 1; t <= tests; t++) {
      long start = currentTimeMillis();
      List<Map<List<Tag>, Integer>> snapshots = new ArrayList<>();

      Thread increaseMetricThread = new Thread(increaseMetricTask);
      increaseMetricThread.start();

      for (int i = 0; i < increases; i++) {
        counterAggregator.increaseMetric(1, tag);
        if (i % 1000 == 0) {
          snapshots.add(counterAggregator.getSnapshotAndReset());
        }
      }

      increaseMetricThread.join();
      snapshots.add(counterAggregator.getSnapshotAndReset());

      int sum = 0;
      for (Map<List<Tag>, Integer> snapshot : snapshots) {
        Integer snapshotValue = snapshot.get(tags);
        if (snapshotValue != null) {
          sum += snapshotValue;
        }
      }

      assertEquals(increases * 2, sum);

      System.out.println("finished iteration " + t + " out of " + tests + " in " + (currentTimeMillis() - start) + " ms");
    }
  }

}
