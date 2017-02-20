package ru.hh.metrics;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static ru.hh.metrics.TestUtils.tagsOf;

public class CounterAggregatorTest {
  private final CounterAggregator counterAggregator = new CounterAggregator(300);

  @Test
  public void sendMetricsWithoutValues() {
    assertTrue(counterAggregator.getSnapshotAndReset().isEmpty());
  }

  @Test
  public void sendMetricsOneValue() {
    Tag tag = new Tag("label", "first");
    counterAggregator.add(5, tag);
    Map<Tags, Integer> tagsToValue;

    tagsToValue = counterAggregator.getSnapshotAndReset();
    assertEquals(1, tagsToValue.size());
    assertEquals(5, tagsToValue.get(tag).intValue());

    tagsToValue = counterAggregator.getSnapshotAndReset();
    assertEquals(1, tagsToValue.size());
    assertEquals(0,  tagsToValue.get(tag).intValue());

    tagsToValue = counterAggregator.getSnapshotAndReset();
    assertTrue(tagsToValue.isEmpty());
  }

  @Test
  public void sendMetricsDifferentOrderOfTags() {
    counterAggregator.add(5, new Tag("label", "first"), new Tag("notlabel", "a"));
    counterAggregator.add(3, new Tag("notlabel", "a"), new Tag("label", "first"));

    Map<Tags, Integer> tagsToCount = counterAggregator.getSnapshotAndReset();

    assertEquals(1, tagsToCount.size());
    assertEquals(8, tagsToCount.get(tagsOf(new Tag("label", "first"), new Tag("notlabel", "a"))).intValue());
  }

  @Test
  public void sendMetricsTwoTags() {
    counterAggregator.add(5, new Tag("label", "first"), new Tag("region", "vacancy"));
    counterAggregator.add(2, new Tag("label", "first"), new Tag("region", "resume"));
    counterAggregator.add(6, new Tag("label", "second"), new Tag("region", "resume"));
    counterAggregator.add(11, new Tag("label", "third"), new Tag("region", "resume"));

    Map<Tags, Integer> tagsToCount = counterAggregator.getSnapshotAndReset();

    assertEquals(4, tagsToCount.size());
    assertEquals(5, tagsToCount.get(tagsOf(new Tag("label", "first"), new Tag("region", "vacancy"))).intValue());
    assertEquals(2, tagsToCount.get(tagsOf(new Tag("label", "first"), new Tag("region", "resume"))).intValue());
    assertEquals(6, tagsToCount.get(tagsOf(new Tag("label", "second"), new Tag("region", "resume"))).intValue());
    assertEquals(11, tagsToCount.get(tagsOf(new Tag("label", "third"), new Tag("region", "resume"))).intValue());
  }

  @Test
  public void sendMetricsTwoThreads() throws InterruptedException {
    int increases = 1_000_000;
    Tag tag = new Tag("label", "first");
    Runnable increaseMetricTask = () -> {
      for (int i = 0; i < increases; i++) {
        counterAggregator.add(1, tag);
      }
    };

    int tests = 100;
    for (int t = 1; t <= tests; t++) {
      long start = currentTimeMillis();
      List<Map<Tags, Integer>> snapshots = new ArrayList<>();

      Thread increaseMetricThread = new Thread(increaseMetricTask);
      increaseMetricThread.start();

      for (int i = 0; i < increases; i++) {
        counterAggregator.add(1, tag);
        if (i % 1000 == 0) {
          snapshots.add(counterAggregator.getSnapshotAndReset());
        }
      }

      increaseMetricThread.join();
      snapshots.add(counterAggregator.getSnapshotAndReset());

      int sum = 0;
      for (Map<Tags, Integer> snapshot : snapshots) {
        Integer snapshotValue = snapshot.get(tag);
        if (snapshotValue != null) {
          sum += snapshotValue;
        }
      }

      assertEquals(increases * 2, sum);

      System.out.println("finished iteration " + t + " out of " + tests + " in " + (currentTimeMillis() - start) + " ms");
    }
  }

}
