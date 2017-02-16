package ru.hh.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.lang.System.currentTimeMillis;

public class CounterAggregatorPerfTest {

  private static final int tests = 20;
  private static final int increases = 100_000_000;
  private static final int snapshotIteration = 100_000;
  private static final String[] tagValues = createTagValues(5);

  private static String[] createTagValues(int numOfTagValues) {
    String[] tagValues = new String[numOfTagValues];
    for (int i=0; i<tagValues.length; i++) {
      tagValues[i] = Integer.toString(i);
    }
    return tagValues;
  }

  public static void main(String[] args) throws InterruptedException {
    for(int t=1; t<=tests; t++) {
      test(t);
    }
  }

  private static void test(int testIteration) throws InterruptedException {
    CounterAggregator counterAggregator = new CounterAggregator(300);

    Runnable addTask = () -> {
      for (int i = 1; i <= increases; i++) {
        add(counterAggregator, i);
      }
    };
    Thread increaseMetricThread = new Thread(addTask);

    Collection<Map<Tags, Integer>> snapshots = new ArrayList<>(increases / snapshotIteration);

    long start = currentTimeMillis();

    increaseMetricThread.start();

    for (int i = 1; i <= increases; i++) {
      add(counterAggregator, i);
      if (i % snapshotIteration == 0) {
        snapshots.add(counterAggregator.getSnapshotAndReset());
      }
    }

    increaseMetricThread.join();

    System.out.println("CounterAggregator " + testIteration + " " + (currentTimeMillis() - start) + " ms");

    snapshots.add(counterAggregator.getSnapshotAndReset());
    checkSnapshots(snapshots);
  }

  private static void add(CounterAggregator counterAggregator, int iteration) {
    counterAggregator.add(1, createTag(iteration % tagValues.length));
  }

  private static Tag createTag(int tagValueIndex) {
    return new Tag("label", tagValues[tagValueIndex]);
  }

  private static void checkSnapshots(Collection<Map<Tags, Integer>> snapshots) {
    Map<Tags, Integer> tagsToValue = merge(snapshots);
    for (int i = 0; i<tagValues.length; i++) {
      int expected = increases * 2 / tagValues.length;
      int actual = tagsToValue.get(new Tags(new Tag[]{createTag(i)}));
      if (actual != expected) {
        throw new IllegalStateException("tag " + i + " expected " + expected + " got " + actual);
      }
    }
  }

  private static Map<Tags, Integer> merge(Collection<Map<Tags, Integer>> snapshots) {
    Map<Tags, Integer> tagsToTotalValue = new HashMap<>();
    for (Map<Tags, Integer> snapshot : snapshots) {
      for (Map.Entry<Tags, Integer> tagsAndSnapshotValue : snapshot.entrySet()) {
        Integer totalValue = tagsToTotalValue.get(tagsAndSnapshotValue.getKey());
        if (totalValue == null) {
          totalValue = 0;
        }
        tagsToTotalValue.put(tagsAndSnapshotValue.getKey(), totalValue + tagsAndSnapshotValue.getValue());
      }
    }
    return tagsToTotalValue;
  }

}
