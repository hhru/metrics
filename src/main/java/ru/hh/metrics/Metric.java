package ru.hh.metrics;

public class Metric {
  private final int value;
  private final Tags tags;

  public static Metric of(int value, Tag... tags) {
    return new Metric(value, new MultiTags(tags));
  }

  private Metric(int value, MultiTags tags) {
    this.value = value;
    this.tags = tags;
  }

  public int getValue() {
    return value;
  }

  public Tag[] getTags() {
    return tags.getTags();
  }
}
