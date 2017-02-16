package ru.hh.metrics;

import java.util.Objects;

public class Tag implements Comparable<Tag> {
  public final String name;
  public final String value;

  public Tag(String name, String value) {
    this.name = name;
    this.value = value;
  }

  @Override
  public String toString() {
    return name + ": " + value;
  }

  @Override
  public int compareTo(Tag otherTag) {
    return this.name.compareTo(otherTag.name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Tag tag = (Tag) o;
    return name.equals(tag.name) && value.equals(tag.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }
}
