package ru.hh.metrics;

class TestUtils {

  static Tags tagsOf(Tag... tags) {
    return new MultiTags(tags);
  }

  private TestUtils(){}
}
