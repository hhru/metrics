package ru.hh.metrics;

abstract class Tags {

  abstract Tag[] getTags();

  static Tags of(Tag[] tags) {
    if (tags.length == 1) {
      return tags[0];
    } else {
      return new MultiTags(tags);
    }
  }

}
