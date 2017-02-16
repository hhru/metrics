package ru.hh.metrics;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StatsDSenderTest {
  @Test
  public void testGetTagStringNoTags() {
    assertEquals("", StatsDSender.getTagString(new Tag[]{}));
  }

  @Test
  public void testGetTagStringOneTag() {
    assertEquals("label_is_right", StatsDSender.getTagString(new Tag[]{new Tag("label", "right")}));
  }

  @Test
  public void testGetTagStringTwoTags() {
    assertEquals("label_is_right.answer_is_42", StatsDSender.getTagString(
            new Tag[]{new Tag("label", "right"), new Tag("answer", "42")}
    ));
  }

  @Test
  public void testGetTagStringTwoTagsAnotherOrder() {
    assertEquals("answer_is_42.label_is_right", StatsDSender.getTagString(
            new Tag[]{new Tag("answer", "42"), new Tag("label", "right")}
    ));
  }
}
