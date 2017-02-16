package ru.hh.metrics;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class StatsDSenderTest {
  @Test
  public void testGetTagStringNoTags() {
    assertEquals("", StatsDSender.getTagString(emptyList()));
  }

  @Test
  public void testGetTagStringOneTag() {
    assertEquals("label_is_right", StatsDSender.getTagString(singletonList(new Tag("label", "right"))));
  }

  @Test
  public void testGetTagStringTwoTags() {
    assertEquals("label_is_right.answer_is_42", StatsDSender.getTagString(
            asList(new Tag("label", "right"), new Tag("answer", "42"))
    ));
  }

  @Test
  public void testGetTagStringTwoTagsAnotherOrder() {
    assertEquals("answer_is_42.label_is_right", StatsDSender.getTagString(
            asList(new Tag("answer", "42"), new Tag("label", "right"))
    ));
  }
}
