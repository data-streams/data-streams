package com.spbsu.flamestream.core.data.invalidation;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.SerializableComparator;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class ArrayInvalidatingBucketTest extends FlameStreamSuite {
  @Test
  public void testInvalidCustomOrder() {
    final ArrayInvalidatingBucket bucket =
            new ArrayInvalidatingBucket(SerializableComparator.comparing(dataItem -> dataItem.payload(Integer.class)));
    final Meta meta = new Meta(new GlobalTime(0, EdgeId.MIN));
    bucket.insert(new PayloadDataItem(meta, 0));
    assertThrows(() -> bucket.insert(new PayloadDataItem(new Meta(meta, 2, 0), -1)));
  }

  @Test
  public void testValidCustomOrder() {
    final ArrayInvalidatingBucket bucket =
            new ArrayInvalidatingBucket(SerializableComparator.comparing(dataItem -> dataItem.payload(Integer.class)));
    final Meta meta = new Meta(new GlobalTime(0, EdgeId.MIN));
    final PayloadDataItem first = new PayloadDataItem(meta, 0);
    final PayloadDataItem second = new PayloadDataItem(new Meta(meta, 2, 1), 1);
    final PayloadDataItem thirdWrong = new PayloadDataItem(new Meta(meta, 3, 0), 3);
    final PayloadDataItem third = new PayloadDataItem(new Meta(meta, 2, 0), 2);
    bucket.insert(first);
    bucket.insert(thirdWrong);
    bucket.insert(third);
    bucket.insert(second);
    final ArrayList<DataItem> actual = new ArrayList<>();
    bucket.forRange(0, 4, actual::add);
    assertEquals(actual, Arrays.asList(first, second, third, thirdWrong));
  }
}
