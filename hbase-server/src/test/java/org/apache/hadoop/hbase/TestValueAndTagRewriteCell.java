/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.nio.ByteBuff;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.IOException;
import java.nio.ByteBuffer;

@Category(SmallTests.class)
public class TestValueAndTagRewriteCell {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestValueAndTagRewriteCell.class);

  @Test
  public void testCellSize() throws IOException {
    Cell originalCell = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
      .setRow(Bytes.toBytes("row"))
      .setFamily(HConstants.EMPTY_BYTE_ARRAY)
      .setQualifier(HConstants.EMPTY_BYTE_ARRAY)
      .setTimestamp(HConstants.LATEST_TIMESTAMP)
      .setType(KeyValue.Type.Maximum.getCode())
      .setValue(Bytes.toBytes("value"))
      .build();

    ByteArrayOutputStream output = new ByteArrayOutputStream(300);
    Cell cell = PrivateCellUtil.createCell(originalCell, Bytes.toBytes("value"), "".getBytes());
    System.out.println(cell.heapSize());
    PrivateCellUtil.writeCell(cell,output,false);
    System.out.println(output.size());

    ByteBuffer buffer = ByteBuffer.allocate(300);
    System.out.println(cell.heapSize());
    PrivateCellUtil.writeCellToBuffer(cell,buffer,0);
    System.out.println(buffer.capacity());

  }
}
