/**
 *
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
package org.apache.hadoop.hbase.io;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FSProtos;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * TODO:
 */
@InterfaceAudience.Private
public class RangeReference {
  private byte [] startKey;
  private byte [] stopKey;

  public byte[] getStartKey() {
    return startKey;
  }

  public byte[] getStopKey() {
    return stopKey;
  }

  public static RangeReference createRangeReference(final byte [] startKey, final byte[] stopKey) {

    return new RangeReference(startKey, stopKey);
  }


  private RangeReference(){}

  RangeReference(final byte [] startKey, final byte [] stopKey) {
    this.startKey = startKey;
    this.stopKey = stopKey;
  }

  public Path write(final FileSystem fs, final Path p)
    throws IOException {
    FSDataOutputStream out = fs.create(p, false);
    try {
      out.write(toByteArray());
    } finally {
      out.close();
    }
    return p;
  }

  /**
   * Read a Reference from FileSystem.
   * @param fs
   * @param p
   * @return New Reference made from passed <code>p</code>
   * @throws IOException
   */
  public static RangeReference read(final FileSystem fs, final Path p)
    throws IOException {
    InputStream in = fs.open(p);
    try {
      // I need to be able to move back in the stream if this is not a pb serialization so I can
      // do the Writable decoding instead.
      in = in.markSupported()? in: new BufferedInputStream(in);
      int pblen = ProtobufUtil.lengthOfPBMagic();
      in.mark(pblen);
      byte [] pbuf = new byte[pblen];
      int read = in.read(pbuf);
      if (read != pblen) {
        throw new IOException("read=" + read + ", wanted=" + pblen);
      }
      // WATCHOUT! Return in middle of function!!!
      if (ProtobufUtil.isPBMagicPrefix(pbuf)) return convert(FSProtos.RangeReference.parseFrom(in));
      // Else presume Writables.  Need to reset the stream since it didn't start w/ pb.
      // We won't bother rewriting thie Reference as a pb since Reference is transitory.
      in.reset();
      RangeReference r = new RangeReference();
      DataInputStream dis = new DataInputStream(in);
      // Set in = dis so it gets the close below in the finally on our way out.
      in = dis;
      r.readFields(dis);
      return r;
    } finally {
      in.close();
    }
  }

  @Deprecated
  public void readFields(DataInput in) throws IOException {
    this.startKey = Bytes.readByteArray(in);
    this.stopKey = Bytes.readByteArray(in);
  }

  public FSProtos.RangeReference convert() {
    FSProtos.RangeReference.Builder builder = FSProtos.RangeReference.newBuilder();

    //TODO:What is UnsafeByte Operations?
    builder.setStopKey(UnsafeByteOperations.unsafeWrap(stopKey));
    builder.setStopKey(UnsafeByteOperations.unsafeWrap(stopKey));
    return builder.build();
  }

  public static RangeReference convert(final FSProtos.RangeReference r) {
    RangeReference result = new RangeReference();
    result.startKey = r.getStartKey().toByteArray();
    result.stopKey = r.getStopKey().toByteArray();
    return result;
  }

  /**
   * Use this when writing to a stream and you want to use the pb mergeDelimitedFrom
   * (w/o the delimiter, pb reads to EOF which may not be what you want).
   * @return This instance serialized as a delimited protobuf w/ a magic pb prefix.
   * @throws IOException
   */
  byte [] toByteArray() throws IOException {
    return ProtobufUtil.prependPBMagic(convert().toByteArray());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(startKey) + Arrays.hashCode(stopKey);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null) return false;
    if (!(o instanceof RangeReference)) return false;

    RangeReference r = (RangeReference) o;
    if(!startKey.equals(r.startKey)){
      return false;
    }
    return stopKey.equals(r.stopKey);
  }
}
