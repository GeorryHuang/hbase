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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class RangeReferenceStoreFileReader extends StoreFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(RangeReferenceStoreFileReader.class);
  protected final byte [] startKey;
  protected final byte [] stopKey;
  private Cell startCell;
  private Cell stopCell;


  /**
   * Creates a half file reader for a hfile referred to by an hfilelink.
   * @param context Reader context info
   * @param fileInfo HFile info
   * @param cacheConf CacheConfig
   * @param r original reference file (contains top or bottom)
   * @param refCount reference count
   * @param conf Configuration
   */
  public RangeReferenceStoreFileReader(final ReaderContext context, final HFileInfo fileInfo,
    final CacheConfig cacheConf, final RangeReference r,
    AtomicInteger refCount, final Configuration conf) throws IOException {
    super(context, fileInfo, cacheConf, refCount, conf);
    this.startKey = r.getStartKey();
    this.stopKey = r.getStopKey();
    this.startCell = new KeyValue.KeyOnlyKeyValue(this.startKey, 0, this.startKey.length);
    this.stopCell = new KeyValue.KeyOnlyKeyValue(this.stopKey, 0, this.stopKey.length);
  }


  @Override
  public HFileScanner getScanner(final boolean cacheBlocks,
    final boolean pread, final boolean isCompaction) {
    final HFileScanner s = super.getScanner(cacheBlocks, pread, isCompaction);
    return new HFileScanner() {
      final HFileScanner delegate = s;
      public boolean atEnd = false;

      @Override
      public Cell getKey() {
        if (atEnd) return null;
        return delegate.getKey();
      }

      @Override
      public String getKeyString() {
        if (atEnd) return null;

        return delegate.getKeyString();
      }

      @Override
      public ByteBuffer getValue() {
        if (atEnd) return null;

        return delegate.getValue();
      }

      @Override
      public String getValueString() {
        if (atEnd) return null;

        return delegate.getValueString();
      }

      @Override
      public Cell getCell() {
        if (atEnd) return null;

        return delegate.getCell();
      }

      @Override
      public boolean next() throws IOException {
        if (atEnd) return false;

        boolean b = delegate.next();
        if (!b) {
          return b;
        }
        // constrain the bottom.
        if (!top) {
          if (getComparator().compare(splitCell, getKey()) <= 0) {
            atEnd = true;
            return false;
          }
        }
        return true;
      }

      @Override public boolean seekTo() throws IOException {
        int r;
        CellComparator cellComparator = this.delegate.getReader().getComparator();
        if(startKey == null){
          this.delegate.seekTo();
          return cellComparator.compare(stopCell, this.delegate.getKey())>0;
        }else {
          this.delegate.seekTo(startCell);
        }
        this.delegate.seekTo();
        }

        int r = this.delegate.seekTo(startCell);
        if (r == HConstants.INDEX_KEY_MAGIC) {
          return true;
        }
        if (r < 0) {
          //startKey < first key in file
          return this.delegate.seekTo();
        }
        if (r > 0) {
          return this.delegate.next();
        }
        return true;
      }

      @Override
      public org.apache.hadoop.hbase.io.hfile.HFile.Reader getReader() {
        return this.delegate.getReader();
      }

      @Override
      public boolean isSeeked() {
        return this.delegate.isSeeked();
      }

      @Override
      public int seekTo(Cell key) throws IOException {

        if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) < 0) {
            return -1;
        }

          if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) >= 0) {
            // we would place the scanner in the second half.
            // it might be an error to return false here ever...
            boolean res = delegate.seekBefore(splitCell);
            if (!res) {
              throw new IOException(
                "Seeking for a key in bottom of file, but key exists in top of file, " +
                  "failed on seekBefore(midkey)");
            }
            return 1;
          }
        }
        return delegate.seekTo(key);
      }

      @Override
      public int reseekTo(Cell key) throws IOException {
        // This function is identical to the corresponding seekTo function
        // except
        // that we call reseekTo (and not seekTo) on the delegate.
        if (top) {
          if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) < 0) {
            return -1;
          }
        } else {
          if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) >= 0) {
            // we would place the scanner in the second half.
            // it might be an error to return false here ever...
            boolean res = delegate.seekBefore(splitCell);
            if (!res) {
              throw new IOException("Seeking for a key in bottom of file, but"
                + " key exists in top of file, failed on seekBefore(midkey)");
            }
            return 1;
          }
        }
        if (atEnd) {
          // skip the 'reseek' and just return 1.
          return 1;
        }
        return delegate.reseekTo(key);
      }

      @Override
      public boolean seekBefore(Cell key) throws IOException {
        if (top) {
          Optional<Cell> fk = getFirstKey();
          if (fk.isPresent() &&
            PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, fk.get()) <= 0) {
            return false;
          }
        } else {
          // The equals sign isn't strictly necessary just here to be consistent
          // with seekTo
          if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) >= 0) {
            boolean ret = this.delegate.seekBefore(splitCell);
            if (ret) {
              atEnd = false;
            }
            return ret;
          }
        }
        boolean ret = this.delegate.seekBefore(key);
        if (ret) {
          atEnd = false;
        }
        return ret;
      }

      @Override
      public Cell getNextIndexedKey() {
        return null;
      }

      @Override
      public void close() {
        this.delegate.close();
      }

      @Override
      public void shipped() throws IOException {
        this.delegate.shipped();
      }
    };
  }

  @Override
  public boolean passesKeyRangeFilter(Scan scan) {
    return true;
  }

  @Override
  public Optional<Cell> getLastKey() {
    if (top) {
      return super.getLastKey();
    }
    // Get a scanner that caches the block and that uses pread.
    HFileScanner scanner = getScanner(true, true);
    try {
      if (scanner.seekBefore(this.splitCell)) {
        return Optional.ofNullable(scanner.getKey());
      }
    } catch (IOException e) {
      LOG.warn("Failed seekBefore " + Bytes.toStringBinary(this.splitkey), e);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<Cell> midKey() throws IOException {
    // Returns null to indicate file is not splitable.
    return Optional.empty();
  }

  @Override
  public Optional<Cell> getFirstKey() {
    if (!firstKeySeeked) {
      HFileScanner scanner = getScanner(true, true, false);
      try {
        if (scanner.seekTo()) {
          this.firstKey = Optional.ofNullable(scanner.getKey());
        }
        firstKeySeeked = true;
      } catch (IOException e) {
        LOG.warn("Failed seekTo first KV in the file", e);
      } finally {
        if(scanner != null) {
          scanner.close();
        }
      }
    }
    return this.firstKey;
  }

  @Override
  public long getEntries() {
    // Estimate the number of entries as half the original file; this may be wildly inaccurate.
    return super.getEntries() / 2;
  }

  @Override
  public long getFilterEntries() {
    // Estimate the number of entries as half the original file; this may be wildly inaccurate.
    return super.getFilterEntries() / 2;
  }
}

