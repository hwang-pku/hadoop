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
package org.apache.hadoop.io.compress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class TestBlockDecompressorStream {
  
  private byte[] buf;
  private ByteArrayInputStream bytesIn;
  private ByteArrayOutputStream bytesOut;

  @Test
  @Parameters({
  "0, 1024",
  "4, 2048",
  "-100, -200",
  "555, 0",
  "-277, 100"
  })
  public void testRead(int bufLen, int buffSize) throws IOException {
    // compress empty stream
    Assume.assumeTrue(buffSize > 0);
    bytesOut = new ByteArrayOutputStream();
    if (bufLen > 0) {
      bytesOut.write(ByteBuffer.allocate(bufLen).putInt(1024).array(), 0,
          bufLen);
    }
    BlockCompressorStream blockCompressorStream = 
      new BlockCompressorStream(bytesOut, 
          new FakeCompressor(), buffSize, 0);
    // close without any write
    blockCompressorStream.close();
    
    // check compressed output 
    buf = bytesOut.toByteArray();
    assertEquals("empty file compressed output size is not " + (Math.max(0, bufLen) + 4),
        Math.max(0, bufLen) + 4, buf.length);
    
    // use compressed output as input for decompression
    bytesIn = new ByteArrayInputStream(buf);
    
    // get decompression stream
    try (BlockDecompressorStream blockDecompressorStream =
      new BlockDecompressorStream(bytesIn, new FakeDecompressor(), buffSize)) {
      assertEquals("return value is not -1", 
          -1 , blockDecompressorStream.read());
    } catch (IOException e) {
      fail("unexpected IOException : " + e);
    }
  }

  @Test
  @Parameters({
  "1024",
  "2048",
  "-200",
  "0",
  "100"
  })
  public void testReadWhenIoExceptionOccure(int buffSize) throws IOException {
    Assume.assumeTrue(buffSize > 0);
    File file = new File("testReadWhenIOException");
    try {
      file.createNewFile();
      InputStream io = new FileInputStream(file) {
        @Override
        public int read() throws IOException {
          throw new IOException("File blocks missing");
        }
      };

      try (BlockDecompressorStream blockDecompressorStream =
          new BlockDecompressorStream(io, new FakeDecompressor(), buffSize)) {
        int byteRead = blockDecompressorStream.read();
        fail("Should not return -1 in case of IOException. Byte read "
            + byteRead);
      } catch (IOException e) {
        assertTrue(e.getMessage().contains("File blocks missing"));
      }
    } finally {
      file.delete();
    }
  }
}