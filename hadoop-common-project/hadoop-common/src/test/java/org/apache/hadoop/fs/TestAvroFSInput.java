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

package org.apache.hadoop.fs;

import java.util.Arrays;
import java.util.Collection;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;

import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.Parameter;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestAvroFSInput {

  private static final String INPUT_DIR = "AvroFSInput";
  @Parameter(0)
  public int checksumSize;
  @Parameter(1)
  public String umask;

  private Path getInputPath() {
    return new Path(GenericTestUtils.getTempPath(INPUT_DIR));
  }

  @Before
  public void setUp() {
      Configuration.MYHACK.clear();
      Configuration.MYHACK.put("fs.permissions.umask-mode", umask);
      Configuration.MYHACK.put("file.bytes-per-checksum", Integer.toString(checksumSize));
  }

  @Parameters(name = "checksumSize={0}, umask={1}")
  public static Collection params() {
    return Arrays.asList(new Object[][] {
        {128, "u=rwx,g=rwx,o="},
        {256, "u=rwx,g=rwx,o="},
        {512, "u=rwx,g=rwx,o="},
        {1024, "u=rwx,g=rwx,o="},
        {128, "022"},
        {256, "022"},
        {512, "022"},
        {1024, "022"},
        {128, "777"},
        {256, "777"},
        {512, "777"},
        {1024, "777"}
    });
  }

  @Test
  public void testAFSInput() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    Path dir = getInputPath();

    if (!fs.exists(dir)) {
      fs.mkdirs(dir);
    }

    Path filePath = new Path(dir, "foo");
    if (fs.exists(filePath)) {
      fs.delete(filePath, false);
    }

    FSDataOutputStream ostream = fs.create(filePath);
    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(ostream));
    w.write("0123456789");
    w.close();

    // Create the stream
    FileContext fc = FileContext.getFileContext(conf);
    AvroFSInput avroFSIn = new AvroFSInput(fc, filePath);

    assertEquals(10, avroFSIn.length());

    // Check initial position
    byte [] buf = new byte[1];
    assertEquals(0, avroFSIn.tell());

    // Check a read from that position.
    avroFSIn.read(buf, 0, 1);
    assertEquals(1, avroFSIn.tell());
    assertEquals('0', (char)buf[0]);

    // Check a seek + read
    avroFSIn.seek(4);
    assertEquals(4, avroFSIn.tell());
    avroFSIn.read(buf, 0, 1);
    assertEquals('4', (char)buf[0]);
    assertEquals(5, avroFSIn.tell());

    avroFSIn.close();
  }
}

