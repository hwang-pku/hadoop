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
package org.apache.hadoop.fs.shell;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.Random;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.Parameter;
import org.mockito.Mockito;
import org.assertj.core.api.Assertions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.mockito.ArgumentMatchers.any;
import static org.junit.Assert.assertEquals;

/**
 * Test Concat.
 */
@RunWith(Parameterized.class)
public class TestFsShellConcat extends AbstractHadoopTestBase {

  private static Configuration conf;
  private static FsShell shell;
  private static LocalFileSystem lfs;
  private static Path testRootDir;
  private static Path dstPath;
  @Parameter(0)
  public boolean missingDefaultFsWarning;
  @Parameter(1)
  public boolean resolveRemoteSymlinks;

  @Parameters(name = "missingDefaultFsWarning={0}, resolveRemoteSymlinks={1}")
  public static Collection params() {
    return Arrays.asList(new Object[][] {
      {true, false},
      {false, true},
      {true, true},
      {false, false}
    });
  }

  @Before
  public void before() throws IOException {
    Configuration.MYHACK.clear();
    Configuration.MYHACK.put("hadoop.shell.missing.defaultFs.warning",
        Boolean.toString(missingDefaultFsWarning));
    Configuration.MYHACK.put("fs.client.resolve.remote.symlinks",
        Boolean.toString(resolveRemoteSymlinks));
    conf = new Configuration();
    shell = new FsShell(conf);
    lfs = FileSystem.getLocal(conf);
    testRootDir = lfs.makeQualified(new Path(GenericTestUtils.getTempPath(
        "testFsShellCopy")));

    lfs.delete(testRootDir, true);
    lfs.mkdirs(testRootDir);
    lfs.setWorkingDirectory(testRootDir);
    dstPath = new Path(testRootDir, "dstFile");
    lfs.create(dstPath).close();

    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      OutputStream out =
          lfs.create(new Path(testRootDir, String.format("file-%02d", i)));
      out.write(random.nextInt());
      out.close();
    }
  }

  @Test
  public void testConcat() throws Exception {
    // Read concatenated files to build the expected file content.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int i = 0; i < 10; i++) {
      try (InputStream in = lfs
          .open(new Path(testRootDir, String.format("file-%02d", i)))) {
        IOUtils.copyBytes(in, out, 1024);
      }
    }
    byte[] expectContent = out.toByteArray();

    // Do concat.
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    Mockito.doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      Path target = (Path)args[0];
      Path[] src = (Path[]) args[1];
      mockConcat(target, src);
      return null;
    }).when(mockFs).concat(any(Path.class), any(Path[].class));
    Concat.setTestFs(mockFs);
    shellRun(0, "-concat", dstPath.toString(), testRootDir+"/file-*");

    // Verify concat result.
    ContractTestUtils
        .assertPathExists(lfs, "The target file doesn't exist.", dstPath);
    Assertions.assertThat(lfs.listStatus(testRootDir).length).isEqualTo(1);
    assertEquals(expectContent.length, lfs.getFileStatus(dstPath).getLen());
    out = new ByteArrayOutputStream();
    try (InputStream in = lfs.open(dstPath)) {
      IOUtils.copyBytes(in, out, 1024);
    }
    // Verify content.
    byte[] concatedContent = out.toByteArray();
    assertEquals(expectContent.length, concatedContent.length);
    ContractTestUtils.compareByteArrays(expectContent, concatedContent,
        expectContent.length);
  }

  @Test
  public void testUnsupportedFs() throws Exception {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    Mockito.doThrow(
        new UnsupportedOperationException("Mock unsupported exception."))
        .when(mockFs).concat(any(Path.class), any(Path[].class));
    Mockito.doAnswer(invocationOnMock -> new URI("mockfs:///")).when(mockFs)
        .getUri();
    Concat.setTestFs(mockFs);
    final ByteArrayOutputStream err = new ByteArrayOutputStream();
    PrintStream oldErr = System.err;
    System.setErr(new PrintStream(err));
    try {
      shellRun(1, "-concat", dstPath.toString(), testRootDir + "/file-*");
    } finally {
      System.setErr(oldErr);
    }
    System.err.print(err.toString());
    String expectedErrMsg = "Dest filesystem 'mockfs' doesn't support concat";
    Assertions.assertThat(err.toString().contains(expectedErrMsg))
        .withFailMessage("The err message should contain \"" + expectedErrMsg
            + "\" message.").isTrue();
  }

  private void shellRun(int n, String... args) {
    assertEquals(n, shell.run(args));
  }

  /**
   * Simple simulation of concat.
   */
  private void mockConcat(Path target, Path[] srcArray) throws IOException {
    Path tmp = new Path(target.getParent(), target.getName() + ".bak");
    lfs.rename(target, tmp);
    try (OutputStream out = lfs.create(target)) {
      try (InputStream in = lfs.open(tmp)) {
        IOUtils.copyBytes(in, out, 1024);
      }
      lfs.delete(tmp, true);
      for (int i = 0; i < srcArray.length; i++) {
        try (InputStream iin = lfs.open(srcArray[i])) {
          IOUtils.copyBytes(iin, out, 1024);
        }
        lfs.delete(srcArray[i], true);
      }
    }
  }
}
