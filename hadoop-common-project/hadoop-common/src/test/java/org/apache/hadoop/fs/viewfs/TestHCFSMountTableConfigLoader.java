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
package org.apache.hadoop.fs.viewfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.Parameter;

/**
 * Tests the mount table loading.
 */
@RunWith(Parameterized.class)
public class TestHCFSMountTableConfigLoader {

  private static final String DOT = ".";

  private static final String TARGET_TWO = "/tar2";

  private static final String TARGET_ONE = "/tar1";

  private static final String SRC_TWO = "/src2";

  private static final String SRC_ONE = "/src1";

  private static final String TABLE_NAME = "test";

  private MountTableConfigLoader loader = new HCFSMountTableConfigLoader();

  private static FileSystem fsTarget;
  private static Configuration conf;
  private static Path targetTestRoot;
  private static FileSystemTestHelper fileSystemTestHelper =
      new FileSystemTestHelper();
  private static File oldVersionMountTableFile;
  private static File newVersionMountTableFile;
  private static final String MOUNT_LINK_KEY_SRC_ONE =
      new StringBuilder(Constants.CONFIG_VIEWFS_PREFIX).append(DOT)
          .append(TABLE_NAME).append(DOT).append(Constants.CONFIG_VIEWFS_LINK)
          .append(DOT).append(SRC_ONE).toString();
  private static final String MOUNT_LINK_KEY_SRC_TWO =
      new StringBuilder(Constants.CONFIG_VIEWFS_PREFIX).append(DOT)
          .append(TABLE_NAME).append(DOT).append(Constants.CONFIG_VIEWFS_LINK)
          .append(DOT).append(SRC_TWO).toString();
  @Parameter(0)
  public boolean isRemoteSymlinks;
  @Parameter(1)
  public int ioFileBufferSize;
  @Parameter(2)
  public int fileStreamBufferSize;

  @Parameters(name = "isRemoteSymlinks={0}, io.file.buffer.size={1}, file.stream-buffer-size={2}")
  public static Collection params() {
    return Arrays.asList(new Object[][] {
        {true, 1, 1},
        {true, 64, 1},
        {true, 256, 1},
        {true, 1, 64},
        {true, 64, 64},
        {true, 256, 64},
        {true, 1, 256},
        {true, 64, 256},
        {true, 256, 256},
        {false, 1, 1},
        {false, 64, 1},
        {false, 256, 1},
        {false, 1, 64},
        {false, 64, 64},
        {false, 256, 64},
        {false, 1, 256},
        {false, 64, 256},
        {false, 256, 256},
    });
  }

  @BeforeClass
  public static void init() throws Exception {
    fsTarget = new LocalFileSystem();
    fsTarget.initialize(new URI("file:///"), new Configuration());
    targetTestRoot = fileSystemTestHelper.getAbsoluteTestRootPath(fsTarget);
    fsTarget.delete(targetTestRoot, true);
    fsTarget.mkdirs(targetTestRoot);
  }

  @Before
  public void setUp() throws Exception {
    Configuration.MYHACK.clear();
    Configuration.MYHACK.put("fs.client.resolve.remote.symlinks",
        Boolean.toString(isRemoteSymlinks));
    Configuration.MYHACK.put("io.file.buffer.size", Integer.toString(ioFileBufferSize));
    Configuration.MYHACK.put("file.stream-buffer-size",
        Integer.toString(fileStreamBufferSize));
    conf = new Configuration();
    conf.set(String.format(
        FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN, "file"),
        LocalFileSystem.class.getName());
    oldVersionMountTableFile =
        new File(new URI(targetTestRoot.toString() + "/table.1.xml"));
    oldVersionMountTableFile.createNewFile();
    newVersionMountTableFile =
        new File(new URI(targetTestRoot.toString() + "/table.2.xml"));
    newVersionMountTableFile.createNewFile();
  }

  @Test
  public void testMountTableFileLoadingWhenMultipleFilesExist()
      throws Exception {
    ViewFsTestSetup.addMountLinksToFile(TABLE_NAME,
        new String[] {SRC_ONE, SRC_TWO }, new String[] {TARGET_ONE,
            TARGET_TWO },
        new Path(newVersionMountTableFile.toURI()), conf);
    loader.load(targetTestRoot.toString(), conf);
    Assert.assertEquals(conf.get(MOUNT_LINK_KEY_SRC_TWO), TARGET_TWO);
    Assert.assertEquals(conf.get(MOUNT_LINK_KEY_SRC_ONE), TARGET_ONE);
  }

  @Test
  public void testMountTableFileWithInvalidFormat() throws Exception {
    Path path = new Path(new URI(
        targetTestRoot.toString() + "/testMountTableFileWithInvalidFormat/"));
    fsTarget.mkdirs(path);
    File invalidMountFileName =
        new File(new URI(path.toString() + "/table.InvalidVersion.xml"));
    invalidMountFileName.createNewFile();
    // Adding mount links to make sure it will not read it.
    ViewFsTestSetup.addMountLinksToFile(TABLE_NAME,
        new String[] {SRC_ONE, SRC_TWO }, new String[] {TARGET_ONE,
            TARGET_TWO },
        new Path(invalidMountFileName.toURI()), conf);
    // Pass mount table directory
    loader.load(path.toString(), conf);
    Assert.assertEquals(null, conf.get(MOUNT_LINK_KEY_SRC_TWO));
    Assert.assertEquals(null, conf.get(MOUNT_LINK_KEY_SRC_ONE));
    invalidMountFileName.delete();
  }

  @Test
  public void testMountTableFileWithInvalidFormatWithNoDotsInName()
      throws Exception {
    Path path = new Path(new URI(targetTestRoot.toString()
        + "/testMountTableFileWithInvalidFormatWithNoDots/"));
    fsTarget.mkdirs(path);
    File invalidMountFileName =
        new File(new URI(path.toString() + "/tableInvalidVersionxml"));
    invalidMountFileName.createNewFile();
    // Pass mount table directory
    loader.load(path.toString(), conf);
    Assert.assertEquals(null, conf.get(MOUNT_LINK_KEY_SRC_TWO));
    Assert.assertEquals(null, conf.get(MOUNT_LINK_KEY_SRC_ONE));
    invalidMountFileName.delete();
  }

  @Test(expected = FileNotFoundException.class)
  public void testLoadWithMountFile() throws Exception {
    loader.load(new URI(targetTestRoot.toString() + "/Non-Existent-File.xml")
        .toString(), conf);
  }

  @Test
  public void testLoadWithNonExistentMountFile() throws Exception {
    ViewFsTestSetup.addMountLinksToFile(TABLE_NAME,
        new String[] {SRC_ONE, SRC_TWO },
        new String[] {TARGET_ONE, TARGET_TWO },
        new Path(oldVersionMountTableFile.toURI()), conf);
    loader.load(oldVersionMountTableFile.toURI().toString(), conf);
    Assert.assertEquals(conf.get(MOUNT_LINK_KEY_SRC_TWO), TARGET_TWO);
    Assert.assertEquals(conf.get(MOUNT_LINK_KEY_SRC_ONE), TARGET_ONE);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    fsTarget.delete(targetTestRoot, true);
  }

}
