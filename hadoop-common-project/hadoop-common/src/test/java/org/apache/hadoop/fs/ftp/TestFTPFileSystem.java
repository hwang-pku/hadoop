/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.fs.ftp;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Collection;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.Parameter;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Test basic @{link FTPFileSystem} class methods. Contract tests are in
 * TestFTPContractXXXX.
 */
@RunWith(Parameterized.class)
public class TestFTPFileSystem {

  private FtpTestServer server;
  private java.nio.file.Path testDir;
  @Rule
  public Timeout testTimeout = new Timeout(180000);
  @Parameter(0)
  public String transferMode;
  public int FTPTransferMode;
  @Parameter(1)
  public String dataConnectionMode;
  public int FTPDataConnectionMode;

  @Parameters(name = "transferMode={0}, dataConnectionMode={1}")
  public static Collection params() {
    return Arrays.asList(new Object[][] {
        {"STREAM_TRANSFER_MODE", "ACTIVE_LOCAL_DATA_CONNECTION_MODE"},
        {"STREAM_TRANSFER_MODE", "PASSIVE_LOCAL_DATA_CONNECTION_MODE"},
        {"STREAM_TRANSFER_MODE", "PASSIVE_REMOTE_DATA_CONNECTION_MODE"},
        {"BLOCK_TRANSFER_MODE", "ACTIVE_LOCAL_DATA_CONNECTION_MODE"},
        {"BLOCK_TRANSFER_MODE", "PASSIVE_LOCAL_DATA_CONNECTION_MODE"},
        {"BLOCK_TRANSFER_MODE", "PASSIVE_REMOTE_DATA_CONNECTION_MODE"},
        {"COMPRESSED_TRANSFER_MODE", "ACTIVE_LOCAL_DATA_CONNECTION_MODE"},
        {"COMPRESSED_TRANSFER_MODE", "PASSIVE_LOCAL_DATA_CONNECTION_MODE"},
        {"COMPRESSED_TRANSFER_MODE", "PASSIVE_REMOTE_DATA_CONNECTION_MODE"}
    });
  }

  @Before
  public void setUp() throws Exception {
    Configuration.MYHACK.clear();
    Configuration.MYHACK.put("fs.ftp.transfer.mode", transferMode);
    Configuration.MYHACK.put("fs.ftp.data.connection.mode", dataConnectionMode);
    switch(transferMode) {
    case "STREAM_TRANSFER_MODE":
      FTPTransferMode = FTP.STREAM_TRANSFER_MODE;
      break;
    case "BLOCK_TRANSFER_MODE":
      FTPTransferMode = FTP.BLOCK_TRANSFER_MODE;
      break;
    case "COMPRESSED_TRANSFER_MODE":
      FTPTransferMode = FTP.COMPRESSED_TRANSFER_MODE;
      break;
    }
    switch(dataConnectionMode) {
    case "ACTIVE_LOCAL_DATA_CONNECTION_MODE":
      FTPDataConnectionMode = FTPClient.ACTIVE_LOCAL_DATA_CONNECTION_MODE;
      break;
    case "PASSIVE_LOCAL_DATA_CONNECTION_MODE":
      FTPDataConnectionMode = FTPClient.PASSIVE_LOCAL_DATA_CONNECTION_MODE;
      break;
    case "PASSIVE_REMOTE_DATA_CONNECTION_MODE":
      FTPDataConnectionMode = FTPClient.PASSIVE_REMOTE_DATA_CONNECTION_MODE;
      break;
    }
    testDir = Files.createTempDirectory(
        GenericTestUtils.getTestDir().toPath(), getClass().getName()
    );
    server = new FtpTestServer(testDir).start();
  }

  @After
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void tearDown() throws Exception {
    if (server != null) {
      server.stop();
      Files.walk(testDir)
          .sorted(Comparator.reverseOrder())
          .map(java.nio.file.Path::toFile)
          .forEach(File::delete);
    }
  }

  @Test
  public void testCreateWithWritePermissions() throws Exception {
    BaseUser user = server.addUser("test", "password", new WritePermission());
    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", "ftp:///");
    configuration.set("fs.ftp.host", "localhost");
    configuration.setInt("fs.ftp.host.port", server.getPort());
    configuration.set("fs.ftp.user.localhost", user.getName());
    configuration.set("fs.ftp.password.localhost", user.getPassword());
    configuration.setBoolean("fs.ftp.impl.disable.cache", true);

    FileSystem fs = FileSystem.get(configuration);
    byte[] bytesExpected = "hello world".getBytes(StandardCharsets.UTF_8);
    //This failed with message "Unable to create file: test1.txt, Aborting" for "PASSIVE_REMOTE_DATA_CONNECTION_MODE"
    try (FSDataOutputStream outputStream = fs.create(new Path("test1.txt"))) {
      outputStream.write(bytesExpected);
    }
    try (FSDataInputStream input = fs.open(new Path("test1.txt"))) {
      assertThat(bytesExpected, equalTo(IOUtils.readFullyToByteArray(input)));
    }
  }

  @Test
  public void testCreateWithoutWritePermissions() throws Exception {
    BaseUser user = server.addUser("test", "password");
    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", "ftp:///");
    configuration.set("fs.ftp.host", "localhost");
    configuration.setInt("fs.ftp.host.port", server.getPort());
    configuration.set("fs.ftp.user.localhost", user.getName());
    configuration.set("fs.ftp.password.localhost", user.getPassword());
    configuration.setBoolean("fs.ftp.impl.disable.cache", true);

    FileSystem fs = FileSystem.get(configuration);
    byte[] bytesExpected = "hello world".getBytes(StandardCharsets.UTF_8);
    LambdaTestUtils.intercept(
        IOException.class, "Unable to create file: test1.txt, Aborting",
        () -> {
          try (FSDataOutputStream out = fs.create(new Path("test1.txt"))) {
            out.write(bytesExpected);
          }
        }
    );
  }

  @Test
  public void testFTPDefaultPort() throws Exception {
    FTPFileSystem ftp = new FTPFileSystem();
    assertEquals(FTP.DEFAULT_PORT, ftp.getDefaultPort());
  }

  @Test
  public void testFTPTransferMode() throws Exception {
    Configuration conf = new Configuration();
    FTPFileSystem ftp = new FTPFileSystem();
    assertEquals(FTPTransferMode, ftp.getTransferMode(conf));

    Configuration.MYHACK.clear(); //Not necessary
    conf.set(FTPFileSystem.FS_FTP_TRANSFER_MODE, "STREAM_TRANSFER_MODE");
    assertEquals(FTP.STREAM_TRANSFER_MODE, ftp.getTransferMode(conf));

    conf.set(FTPFileSystem.FS_FTP_TRANSFER_MODE, "COMPRESSED_TRANSFER_MODE");
    assertEquals(FTP.COMPRESSED_TRANSFER_MODE, ftp.getTransferMode(conf));

    conf.set(FTPFileSystem.FS_FTP_TRANSFER_MODE, "invalid");
    assertEquals(FTPClient.BLOCK_TRANSFER_MODE, ftp.getTransferMode(conf));
  }

  @Test
  public void testFTPDataConnectionMode() throws Exception {
    Configuration conf = new Configuration();
    FTPClient client = new FTPClient();
    FTPFileSystem ftp = new FTPFileSystem();
    assertEquals(FTPClient.ACTIVE_LOCAL_DATA_CONNECTION_MODE, client.getDataConnectionMode());

    Configuration.MYHACK.clear(); //Not necessary.
    ftp.setDataConnectionMode(client, conf);
    assertEquals(FTPClient.ACTIVE_LOCAL_DATA_CONNECTION_MODE,
        client.getDataConnectionMode());

    conf.set(FTPFileSystem.FS_FTP_DATA_CONNECTION_MODE, "invalid");
    ftp.setDataConnectionMode(client, conf);
    assertEquals(FTPClient.ACTIVE_LOCAL_DATA_CONNECTION_MODE,
        client.getDataConnectionMode());

    conf.set(FTPFileSystem.FS_FTP_DATA_CONNECTION_MODE,
        "PASSIVE_LOCAL_DATA_CONNECTION_MODE");
    ftp.setDataConnectionMode(client, conf);
    assertEquals(FTPClient.PASSIVE_LOCAL_DATA_CONNECTION_MODE,
        client.getDataConnectionMode());
  }

  @Test
  public void testGetFsAction(){
    FTPFileSystem ftp = new FTPFileSystem();
    int[] accesses = new int[] {FTPFile.USER_ACCESS, FTPFile.GROUP_ACCESS,
        FTPFile.WORLD_ACCESS};
    FsAction[] actions = FsAction.values();
    for(int i = 0; i < accesses.length; i++){
      for(int j = 0; j < actions.length; j++){
        enhancedAssertEquals(actions[j], ftp.getFsAction(accesses[i],
            getFTPFileOf(accesses[i], actions[j])));
      }
    }
  }

  private void enhancedAssertEquals(FsAction actionA, FsAction actionB){
    String notNullErrorMessage = "FsAction cannot be null here.";
    Preconditions.checkNotNull(actionA, notNullErrorMessage);
    Preconditions.checkNotNull(actionB, notNullErrorMessage);
    String errorMessageFormat = "expect FsAction is %s, whereas it is %s now.";
    String notEqualErrorMessage = String.format(errorMessageFormat,
        actionA.name(), actionB.name());
    assertEquals(notEqualErrorMessage, actionA, actionB);
  }

  private FTPFile getFTPFileOf(int access, FsAction action) {
    boolean check = access == FTPFile.USER_ACCESS ||
                      access == FTPFile.GROUP_ACCESS ||
                      access == FTPFile.WORLD_ACCESS;
    String errorFormat = "access must be in [%d,%d,%d], but it is %d now.";
    String errorMessage = String.format(errorFormat, FTPFile.USER_ACCESS,
         FTPFile.GROUP_ACCESS, FTPFile.WORLD_ACCESS, access);
    Preconditions.checkArgument(check, errorMessage);
    Preconditions.checkNotNull(action);
    FTPFile ftpFile = new FTPFile();

    if(action.implies(FsAction.READ)){
      ftpFile.setPermission(access, FTPFile.READ_PERMISSION, true);
    }

    if(action.implies(FsAction.WRITE)){
      ftpFile.setPermission(access, FTPFile.WRITE_PERMISSION, true);
    }

    if(action.implies(FsAction.EXECUTE)){
      ftpFile.setPermission(access, FTPFile.EXECUTE_PERMISSION, true);
    }

    return ftpFile;
  }

  @Test
  public void testFTPSetTimeout() {
    Configuration conf = new Configuration();
    FTPClient client = new FTPClient();
    FTPFileSystem ftp = new FTPFileSystem();

    ftp.setTimeout(client, conf);
    assertEquals(client.getControlKeepAliveTimeout(),
        FTPFileSystem.DEFAULT_TIMEOUT);

    long timeout = 600;
    conf.setLong(FTPFileSystem.FS_FTP_TIMEOUT, timeout);
    ftp.setTimeout(client, conf);
    assertEquals(client.getControlKeepAliveTimeout(), timeout);
  }
}
