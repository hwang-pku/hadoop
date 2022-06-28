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
package org.apache.hadoop.util.curator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Collection;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;

/**
 * Test the manager for ZooKeeper Curator.
 */
@RunWith(Parameterized.class)
public class TestZKCuratorManager {

  private TestingServer server;
  private ZKCuratorManager curator;
  private Boolean isBadValue;
  private String numRetries;

  @Before
  public void setup() throws Exception {
    Configuration.MYHACK.clear();
    Configuration.MYHACK.put("hadoop.zk.num-retries", this.numRetries);
    this.server = new TestingServer();

    Configuration conf = new Configuration();
    conf.set(
        CommonConfigurationKeys.ZK_ADDRESS, this.server.getConnectString());

    this.curator = new ZKCuratorManager(conf);
    this.curator.start();
  }

  public TestZKCuratorManager (String numRetries) {
      this.numRetries = numRetries;
  }

  @Parameterized.Parameters
  public static Collection retryTimes() {
      return Arrays.asList(new Object[][] {
                { "1000" },
                { "2000" },
                { "3000" },
                { "foobar" }
        });
  }


  @After
  public void teardown() throws Exception {
    this.curator.close();
    if (this.server != null) {
      this.server.close();
      this.server = null;
    }
  }

  @Test
  public void testChildren() throws Exception {
    List<String> children = curator.getChildren("/");
    assertEquals(1, children.size());

    assertFalse(curator.exists("/node1"));
    curator.create("/node1");
    assertTrue(curator.exists("/node1"));

    assertFalse(curator.exists("/node2"));
    curator.create("/node2");
    assertTrue(curator.exists("/node2"));

    children = curator.getChildren("/");
    assertEquals(3, children.size());

    curator.delete("/node2");
    assertFalse(curator.exists("/node2"));
    children = curator.getChildren("/");
    assertEquals(2, children.size());
  }

}
