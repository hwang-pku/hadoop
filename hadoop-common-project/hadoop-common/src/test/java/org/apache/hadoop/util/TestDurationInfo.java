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
package org.apache.hadoop.util;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class to test DurationInfo.
 */
@RunWith(JUnitParamsRunner.class)
public class TestDurationInfo {
  private final Logger log = LoggerFactory.getLogger(TestDurationInfo.class);

  @Test( timeout = 10540)
  @Parameters({
  "1000",
  "200",
  "99999999",
  "0",
  "-1",
  "-1000"
  })
  public void testDurationInfoCreation(long sleepTime) throws Exception {
    Assume.assumeTrue(sleepTime > 0 && sleepTime < 10000);
    DurationInfo info = new DurationInfo(log, "test");
    Assert.assertTrue(info.value() == 0);
    Thread.sleep(sleepTime);
    info.finished();
    Assert.assertTrue(info.value() > 0);

    info = new DurationInfo(log, true, "test format %s", "value");
    Assert.assertEquals("test format value: duration 0:00.000s",
        info.toString());

    info = new DurationInfo(log, false, "test format %s", "value");
    Assert.assertEquals("test format value: duration 0:00.000s",
        info.toString());
  }

  @Test
  public void testDurationInfoWithMultipleClose() throws Exception {
    DurationInfo info = new DurationInfo(log, "test");
    Thread.sleep(1000);
    info.close();
    info.close();
    Assert.assertTrue(info.value() > 0);
  }

  @Test(expected = NullPointerException.class)
  public void testDurationInfoCreationWithNullMsg() {
    DurationInfo info = new DurationInfo(log, null);
    info.close();
  }
}
