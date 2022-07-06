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
package org.apache.hadoop.crypto;

import java.io.OutputStream;
import java.util.Collection;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Before;
import static org.mockito.Mockito.*;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;

/**
 * To test proper closing of underlying stream of CryptoOutputStream.
 */
@RunWith(Parameterized.class)
public class TestCryptoOutputStreamClosing {
  private static CryptoCodec codec;
  private String cipherType;

  @BeforeClass
  public static void init() throws Exception {
    codec = CryptoCodec.getInstance(new Configuration());
  }

  public TestCryptoOutputStreamClosing(String cipherType) {
    this.cipherType = cipherType;
  }

  @Parameterized.Parameters
  public static Collection cipherTypes() {
    return Arrays.asList(new Object[][] {
    { "AES/CTR/NoPadding" },
    { "AES/CTR/Padding" }
    });
  }

  @Before
  public void setup() throws Exception {
      Configuration.MYHACK.clear();
      Configuration.MYHACK.put("hadoop.security.crypto.cipher.suite", this.cipherType);
  }

  @Test
  public void testOutputStreamClosing() throws Exception {
    OutputStream outputStream = mock(OutputStream.class);
    CryptoOutputStream cos = new CryptoOutputStream(outputStream, codec,
        new byte[16], new byte[16], 0L, true);
    cos.close();
    verify(outputStream).close();
  }

  @Test
  public void testOutputStreamNotClosing() throws Exception {
    OutputStream outputStream = mock(OutputStream.class);
    CryptoOutputStream cos = new CryptoOutputStream(outputStream, codec,
        new byte[16], new byte[16], 0L, false);
    cos.close();
    verify(outputStream, never()).close();
  }

}
