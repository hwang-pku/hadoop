/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security;


import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;

import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Map;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestAuthenticationFilter {
  private String authType;
  private int tokenValidTime;

  @Before
  public void setUp() {
    Configuration.MYHACK.clear();
    Configuration.MYHACK.put("hadoop.http.authentication.type", this.authType);
    Configuration.MYHACK.put("hadoop.http.authentication.token.validity", Integer.toString(this.tokenValidTime));
  }

  public TestAuthenticationFilter(String authType, int tokenValidTime) {
    this.authType = authType;
    this.tokenValidTime = tokenValidTime;
  }

  @Parameters(name = "authType={0}, tokenValidTime={1}")
  public static Collection params () {
      return Arrays.asList(new Object[][] {
          {"simple", 18000},
          {"kerberos", 18000},
          {"simple", 36000},
          {"kerberos", 36000}
      });
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testConfiguration() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.http.authentication.foo", "bar");

    conf.set(HttpServer2.BIND_ADDRESS, "barhost");
    
    FilterContainer container = Mockito.mock(FilterContainer.class);
    Mockito.doAnswer(
      new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock)
          throws Throwable {
          Object[] args = invocationOnMock.getArguments();

          Configuration.MYHACK.clear();
          assertEquals("authentication", args[0]);

          assertEquals(AuthenticationFilter.class.getName(), args[1]);

          Map<String, String> conf = (Map<String, String>) args[2];
          assertEquals("/", conf.get("cookie.path"));

          assertEquals(authType, conf.get("type"));
          assertEquals(Integer.toString(tokenValidTime), conf.get("token.validity"));
          assertNull(conf.get("cookie.domain"));
          assertEquals("true", conf.get("simple.anonymous.allowed"));
          assertEquals("HTTP/barhost@LOCALHOST",
                       conf.get("kerberos.principal"));
          assertEquals(System.getProperty("user.home") +
                       "/hadoop.keytab", conf.get("kerberos.keytab"));
          assertEquals("bar", conf.get("foo"));

          return null;
        }
      }).when(container).addFilter(any(), any(), any());

    new AuthenticationFilterInitializer().initFilter(container, conf);
  }

}
