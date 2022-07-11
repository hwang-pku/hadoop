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

package org.apache.hadoop.fs.contract.sftp;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractSeekTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.Parameter;

@RunWith(Parameterized.class)
public class TestSFTPContractSeek extends AbstractContractSeekTest {
  @Parameter(0)
  public String hadoopKerberosKeytabLoginAutorenewalEnabled;

  @Parameter(1)
  public String hadoopSecurityAuthToLocalMechanism;

  @Before
  public void setUp() {
      Configuration.MYHACK.clear();
      Configuration.MYHACK.put("hadoop.kerberos.keytab.login.autorenewal.enabled",
          this.hadoopKerberosKeytabLoginAutorenewalEnabled);
      Configuration.MYHACK.put("hadoop.security.auth_to_local.mechanism",
          this.hadoopSecurityAuthToLocalMechanism);
  }

  @Parameters(name = "hadoop.kerberos.keytab.login.autorenewal.enabled={0},hadoop.security.auth_to_local.mechanism={1}")
  public static Collection params() {
    return Arrays.asList(new Object[][]{
        {"true", "hadoop"},
        {"true", "MIT"},
        {"false", "hadoop"},
        {"false", "MIT"}
    });
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new SFTPContract(conf);
  }
}
