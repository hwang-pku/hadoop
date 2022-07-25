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

import static org.apache.hadoop.util.ApplicationClassLoader.constructUrlsFromClasspath;
import static org.apache.hadoop.util.ApplicationClassLoader.isSystemClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Arrays;
import java.util.Collection;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.Parameter;

import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class TestApplicationClassLoader {
  
  private static File testDir = GenericTestUtils.getTestDir("appclassloader");
  @Parameter(0)
  public String authToLocalMechanism;
  @Parameter(1)
  public boolean remoteSymlink;
  @Parameter(2)
  public int cacheSecs;

  @Parameters(name = "authToLocalMechanism={0}, remoteSymlink={1}, cacheSecs={2}")
  public static Collection params() {
    return Arrays.asList(new Object[][] {
        { "simple", true, 1 },
        { "simple", true, 10 },
        { "simple", true, 100 },
        { "simple", true, 1000 },
        { "simple", false, 1 },
        { "simple", false, 10 },
        { "simple", false, 100 },
        { "simple", false, 1000 },
        { "kerberos", true, 1 },
        { "kerberos", true, 10 },
        { "kerberos", true, 100 },
        { "kerberos", true, 1000 },
        { "kerberos", false, 1 },
        { "kerberos", false, 10 },
        { "kerberos", false, 100 },
        { "kerberos", false, 1000 },
    });
  }
  
  @Before
  public void setUp() {
    Configuration.MYHACK.clear();
    Configuration.MYHACK.put("hadoop.security.auth_to_local", authToLocalMechanism);
    Configuration.MYHACK.put("fs.client.resolve.remote.symlinks", Boolean.toString(remoteSymlink));
    Configuration.MYHACK.put("hadoop.security.groups.cache.secs", Integer.toString(cacheSecs));
    FileUtil.fullyDelete(testDir);
    testDir.mkdirs();
  }

  @Test
  public void testConstructUrlsFromClasspath() throws Exception {
    File file = new File(testDir, "file");
    assertTrue("Create file", file.createNewFile());

    File dir = new File(testDir, "dir");
    assertTrue("Make dir", dir.mkdir());

    File jarsDir = new File(testDir, "jarsdir");
    assertTrue("Make jarsDir", jarsDir.mkdir());
    File nonJarFile = new File(jarsDir, "nonjar");
    assertTrue("Create non-jar file", nonJarFile.createNewFile());
    File jarFile = new File(jarsDir, "a.jar");
    assertTrue("Create jar file", jarFile.createNewFile());

    File nofile = new File(testDir, "nofile");
    // don't create nofile

    StringBuilder cp = new StringBuilder();
    cp.append(file.getAbsolutePath()).append(File.pathSeparator)
      .append(dir.getAbsolutePath()).append(File.pathSeparator)
      .append(jarsDir.getAbsolutePath() + "/*").append(File.pathSeparator)
      .append(nofile.getAbsolutePath()).append(File.pathSeparator)
      .append(nofile.getAbsolutePath() + "/*").append(File.pathSeparator);
    
    URL[] urls = constructUrlsFromClasspath(cp.toString());
    
    assertEquals(3, urls.length);
    assertEquals(file.toURI().toURL(), urls[0]);
    assertEquals(dir.toURI().toURL(), urls[1]);
    assertEquals(jarFile.toURI().toURL(), urls[2]);
    // nofile should be ignored
  }

  @Test
  public void testIsSystemClass() {
    testIsSystemClassInternal("");
  }

  @Test
  public void testIsSystemNestedClass() {
    testIsSystemClassInternal("$Klass");
  }

  private void testIsSystemClassInternal(String nestedClass) {
    assertFalse(isSystemClass("org.example.Foo" + nestedClass, null));
    assertTrue(isSystemClass("org.example.Foo" + nestedClass,
        classes("org.example.Foo")));
    assertTrue(isSystemClass("/org.example.Foo" + nestedClass,
        classes("org.example.Foo")));
    assertTrue(isSystemClass("org.example.Foo" + nestedClass,
        classes("org.example.")));
    assertTrue(isSystemClass("net.example.Foo" + nestedClass,
        classes("org.example.,net.example.")));
    assertFalse(isSystemClass("org.example.Foo" + nestedClass,
        classes("-org.example.Foo,org.example.")));
    assertTrue(isSystemClass("org.example.Bar" + nestedClass,
        classes("-org.example.Foo.,org.example.")));
    assertFalse(isSystemClass("org.example.Foo" + nestedClass,
        classes("org.example.,-org.example.Foo")));
    assertFalse(isSystemClass("org.example.Foo" + nestedClass,
        classes("org.example.Foo,-org.example.Foo")));
  }

  private List<String> classes(String classes) {
    return Lists.newArrayList(Splitter.on(',').split(classes));
  }
  
  @Test
  public void testGetResource() throws IOException {
    URL testJar = makeTestJar().toURI().toURL();
    
    ClassLoader currentClassLoader = getClass().getClassLoader();
    ClassLoader appClassloader = new ApplicationClassLoader(
        new URL[] { testJar }, currentClassLoader, null);

    assertNull("Resource should be null for current classloader",
        currentClassLoader.getResourceAsStream("resource.txt"));

    InputStream in = appClassloader.getResourceAsStream("resource.txt");
    assertNotNull("Resource should not be null for app classloader", in);
    assertEquals("hello", IOUtils.toString(in));
  }
  
  private File makeTestJar() throws IOException {
    File jarFile = new File(testDir, "test.jar");
    JarOutputStream out = new JarOutputStream(new FileOutputStream(jarFile));
    ZipEntry entry = new ZipEntry("resource.txt");
    out.putNextEntry(entry);
    out.write("hello".getBytes());
    out.closeEntry();
    out.close();
    return jarFile;
  }
  
}
