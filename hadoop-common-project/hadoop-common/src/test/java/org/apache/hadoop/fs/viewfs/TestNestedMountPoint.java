/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.viewfs;

import java.net.URI;
import java.util.List;
import java.util.function.Function;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;


/**
 * Unit test of nested mount point support in INodeTree
 */
@RunWith(JUnitParamsRunner.class)
public class TestNestedMountPoint {
  private InodeTree inodeTree;
  private Configuration conf;
  private String mtName;
  private URI fsUri;

  static class TestNestMountPointFileSystem {
    public URI getUri() {
      return uri;
    }

    private URI uri;

    TestNestMountPointFileSystem(URI uri) {
      this.uri = uri;
    }
  }

  static class TestNestMountPointInternalFileSystem extends TestNestMountPointFileSystem {
    TestNestMountPointInternalFileSystem(URI uri) {
      super(uri);
    }
  }

  private static final URI LINKFALLBACK_TARGET = URI.create("hdfs://nn00");
  private static final URI NN1_TARGET = URI.create("hdfs://nn01/a/b");
  private static final URI NN2_TARGET = URI.create("hdfs://nn02/a/b/e");
  private static final URI NN3_TARGET = URI.create("hdfs://nn03/a/b/c/d");
  private static final URI NN4_TARGET = URI.create("hdfs://nn04/a/b/c/d/e");
  private static final URI NN5_TARGET = URI.create("hdfs://nn05/b/c/d/e");
  private static final URI NN6_TARGET = URI.create("hdfs://nn06/b/c/d/e/f");

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    mtName = TestNestedMountPoint.class.getName();
    ConfigUtil.setIsNestedMountPointSupported(conf, true);
    ConfigUtil.addLink(conf, mtName, "/a/b", NN1_TARGET);
    ConfigUtil.addLink(conf, mtName, "/a/b/e", NN2_TARGET);
    ConfigUtil.addLink(conf, mtName, "/a/b/c/d", NN3_TARGET);
    ConfigUtil.addLink(conf, mtName, "/a/b/c/d/e", NN4_TARGET);
    ConfigUtil.addLink(conf, mtName, "/b/c/d/e", NN5_TARGET);
    ConfigUtil.addLink(conf, mtName, "/b/c/d/e/f", NN6_TARGET);
    ConfigUtil.addLinkFallback(conf, mtName, LINKFALLBACK_TARGET);

    fsUri = new URI(FsConstants.VIEWFS_SCHEME, mtName, "/", null, null);

    inodeTree = new InodeTree<TestNestedMountPoint.TestNestMountPointFileSystem>(conf,
        mtName, fsUri, false) {
      @Override
      protected Function<URI, TestNestedMountPoint.TestNestMountPointFileSystem> initAndGetTargetFs() {
        return new Function<URI, TestNestMountPointFileSystem>() {
          @Override
          public TestNestedMountPoint.TestNestMountPointFileSystem apply(URI uri) {
            return new TestNestMountPointFileSystem(uri);
          }
        };
      }

      // For intenral dir fs
      @Override
      protected TestNestedMountPoint.TestNestMountPointInternalFileSystem getTargetFileSystem(
          final INodeDir<TestNestedMountPoint.TestNestMountPointFileSystem> dir) {
        return new TestNestMountPointInternalFileSystem(fsUri);
      }

      @Override
      protected TestNestedMountPoint.TestNestMountPointInternalFileSystem getTargetFileSystem(
          final String settings, final URI[] mergeFsURIList) {
        return new TestNestMountPointInternalFileSystem(null);
      }
    };
  }

  @After
  public void tearDown() throws Exception {
    inodeTree = null;
  }

  private Object[] valueSetForTestPathResolveToLink() {
    return new Object[] {
                // Old Test 1 testPathResolveToLink
                // /a/b/c/d/e/f resolves to /a/b/c/d/e and /f
                new Object[] {"/a/b/c/d/e/f","/a/b/c/d/e","/f", true, NN4_TARGET},
                // /a/b/c/d/e resolves to /a/b/c/d/e and /
                new Object[] {"/a/b/c/d/e","/a/b/c/d/e","/", true, NN4_TARGET},
                // /a/b/c/d/e/f/g/h/i resolves to /a/b/c/d/e and /f/g/h/i
                new Object[] {"/a/b/c/d/e/f/g/h/i", "/a/b/c/d/e", "/f/g/h/i", true, NN4_TARGET},
                // /a/b/c/d/e/f/g/ resolves to /a/b/c/d/e and /f/g
                new Object[] {"/a/b/c/d/e/f/g", "/a/b/c/d/e", "/f/g", true, NN4_TARGET},
                // /a/b/c/d/e/f/g/h/i/j/k/l/m resolves to /a/b/c/d/e and /f/g/h/i/j/k/l/m
                new Object[] {"/a/b/c/d/e/f/g/h/i/j/k/l/m", "/a/b/c/d/e", "/f/g/h/i/j/k/l/m", true, NN4_TARGET},
                // /a/b/c/d/e/f resolves to /a/b/c/d/e and /f
                // Old Test 2 testPathResolveToLinkNotResolveLastComponent
                new Object[] {"/a/b/c/d/e/f", "/a/b/c/d/e", "/f", false, NN4_TARGET},
                // /a/b/c/d/e resolves to /a/b/c/d and /e
                new Object[] {"/a/b/c/d/e", "/a/b/c/d", "/e", false, NN3_TARGET},
                // /a/b/c/d/e/f/g/h/i resolves to /a/b/c/d/e and /f/g/h/i
                new Object[] {"/a/b/c/d/e/f/g/h/i", "/a/b/c/d/e", "/f/g/h/i", false, NN4_TARGET},
                // /a/b/e/c/d/a/g/h/i resolves to /a/b/e and /c/d/a/g/h/i
                new Object[] {"/a/b/e/c/d/a/g/h/i", "/a/b/e", "/c/d/a/g/h/i", false, NN2_TARGET},
                // /a/b/a/c/d/a/g/h/i resolves to /a/b and /a/c/d/a/g/h/i
                new Object[] {"/a/b/a/c/d/a/g/h/i", "/a/b", "/a/c/d/a/g/h/i", false, NN1_TARGET},
                // /b/c/d/e/d/a/g/h/i resolves to /b/c/d/e and /d/a/g/h/i
                new Object[] {"/b/c/d/e/d/a/g/h/i", "/b/c/d/e", "/d/a/g/h/i", false, NN5_TARGET},
                // /b/c/d/e/f/d/a/g/h/i resolves to /b/c/d/e/f and /d/a/g/h/i
                new Object[] {"/b/c/d/e/f/d/a/g/h/i", "/b/c/d/e/f", "/d/a/g/h/i", false, NN6_TARGET},
                // Old Test 3 testPathResolveToDirLink
                // /a/b/c/d/f resolves to /a/b/c/d, /f
                new Object[] {"/a/b/c/d/f", "/a/b/c/d", "/f", true, NN3_TARGET},
                // /a/b/c/d resolves to /a/b/c/d and /
                new Object[] {"/a/b/c/d", "/a/b/c/d", "/", true, NN3_TARGET},
                // /a/b/c/d/f/g/h/i resolves to /a/b/c/d and /f/g/h/i
                new Object[] {"/a/b/c/d/f/g/h/i", "/a/b/c/d", "/f/g/h/i", true, NN3_TARGET},
                // Old Test 4 testPathResolveToDirLinkNotResolveLastComponent
                // /a/b/c/d/f resolves to /a/b/c/d, /f
                new Object[] {"/a/b/c/d/f", "/a/b/c/d", "/f", false, NN3_TARGET},
                // /a/b/c/d resolves to /a/b and /c/d
                new Object[] {"/a/b/c/d", "/a/b", "/c/d", false, NN1_TARGET},
                // /a/b/c/d/f/g/h/i resolves to /a/b/c/d and /f/g/h/i
                new Object[] {"/a/b/c/d/f/g/h/i", "/a/b/c/d", "/f/g/h/i", false, NN3_TARGET},
                // Old Test 5 testMultiNestedMountPointsPathResolveToDirLink
                // /a/b/f resolves to /a/b and /f
                new Object[] {"/a/b/f", "/a/b", "/f", true, NN1_TARGET},
                // /a/b resolves to /a/b and /
                new Object[] {"/a/b", "/a/b", "/", true, NN1_TARGET},

    };
  }

  @Test
  @Parameters(method = "valueSetForTestPathResolveToLink")
  public void testPathResolveToLink(String path, String resolvedPath, String remainingPath,
        boolean resolveLastComponent, URI expectedURI) throws Exception {

    InodeTree.ResolveResult resolveResult = inodeTree.resolve(path, resolveLastComponent);
    Assert.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult.kind);
    Assert.assertEquals(resolvedPath, resolveResult.resolvedPath);
    Assert.assertEquals(new Path(remainingPath), resolveResult.remainingPath);
    Assert.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assert.assertEquals(expectedURI, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri());
    Assert.assertTrue(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testMultiNestedMountPointsPathResolveToDirLinkNotResolveLastComponent() throws Exception {
    // /a/b/f resolves to /a/b and /f
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/b/f", false);
    Assert.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult.kind);
    Assert.assertEquals("/a/b", resolveResult.resolvedPath);
    Assert.assertEquals(new Path("/f"), resolveResult.remainingPath);
    Assert.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assert.assertEquals(NN1_TARGET, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri());
    Assert.assertTrue(resolveResult.isLastInternalDirLink());

    // /a/b resolves to /a and /b
    InodeTree.ResolveResult resolveResult2 = inodeTree.resolve("/a/b", false);
    Assert.assertEquals(InodeTree.ResultKind.INTERNAL_DIR, resolveResult2.kind);
    Assert.assertEquals("/a", resolveResult2.resolvedPath);
    Assert.assertEquals(new Path("/b"), resolveResult2.remainingPath);
    Assert.assertTrue(resolveResult2.targetFileSystem instanceof TestNestMountPointInternalFileSystem);
    Assert.assertEquals(fsUri, ((TestNestMountPointInternalFileSystem) resolveResult2.targetFileSystem).getUri());
    Assert.assertFalse(resolveResult2.isLastInternalDirLink());
  }

  @Test
  public void testPathResolveToDirLinkLastComponentInternalDir() throws Exception {
    // /a/b/c resolves to /a/b and /c
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/b/c", true);
    Assert.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult.kind);
    Assert.assertEquals("/a/b", resolveResult.resolvedPath);
    Assert.assertEquals(new Path("/c"), resolveResult.remainingPath);
    Assert.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assert.assertEquals(NN1_TARGET, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri());
    Assert.assertTrue(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testPathResolveToDirLinkLastComponentInternalDirNotResolveLastComponent() throws Exception {
    // /a/b/c resolves to /a/b and /c
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/b/c", false);
    Assert.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult.kind);
    Assert.assertEquals("/a/b", resolveResult.resolvedPath);
    Assert.assertEquals(new Path("/c"), resolveResult.remainingPath);
    Assert.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assert.assertEquals(NN1_TARGET, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri());
    Assert.assertTrue(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testPathResolveToLinkFallBack() throws Exception {
    // /a/e resolves to linkfallback
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/e", true);
    Assert.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult.kind);
    Assert.assertEquals("/", resolveResult.resolvedPath);
    Assert.assertEquals(new Path("/a/e"), resolveResult.remainingPath);
    Assert.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assert.assertEquals(LINKFALLBACK_TARGET, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri());
    Assert.assertFalse(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testPathNotResolveToLinkFallBackNotResolveLastComponent() throws Exception {
    // /a/e resolves to internalDir instead of linkfallback
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/e", false);
    Assert.assertEquals(InodeTree.ResultKind.INTERNAL_DIR, resolveResult.kind);
    Assert.assertEquals("/a", resolveResult.resolvedPath);
    Assert.assertEquals(new Path("/e"), resolveResult.remainingPath);
    Assert.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointInternalFileSystem);
    Assert.assertEquals(fsUri, ((TestNestMountPointInternalFileSystem) resolveResult.targetFileSystem).getUri());
    Assert.assertFalse(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testPathResolveToInternalDir() throws Exception {
    // /b/c resolves to internal dir
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/b/c", true);
    Assert.assertEquals(InodeTree.ResultKind.INTERNAL_DIR, resolveResult.kind);
    Assert.assertEquals("/b/c", resolveResult.resolvedPath);
    Assert.assertEquals(new Path("/"), resolveResult.remainingPath);
    Assert.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointInternalFileSystem);
    Assert.assertEquals(fsUri, ((TestNestMountPointInternalFileSystem) resolveResult.targetFileSystem).getUri());
    Assert.assertFalse(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testPathResolveToInternalDirNotResolveLastComponent() throws Exception {
    // /b/c resolves to internal dir
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/b/c", false);
    Assert.assertEquals(InodeTree.ResultKind.INTERNAL_DIR, resolveResult.kind);
    Assert.assertEquals("/b", resolveResult.resolvedPath);
    Assert.assertEquals(new Path("/c"), resolveResult.remainingPath);
    Assert.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointInternalFileSystem);
    Assert.assertEquals(fsUri, ((TestNestMountPointInternalFileSystem) resolveResult.targetFileSystem).getUri());
    Assert.assertFalse(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testSlashResolveToInternalDir() throws Exception {
    // / resolves to internal dir
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/", true);
    Assert.assertEquals(InodeTree.ResultKind.INTERNAL_DIR, resolveResult.kind);
    Assert.assertEquals("/", resolveResult.resolvedPath);
    Assert.assertEquals(new Path("/"), resolveResult.remainingPath);
    Assert.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointInternalFileSystem);
    Assert.assertFalse(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testInodeTreeMountPoints() throws Exception {
    List<InodeTree.MountPoint<FileSystem>> mountPoints = inodeTree.getMountPoints();
    Assert.assertEquals(6, mountPoints.size());
  }
}
