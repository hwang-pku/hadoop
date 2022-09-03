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

package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.hadoop.fs.shell.PathData;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class TestAnd {

  @Rule
  public Timeout globalTimeout = new Timeout(10000, TimeUnit.MILLISECONDS);

  private Object[] valueSetsForDifferentCases() {
    return new Object[] {
                // Test 1 testPass ->  test all expressions passing
                new Object[] {Result.PASS, Result.PASS, Result.PASS},
                // Test 2 testFailFirst -> test the first expression failing
                new Object[] {Result.FAIL, Result.PASS, Result.FAIL},
    };
  }


  @Test
  @Parameters(method = "valueSetsForDifferentCases")
  public void testPass(Result firstResult, Result secondResult, Result expectedResult) throws IOException {
    And and = new And();

    PathData pathData = mock(PathData.class);

    Expression first = mock(Expression.class);
    when(first.apply(pathData, -1)).thenReturn(firstResult);

    Expression second = mock(Expression.class);
    when(second.apply(pathData, -1)).thenReturn(secondResult);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    assertEquals(expectedResult, and.apply(pathData, -1));
    verify(first).apply(pathData, -1);
    if (firstResult == Result.PASS) {
        verify(second).apply(pathData, -1);
    }
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }


  // test the second expression failing
  @Test
  public void testFailSecond() throws IOException {
    testPass(Result.PASS, Result.FAIL, Result.FAIL);
  }

  // test both expressions failing
  @Test
  public void testFailBoth() throws IOException {
    And and = new And();

    PathData pathData = mock(PathData.class);

    Expression first = mock(Expression.class);
    when(first.apply(pathData, -1)).thenReturn(Result.FAIL);

    Expression second = mock(Expression.class);
    when(second.apply(pathData, -1)).thenReturn(Result.FAIL);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    assertEquals(Result.FAIL, and.apply(pathData, -1));
    verify(first).apply(pathData, -1);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  // test the first expression stopping
  @Test
  public void testStopFirst() throws IOException {
    testPass(Result.STOP, Result.PASS, Result.STOP);
  }

  // test the second expression stopping
  @Test
  public void testStopSecond() throws IOException {
    testPass(Result.PASS, Result.STOP, Result.STOP);
  }

  // test first expression stopping and second failing
  @Test
  public void testStopFail() throws IOException {
    testPass(Result.STOP, Result.FAIL, Result.STOP.combine(Result.FAIL));
  }

  // test setOptions is called on child
  @Test
  public void testSetOptions() throws IOException {
    And and = new And();
    Expression first = mock(Expression.class);
    Expression second = mock(Expression.class);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    FindOptions options = mock(FindOptions.class);
    and.setOptions(options);
    verify(first).setOptions(options);
    verify(second).setOptions(options);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  // test prepare is called on child
  @Test
  public void testPrepare() throws IOException {
    And and = new And();
    Expression first = mock(Expression.class);
    Expression second = mock(Expression.class);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    and.prepare();
    verify(first).prepare();
    verify(second).prepare();
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  // test finish is called on child
  @Test
  public void testFinish() throws IOException {
    And and = new And();
    Expression first = mock(Expression.class);
    Expression second = mock(Expression.class);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    and.finish();
    verify(first).finish();
    verify(second).finish();
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }
}
