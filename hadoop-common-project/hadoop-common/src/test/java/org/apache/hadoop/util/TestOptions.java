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
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

@RunWith(JUnitParamsRunner.class)
public class TestOptions {

  private Object[] valueSetForTestAppend() {
    return new Object[] {
                new Object[] {new String[]{"hi", "there"}, new String[]{"Dr.", "Who"}},
                new Object[] {new String[]{"dd", "ee", "ff"}, new String[]{"aa", "bb", "cc"}},
                new Object[] {new String[]{"7", "8", "9"}, new String[]{"1", "2", "3", "4", "5", "6"}},
                new Object[] {new String[]{"      ", "       ", "          "}, new String[]{" ", "    ", "    "}},
                new Object[] {new String[]{""}, new String[]{""}},
                new Object[] {new String[]{}, new String[]{"aa", "bb", "cc"}},
                new Object[] {new String[]{}, new String[]{"Dr.", "Who"}},
                new Object[] {new String[]{}, new String[]{}},
                new Object[] {new String[]{null, null, null, null}, new String[]{null, null}},
                new Object[] {new String[]{}, new String[]{null}},
                new Object[] {new String[]{null}, new String[]{}},
                new Object[] {new String[]{"!@#$%^&*()_+=-{}][|;',./?><"}, new String[]{"!@#$%^&*()_+=-{}][|;',./?><"}},
    };
  }

  @Test
  @Parameters(method = "valueSetForTestAppend")
  public void testAppend(String[] oldOpts, String[] newOpts) throws Exception {
    assertArrayEquals("append failure",
                      ArrayUtils.addAll(newOpts, oldOpts),
                      Options.prependOptions(oldOpts,
                                             newOpts));
  }

  private Object[] valueSetForTestFind() {
    return new Object[] {
            new Object[] {new Object[]{1, "hi", true, "bye", 'x'}, 0, 1, 2},
    };
  }

  @Test
  @Parameters(method = "valueSetForTestFind")
  public void testFind(Object[] objects, int intInd, int stringInd, int boolInd) throws Exception {
     assertEquals(objects[intInd], Options.getOption(Integer.class, objects).intValue());
     assertEquals(objects[stringInd], Options.getOption(String.class, objects));
     assertEquals(objects[boolInd], Options.getOption(Boolean.class, objects).booleanValue());
  }  
}
