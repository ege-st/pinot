/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.function.scalar;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Inbuilt array scalar functions. See {@link ArrayUtils} for details.
 */
public class MapFunctions {
  private MapFunctions() {
  }

  @ScalarFunction
  public static int[] arrayReverseInt(int[] values) {
    int[] clone = values.clone();
    ArrayUtils.reverse(clone);
    return clone;
  }

  @ScalarFunction
  public static String[] arrayReverseString(String[] values) {
    String[] clone = values.clone();
    ArrayUtils.reverse(clone);
    return clone;
  }

  @ScalarFunction
  public static int mapElementForKey(@Nullable Map<String, Integer> map, String key) {
    if(map != null) {
      return map.get(key);
    } else {
      throw new UnsupportedOperationException("Key not found not yet implemented");
    }
  }
}
