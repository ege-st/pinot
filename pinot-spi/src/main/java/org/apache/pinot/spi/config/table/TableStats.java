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

package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonProperty;


/*
 * Container object for metadata info / stats of Pinot tables
 */
public class TableStats {
  public static final String CREATION_TIME_KEY = "creationTime";
  public static final String MODIFICATION_TIME_KEY = "modificationTime";

  private String _creationTime;
  private String _modificationTime;

  public TableStats(String creationTime) {
    this(creationTime, null);
  }

  public TableStats(String creationTime, String modificationTime) {
    _creationTime = creationTime;
    _modificationTime = modificationTime;
  }

  @JsonProperty(CREATION_TIME_KEY)
  public String getCreationTime() {
    return _creationTime;
  }

  @JsonProperty(MODIFICATION_TIME_KEY)
  public String getModificationTime() {
    return _modificationTime;
  }

  public void setCreationTime(String creationTime) {
    _creationTime = creationTime;
  }

  public void setModificationTime(String modificationTime) {
    _modificationTime = modificationTime;
  }
}
