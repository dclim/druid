/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.benchmark.runner;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.jackson.DefaultObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class JsonFileBaselineReader implements BaselineReader
{
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final String filePath;

  public JsonFileBaselineReader(String filePath)
  {
    this.filePath = filePath;
  }

  @Override
  public Map<String, BaselineEntry> getBaseline() throws IOException
  {
    JsonParser parser = mapper.getFactory().createParser(new File(filePath));
    Iterator<JMHRunnerResult> it = parser.readValuesAs(JMHRunnerResult.class);
    Map<String, BaselineEntry> baselines = Maps.newHashMap();

    while (it.hasNext()) {
      JMHRunnerResult entry = it.next();
      if (entry.isBaseline()) {
        BaselineEntry existingBaseline = baselines.get(entry.getId());
        if (existingBaseline == null || existingBaseline.getTimestamp().isBefore(entry.getTimestamp())) {
          baselines.put(entry.getId(), entry);
        }
      }
    }

    return ImmutableMap.copyOf(baselines);
  }
}
