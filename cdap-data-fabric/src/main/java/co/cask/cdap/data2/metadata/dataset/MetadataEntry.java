/*
 * Copyright 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.proto.id.NamespacedEntityId;

import java.util.Objects;

/**
 * Represents a single Metadata entry for a CDAP Entity.
 */
public class MetadataEntry {
  private final NamespacedEntityId targetId;
  private final String key;
  private final String value;
  private final String schema;
  // TODO: Doing this so we can return cursors, but this may not be the most optimal way to do it.
  private final String rowKey;

  public MetadataEntry(NamespacedEntityId targetId, String key, String value) {
    this.targetId = targetId;
    this.key = key;
    this.value = value;
    this.schema = null;
    this.rowKey = null;
  }

  public NamespacedEntityId getTargetId() {
    return targetId;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public String getSchema() {
    return schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MetadataEntry that = (MetadataEntry) o;

    return Objects.equals(targetId, that.targetId) &&
      Objects.equals(key, that.key) &&
      Objects.equals(value, that.value) &&
      Objects.equals(schema, that.schema) &&
      Objects.equals(rowKey, that.rowKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetId, key, value, schema);
  }

  @Override
  public String toString() {
    return "{" +
      "targetId='" + targetId + '\'' +
      ", key='" + key + '\'' +
      ", value='" + value + '\'' +
      ", schema='" + (schema != null ? schema : 0) +
      '}';
  }
}
