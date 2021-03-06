/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.messaging;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;

/**
 * Utility class for table operations.
 */
public final class MessagingUtils {

  private MessagingUtils() {
    // prevent object creation
  }

  /**
   * Constants
   */
  public static final class Constants {
    public static final String DEFAULT_GENERATION = Integer.toString(1);
  }

  /**
   * Convert {@link TopicId} to byte array to be used for metadata table row key.
   *
   * @param topicId {@link TopicId}
   * @return byte array representation for the topic id
   */
  public static byte[] toMetadataRowKey(TopicId topicId) {
    String topic = topicId.getNamespace() + ":" + topicId.getTopic() + ":";
    return Bytes.toBytes(topic);
  }

  /**
   * Convert {@link TopicId} and generation id to byte array to be used for data tables (message and payload) as
   * row key prefix.
   *
   * @param topicId {@link TopicId}
   * @param generation generation id of the topic
   * @return byte array representation to be used as row key prefix for data tables
   */
  public static byte[] toDataKeyPrefix(TopicId topicId, int generation) {
    byte[] metadataRowKey = toMetadataRowKey(topicId);
    byte[] keyPrefix = new byte[metadataRowKey.length + Bytes.SIZEOF_INT];
    Bytes.putBytes(keyPrefix, 0, metadataRowKey, 0, metadataRowKey.length);
    Bytes.putInt(keyPrefix, metadataRowKey.length, generation);
    return keyPrefix;
  }

  /**
   * Convert byte array encoded with the {@link #toMetadataRowKey(TopicId)} method back to the {@link TopicId}
   *
   * @param topicBytes byte array which contains the representation of the topic id
   * @param offset offset to start decoding
   * @param length number of bytes to decode
   * @return {@link TopicId}
   */
  public static TopicId toTopicId(byte[] topicBytes, int offset, int length) {
    String topic = Bytes.toString(topicBytes, offset, length);
    int firstSeparator = topic.indexOf(":", offset);
    String ns = topic.substring(offset, firstSeparator);
    String topicId = topic.substring(firstSeparator + 1, topic.length() - 1);
    return new TopicId(ns, topicId);
  }

  /**
   * Convert byte array encoded with the {@link #toMetadataRowKey(TopicId)} method back to the {@link TopicId}.
   * Same as calling {@link #toTopicId(byte[], int, int)} with {@code offset = 0}
   * and {@code length = topicBytes.length}.
   *
   * @param topicBytes byte array which contains the representation of the topic id
   * @return {@link TopicId}
   */
  public static TopicId toTopicId(byte[] topicBytes) {
    return toTopicId(topicBytes, 0, topicBytes.length);
  }

  /**
   * Construct the scan key for topicIds which are in a particular {@link NamespaceId}.
   *
   * @param namespaceId Namespace in which the topics are present.
   * @return prefix key that contains the namespace id.
   */
  public static byte[] topicScanKey(NamespaceId namespaceId) {
    return Bytes.toBytes(namespaceId.getNamespace() + ":");
  }
}
