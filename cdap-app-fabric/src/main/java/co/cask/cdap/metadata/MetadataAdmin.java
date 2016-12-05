/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.common.InvalidMetadataException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Interface that the {@link MetadataHttpHandler} uses to interact with Metadata.
 * All the create, update and remove operations through this interface are restricted to {@link MetadataScope#USER},
 * so that clients of the RESTful API cannot create, update or remove {@link MetadataScope#SYSTEM} metadata.
 * The operations to retrieve metadata properties and tags allow passing in a scope, so clients of the RESTful API
 * can retrieve both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM} metadata.
 */
public interface MetadataAdmin {

  /**
   * Adds the specified {@link Map} to the metadata of the specified {@link NamespacedEntityId namespacedEntityId}.
   * Existing keys are updated with new values, newer keys are appended to the metadata. This API only supports adding
   * properties in {@link MetadataScope#USER}.
   *
   * @throws NotFoundException if the specified entity was not found
   * @throws InvalidMetadataException if some of the properties violate metadata validation rules
   */
  void addProperties(NamespacedEntityId namespacedEntityId, Map<String, String> properties)
    throws NotFoundException, InvalidMetadataException;

  /**
   * Adds the specified tags to specified {@link NamespacedEntityId}. This API only supports adding tags in
   * {@link MetadataScope#USER}.
   *
   * @throws NotFoundException if the specified entity was not found
   * @throws InvalidMetadataException if some of the properties violate metadata validation rules
   */
  void addTags(NamespacedEntityId namespacedEntityId, String... tags)
    throws NotFoundException, InvalidMetadataException;

  /**
   * Returns a set of {@link MetadataRecord} representing all metadata (including properties and tags) for the specified
   * {@link NamespacedEntityId} in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   *
   * @throws NotFoundException if the specified entity was not found
   */
  Set<MetadataRecord> getMetadata(NamespacedEntityId namespacedEntityId) throws NotFoundException;

  /**
   * Returns a set of {@link MetadataRecord} representing all metadata (including properties and tags) for the specified
   * {@link NamespacedEntityId} in the specified {@link MetadataScope}.
   *
   * @throws NotFoundException if the specified entity was not found
   */
  // TODO: Should this return a single metadata record instead or is a set of one record ok?
  Set<MetadataRecord> getMetadata(MetadataScope scope, NamespacedEntityId namespacedEntityId) throws NotFoundException;

  /**
   * @return a {@link Map} representing the metadata of the specified {@link NamespacedEntityId} in both
   * {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}
   * @throws NotFoundException if the specified entity was not found
   */
  // TODO: This should perhaps return a Map<MetadataScope, Map<String, String>>
  Map<String, String> getProperties(NamespacedEntityId namespacedEntityId) throws NotFoundException;

  /**
   * @return a {@link Map} representing the metadata of the specified {@link NamespacedEntityId} in the specified
   * {@link MetadataScope}
   * @throws NotFoundException if the specified entity was not found
   */
  Map<String, String> getProperties(MetadataScope scope, NamespacedEntityId namespacedEntityId)
    throws NotFoundException;

  /**
   * @return all the tags for the specified {@link NamespacedEntityId} in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}
   * @throws NotFoundException if the specified entity was not found
   */
  // TODO: This should perhaps return a Map<MetadataScope, Set<String>>
  Set<String> getTags(NamespacedEntityId namespacedEntityId) throws NotFoundException;

  /**
   * @return all the tags for the specified {@link NamespacedEntityId} in the specified {@link MetadataScope}
   * @throws NotFoundException if the specified entity was not found
   */
  Set<String> getTags(MetadataScope scope, NamespacedEntityId namespacedEntityId) throws NotFoundException;

  /**
   * Removes all the metadata (including properties and tags) for the specified {@link NamespacedEntityId}. This
   * API only supports removing metadata in {@link MetadataScope#USER}.
   *
   * @param namespacedEntityId the {@link NamespacedEntityId} to remove metadata for
   * @throws NotFoundException if the specified entity was not found
   */
  void removeMetadata(NamespacedEntityId namespacedEntityId) throws NotFoundException;

  /**
   * Removes all properties from the metadata of the specified {@link NamespacedEntityId}. This API only supports
   * removing properties in {@link MetadataScope#USER}.
   *
   * @param namespacedEntityId the {@link NamespacedEntityId} to remove properties for
   * @throws NotFoundException if the specified entity was not found
   */
  void removeProperties(NamespacedEntityId namespacedEntityId) throws NotFoundException;

  /**
   * Removes the specified keys from the metadata properties of the specified {@link NamespacedEntityId}. This API only
   * supports removing properties in {@link MetadataScope#USER}.
   *
   * @param namespacedEntityId the {@link NamespacedEntityId} to remove the specified properties for
   * @param keys the metadata property keys to remove
   * @throws NotFoundException if the specified entity was not found
   */
  void removeProperties(NamespacedEntityId namespacedEntityId, String... keys) throws NotFoundException;

  /**
   * Removes all tags from the specified {@link NamespacedEntityId}. This API only supports removing tags in
   * {@link MetadataScope#USER}.
   *
   * @param namespacedEntityId the {@link NamespacedEntityId} to remove tags for
   * @throws NotFoundException if the specified entity was not found
   */
  void removeTags(NamespacedEntityId namespacedEntityId) throws NotFoundException;

  /**
   * Removes the specified tags from the specified {@link NamespacedEntityId}. This API only supports removing tags in
   * {@link MetadataScope#USER}.
   *
   * @param namespacedEntityId the {@link NamespacedEntityId} to remove the specified tags for
   * @param tags the tags to remove
   * @throws NotFoundException if the specified entity was not found
   */
  void removeTags(NamespacedEntityId namespacedEntityId, String ... tags) throws NotFoundException;

  /**
   * Executes a search for CDAP entities in the specified namespace with the specified search query and
   * an optional set of {@link MetadataSearchTargetType entity types} in the specified {@link MetadataScope}.
   *
   * @param namespaceId The namespace id to filter the search by
   * @param searchQuery The search query
   * @param types The types of CDAP entity to be searched. If empty all possible types will be searched
   * @param sort The sort string representing information for sorting. This string can be in a format that the
   *             configured search/indexing provider supports. If {@code null}, no additional sorting must be performed
   * @return a {@link Set} containing a {@link MetadataSearchResultRecord} for each matching entity
   */
  Set<MetadataSearchResultRecord> search(String namespaceId, String searchQuery, Set<MetadataSearchTargetType> types,
                                         @Nullable String sort) throws Exception;
}
