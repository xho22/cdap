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

package co.cask.cdap.data2.dataset2.preview;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Dataset framework that delegates either to local or shared (actual) dataset framework.
 */
public class PreviewDatasetFramework implements DatasetFramework {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewDatasetFramework.class);
  private static final DatasetAdmin NOOP_DATASET_ADMIN = new DatasetAdmin() {
    @Override
    public boolean exists() throws IOException {
      return true;
    }

    @Override
    public void create() throws IOException {
    }

    @Override
    public void drop() throws IOException {
    }

    @Override
    public void truncate() throws IOException {
    }

    @Override
    public void upgrade() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }
  };

  private final DatasetFramework localDatasetFramework;
  private final DatasetFramework actualDatasetFramework;
  private final Set<String> datasetNames;

  /**
   * Create instance of the {@link PreviewDatasetFramework}.
   * @param local the dataset framework instance in the preview space
   * @param actual the dataset framework instance in the real space
   * @param datasetNames list of dataset names which need to be accessed for read only purpose from the real space
   */
  public PreviewDatasetFramework(DatasetFramework local, DatasetFramework actual, Set<String> datasetNames) {
    this.localDatasetFramework = local;
    this.actualDatasetFramework = actual;
    this.datasetNames = datasetNames;
  }

  @Override
  public void addModule(DatasetModuleId moduleId, DatasetModule module) throws DatasetManagementException {
    localDatasetFramework.addModule(moduleId, module);
  }

  @Override
  public void addModule(DatasetModuleId moduleId, DatasetModule module, Location jarLocation)
    throws DatasetManagementException {
    // called while deploying the new application from DatasetModuleDeployer stage.
    // any new module should be deployed in the preview space.
    localDatasetFramework.addModule(moduleId, module, jarLocation);
  }

  @Override
  public void deleteModule(DatasetModuleId moduleId) throws DatasetManagementException {
    localDatasetFramework.deleteModule(moduleId);
  }

  @Override
  public void deleteAllModules(NamespaceId namespaceId) throws DatasetManagementException {
    localDatasetFramework.deleteAllModules(namespaceId);
  }

  @Override
  public void addInstance(String datasetTypeName, DatasetId datasetInstanceId,
                          DatasetProperties props) throws DatasetManagementException, IOException {
    localDatasetFramework.addInstance(datasetTypeName, datasetInstanceId, props);
  }

  @Override
  public void updateInstance(DatasetId datasetInstanceId,
                             DatasetProperties props) throws DatasetManagementException, IOException {
    // allow updates to the datasets in preview space only
    if (localDatasetFramework.hasInstance(datasetInstanceId)) {
      localDatasetFramework.updateInstance(datasetInstanceId, props);
    }
  }

  @Override
  public Collection<DatasetSpecificationSummary> getInstances(NamespaceId namespaceId)
    throws DatasetManagementException {
    return localDatasetFramework.getInstances(namespaceId);
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(DatasetId datasetInstanceId) throws DatasetManagementException {
    if (datasetNames.contains(datasetInstanceId.getDataset())) {
      return actualDatasetFramework.getDatasetSpec(datasetInstanceId);
    }
    return localDatasetFramework.getDatasetSpec(datasetInstanceId);
  }

  @Override
  public boolean hasInstance(DatasetId datasetInstanceId) throws DatasetManagementException {
    if (datasetNames.contains(datasetInstanceId.getDataset())) {
      return actualDatasetFramework.hasInstance(datasetInstanceId);
    }
    return localDatasetFramework.hasInstance(datasetInstanceId);
  }

  @Override
  public boolean hasSystemType(String typeName) throws DatasetManagementException {
    return localDatasetFramework.hasSystemType(typeName);
  }

  @Override
  public boolean hasType(DatasetTypeId datasetTypeId) throws DatasetManagementException {
    return localDatasetFramework.hasType(datasetTypeId);
  }

  @Nullable
  @Override
  public DatasetTypeMeta getTypeInfo(DatasetTypeId datasetTypeId) throws DatasetManagementException {
    return localDatasetFramework.getTypeInfo(datasetTypeId);
  }

  @Override
  public void truncateInstance(DatasetId datasetInstanceId) throws DatasetManagementException, IOException {
    // If dataset exists in the preview space then only truncate it otherwise its a no-op
    if (localDatasetFramework.hasInstance(datasetInstanceId)) {
      localDatasetFramework.truncateInstance(datasetInstanceId);
    }
  }

  @Override
  public void deleteInstance(DatasetId datasetInstanceId) throws DatasetManagementException, IOException {
    // If dataset exists in the preview space then only delete it otherwise its a no-op
    if (localDatasetFramework.hasInstance(datasetInstanceId)) {
      localDatasetFramework.deleteInstance(datasetInstanceId);
    }
  }

  @Override
  public void deleteAllInstances(NamespaceId namespaceId) throws DatasetManagementException, IOException {
    localDatasetFramework.deleteAllInstances(namespaceId);
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T extends DatasetAdmin> T getAdmin(DatasetId datasetInstanceId, @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    // Return the no-op admin for the dataset from the real space
    if (datasetNames.contains(datasetInstanceId.getDataset())
      && actualDatasetFramework.hasInstance(datasetInstanceId)) {
      return (T) NOOP_DATASET_ADMIN;
    }
    return actualDatasetFramework.getAdmin(datasetInstanceId, classLoader);
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T extends DatasetAdmin> T getAdmin(DatasetId datasetInstanceId, @Nullable ClassLoader classLoader,
                                             DatasetClassLoaderProvider classLoaderProvider)
    throws DatasetManagementException, IOException {
    // Return the no-op admin for the dataset from the real space
    if (datasetNames.contains(datasetInstanceId.getDataset())
      && actualDatasetFramework.hasInstance(datasetInstanceId)) {
      return (T) NOOP_DATASET_ADMIN;
    }
    return actualDatasetFramework.getAdmin(datasetInstanceId, classLoader, classLoaderProvider);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(DatasetId datasetInstanceId, @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    if (datasetNames.contains(datasetInstanceId.getDataset())) {
      return actualDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader);
    }
    return localDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(DatasetId datasetInstanceId, @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader,
                                          DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable Iterable<? extends EntityId> owners,
                                          AccessType accessType) throws DatasetManagementException, IOException {
    if (datasetNames.contains(datasetInstanceId.getDataset())) {
      return actualDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider,
                                               owners, accessType);
    }
    return localDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider, owners,
                                            accessType);
  }

  @Override
  public void writeLineage(DatasetId datasetInstanceId, AccessType accessType) {
    // no-op
  }
}
