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

package co.cask.cdap.operations.cdap;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.operations.OperationalStats;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.base.Predicates;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

/**
 * {@link OperationalStats} for reporting CDAP entities.
 */
@SuppressWarnings("unused")
public class CDAPEntities extends AbstractCDAPStats implements CDAPEntitiesMXBean {
  private int namespaces;
  private int artifacts;
  private int apps;
  private int programs;
  private int datasets;
  private int streams;
  private int streamViews;

  @Override
  public String getStatType() {
    return "entities";
  }

  @Override
  public int getNamespaces() {
    return namespaces;
  }

  @Override
  public int getArtifacts() {
    return artifacts;
  }

  @Override
  public int getApplications() {
    return apps;
  }

  @Override
  public int getPrograms() {
    return programs;
  }

  @Override
  public int getDatasets() {
    return datasets;
  }

  @Override
  public int getStreams() {
    return streams;
  }

  @Override
  public int getStreamViews() {
    return streamViews;
  }

  @Override
  public void collect() throws IOException {
    NamespaceQueryAdmin nsQueryAdmin = injector.getInstance(NamespaceQueryAdmin.class);
    ApplicationLifecycleService appLifecycleService = injector.getInstance(ApplicationLifecycleService.class);
    ProgramLifecycleService programLifecycleService = injector.getInstance(ProgramLifecycleService.class);
    ArtifactRepository artifactRepository = injector.getInstance(ArtifactRepository.class);
    DatasetFramework dsFramework = injector.getInstance(DatasetFramework.class);
    StreamAdmin streamAdmin = injector.getInstance(StreamAdmin.class);
    try {
      List<NamespaceMeta> namespaceMetas;
      namespaceMetas = nsQueryAdmin.list();
      namespaces = namespaceMetas.size();
      for (NamespaceMeta meta : namespaceMetas) {
        List<ApplicationRecord> appRecords = appLifecycleService.getApps(meta.getNamespaceId(), Predicates.<ApplicationRecord>alwaysTrue());
        apps += appRecords.size();
        for (ProgramType programType : EnumSet.allOf(ProgramType.class)) {
          programs += programLifecycleService.list(meta.getNamespaceId(), programType).size();
        }
        artifacts += artifactRepository.getArtifacts(meta.getNamespaceId(), true).size();
        datasets += dsFramework.getInstances(meta.getNamespaceId()).size();
        List<StreamSpecification> streamSpecs = streamAdmin.listStreams(meta.getNamespaceId());
        streams += streamSpecs.size();
        for (StreamSpecification streamSpec : streamSpecs) {
          StreamId streamId = meta.getNamespaceId().stream(streamSpec.getName());
          streamViews += streamAdmin.listViews(streamId).size();
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
