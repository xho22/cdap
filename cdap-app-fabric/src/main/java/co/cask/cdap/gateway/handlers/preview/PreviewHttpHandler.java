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

package co.cask.cdap.gateway.handlers.preview;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.app.preview.PreviewStatus;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * /**
 * {@link co.cask.http.HttpHandler} to manage preview lifecycle for v3 REST APIs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class PreviewHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewHttpHandler.class);
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(Schema.class,
                                                                         new SchemaTypeAdapter()).create();

  @Inject
  public PreviewHttpHandler() {
  }

  @POST
  @Path("/previews")
  public void start(HttpRequest request, HttpResponder responder,
                    @PathParam("namespace-id") String namespaceId) throws Exception {
    String appName = RunIds.generate().getId();
    responder.sendJson(HttpResponseStatus.OK, new ApplicationId(namespaceId, appName));
  }

  @POST
  @Path("/previews/{preview-id}/stop")
  public void stop(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("preview-id") String previewId) throws Exception {
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/previews/{preview-id}/status")
  public void getStatus(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                        @PathParam("preview-id") String previewId)  throws Exception {
    responder.sendJson(HttpResponseStatus.OK, new PreviewStatus(PreviewStatus.Status.COMPLETED));
  }

  @GET
  @Path("/previews/{preview-id}/loggers")
  public void getLoggers(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                         @PathParam("preview-id") String previewId) throws Exception {
    List<String> loggers = Arrays.asList("File", "CSVParser", "Table");
    responder.sendJson(HttpResponseStatus.OK, loggers);
  }

  @GET
  @Path("/previews/{preview-id}/loggers/{logger-id}")
  public void getData(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                      @PathParam("preview-id") String previewId, @PathParam("logger-id") String loggerId)
    throws Exception {
    responder.sendJson(HttpResponseStatus.OK, getMockData());
  }

  static Map<String, List<String>> getMockData() {
    Map<String, List<String>> data = new HashMap<>();
    List<String> records = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String userName = String.format("user_%s_fname,user_%s_lname", i, i);
      records.add(GSON.toJson(new Input(userName, 94306 + i)));
    }
    data.put("input.records", records);

    records = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String userFirstName = String.format("user_%s_fname", i);
      String userLastName = String.format("user_%s_lname", i);
      records.add(GSON.toJson(new Output(userFirstName, userLastName, 94306 + i)));
    }
    data.put("output.records", records);

    return data;
  }

  private static class Input {
    private final String name;
    private int zipcode;

    private Input(String name, int zipcode) {
      this.name = name;
      this.zipcode = zipcode;
    }
  }

  private static class Output {
    private final String firstName;
    private final String lastName;
    private int zipcode;

    private Output(String firstName, String lastName, int zipcode) {
      this.firstName = firstName;
      this.lastName = lastName;
      this.zipcode = zipcode;
    }
  }
}
