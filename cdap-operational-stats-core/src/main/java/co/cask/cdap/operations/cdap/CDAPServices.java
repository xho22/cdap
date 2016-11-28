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

import co.cask.cdap.operations.OperationalStats;

import java.io.IOException;

/**
 * {@link OperationalStats} for CDAP.
 */
@SuppressWarnings("unused")
public class CDAPServices extends AbstractCDAPStats implements CDAPServicesMXBean {

  @Override
  public String getStatType() {
    return "services";
  }

  @Override
  public int getMasters() {
    return 1;
  }

  @Override
  public int getAuthServers() {
    return 1;
  }

  @Override
  public int getRouters() {
    return 1;
  }

  @Override
  public int getKafkaServers() {
    return 1;
  }

  @Override
  public void collect() throws IOException {

  }
}
