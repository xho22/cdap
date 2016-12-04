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

package co.cask.cdap.data2.metadata.dataset;

import javax.annotation.Nullable;

/**
 * Represents sorting info for search results.
 */
public class SortInfo {

  public static final SortInfo DEFAULT = new SortInfo(null, SortOrder.WEIGHTED);

  /**
   * Represents sorting order.
   */
  public enum SortOrder {
    ASC,
    DESC,
    WEIGHTED
  }

  private final String sortBy;
  private final SortOrder sortOrder;

  public SortInfo(@Nullable String sortBy, SortOrder sortOrder) {
    this.sortBy = sortBy;
    this.sortOrder = sortOrder;
  }

  /**
   * Returns the sort by column, unless the column does not matter, when the sort order is {@link SortOrder#WEIGHTED}.
   */
  @Nullable
  public String getSortBy() {
    return sortBy;
  }

  public SortOrder getSortOrder() {
    return sortOrder;
  }
}
