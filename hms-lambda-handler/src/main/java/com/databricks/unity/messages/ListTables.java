/*-
 * #%L
 * hms-lambda-handler
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.databricks.unity.messages;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ListTables {
    @JsonProperty("tables")
    private TableInfo[] tables;

    public ListTables() {
    }

    public TableInfo[] getTables() {
        return tables;
    }

    public void setTables(TableInfo[] tables) {
        this.tables = tables;
    }

    public List<TableInfo> getTableInfos() {
        List<TableInfo> tableInfoList = new ArrayList<>();
        if (tables == null) return tableInfoList;
        for (TableInfo t : tables) {
            tableInfoList.add(t);
        }
        return tableInfoList;
    }
}
