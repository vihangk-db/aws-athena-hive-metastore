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

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TableInfo {
    private String name;

    @JsonProperty("catalog_name")
    private String catalogName;

    @JsonProperty("schema_name")
    private String schemaName;

    @JsonProperty("table_type")
    private String tableType;

    @JsonProperty("data_source_format")
    private String dataSourceFormat;

    @JsonProperty("columns")
    private ColumnInfo[] columnInfo;

    @JsonProperty("storage_location")
    private String storageLocation;

    private String owner;

    private int generation;

    @JsonProperty("metastore_id")
    private String metastoreid;

    @JsonProperty("full_name")
    private String fullName;

    @JsonProperty("created_at")
    private long createdAt;

    @JsonProperty("updated_at")
    private long updatedAt;

    @JsonProperty("created_by")
    private String createdBy;

    @JsonProperty("updated_by")
    private String updatedBy;

    @JsonProperty("delta_runtime_properties_kvpairs")
    private Map<String, String> deltaRuntimeProperties;

    public TableInfo() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public String getDataSourceFormat() {
        return dataSourceFormat;
    }

    public void setDataSourceFormat(String dataSourceFormat) {
        this.dataSourceFormat = dataSourceFormat;
    }

    public ColumnInfo[] getColumnInfo() {
        return columnInfo;
    }

    public void setColumnInfo(ColumnInfo[] columnInfo) {
        this.columnInfo = columnInfo;
    }

    public String getStorageLocation() {
        return storageLocation;
    }

    public void setStorageLocation(String storageLocation) {
        this.storageLocation = storageLocation;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public int getGeneration() {
        return generation;
    }

    public void setGeneration(int generation) {
        this.generation = generation;
    }

    public String getMetastoreid() {
        return metastoreid;
    }

    public void setMetastoreid(String metastoreid) {
        this.metastoreid = metastoreid;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public Map<String, String> getDeltaRuntimeProperties() {
        return deltaRuntimeProperties;
    }

    public void setDeltaRuntimeProperties(Map<String, String> deltaRuntimeProperties) {
        this.deltaRuntimeProperties = deltaRuntimeProperties;
    }
}
