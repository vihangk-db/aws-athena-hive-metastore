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
package com.databricks.unity;

import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.databricks.unity.messages.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpMessage;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;

public class UnityCatalogClient implements HiveMetaStoreClient {
    private static final Logger LOG = LoggerFactory.getLogger(UnityCatalogClient.class);
    private final String defaultCatalog;
    private final URI endpoint;
    private final Map<String, String> commonHeaders = new HashMap<>();

    public UnityCatalogClient(String defaultCatalog, String token, String endpoint) {
        this.defaultCatalog = defaultCatalog;
        this.endpoint = URI.create(endpoint);
        this.commonHeaders.put(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        this.commonHeaders.put(HttpHeaders.CONTENT_TYPE, "application/json");
        this.commonHeaders.put(HttpHeaders.ACCEPT, "application/json");
    }

    private static class HttpGetWithEntity extends HttpEntityEnclosingRequestBase {

        public HttpGetWithEntity(String uri) {
            super();
            setURI(URI.create(uri));
        }

        public HttpGetWithEntity(URI uri) {
            super();
            setURI(uri);
        }

        @Override
        public String getMethod() {
            return "GET";
        }
    }

    @Override
    public boolean dbExists(String dbName) throws TException {
        try {
            getDatabase(dbName);
            return true;
        } catch (NoSuchObjectException e) {
            return false;
        }
    }

    @Override
    public boolean tableExists(String dbName, String tableName) throws TException {
        try {
            getTable(dbName, tableName);
            return true;
        } catch (NoSuchObjectException e) {
            return false;
        }
    }

    @Override
    public Database getDatabase(String dbName) throws TException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet get = new HttpGet(endpoint + "/schemas/" + defaultCatalog + "." + dbName);
            addCommonHeaders(get);
            HttpResponse httpResponse = httpClient.execute(get);
            handleErrorResponse(httpResponse);
            String response = IOUtils.toString(httpResponse.getEntity().getContent());
            SchemaInfo schemaInfo = new ObjectMapper()
                    .readerFor(SchemaInfo.class)
                    .readValue(response);
            return getDatabase(schemaInfo);
        } catch (IOException ex) {
            throw convertToTException(ex);
        }
    }

    private void handleErrorResponse(HttpResponse response) throws TException {
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200) {
            return;
        }
        // handle 4xx
        ErrorResponse errorResponse = null;
        try {
            errorResponse = new ObjectMapper()
                    .readerFor(ErrorResponse.class)
                    .readValue(IOUtils.toString(response.getEntity().getContent()));
        } catch (Exception ex) {
            throw convertToTException(ex);
        }
        if (statusCode == 404) {
            switch (errorResponse.getErrorCode()) {
                case "SCHEMA_DOES_NOT_EXIST":
                case "TABLE_DOES_NOT_EXIST":
                case "CATALOG_DOES_NOT_EXIST":
                    throw new NoSuchObjectException(errorResponse.getMessage());
                default:
                    throw new TException(errorResponse.getMessage());
            }
        }
        // handle 5xx
        throw new TException(errorResponse.getErrorCode() + ":" + errorResponse.getMessage());
    }

    private Database getDatabase(SchemaInfo schemaInfo) {
        Database database = new Database();
        database.setName(schemaInfo.getName());
        database.setParameters(schemaInfo.getProperties());
        database.setDescription(schemaInfo.getComment());
        database.setOwnerName(schemaInfo.getOwner());
        return database;
    }

    private void addCommonHeaders(HttpMessage get) {
        for (String name : commonHeaders.keySet()) {
            get.setHeader(name, commonHeaders.get(name));
        }
    }

    private String getJsonResponse(HttpResponse response) throws IOException {
        return IOUtils.toString(response.getEntity().getContent());
    }

    private List<SchemaInfo> getSchemaInfos(String filter) throws TException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGetWithEntity httpGet = new HttpGetWithEntity(endpoint + "/schemas");
            addCommonHeaders(httpGet);
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append(String.format("\"catalog_name\":\"%s\"", defaultCatalog));
            sb.append("}");
            httpGet.setEntity(new StringEntity(sb.toString()));
            HttpResponse httpResponse = httpClient.execute(httpGet);
            handleErrorResponse(httpResponse);
            String json = getJsonResponse(httpResponse);
            ListSchemas schemaInfos = new ObjectMapper()
                    .readerFor(ListSchemas.class)
                    .readValue(json);
            return schemaInfos.getSchemaInfos();
        } catch (IOException ex) {
            throw convertToTException(ex);
        }
    }

    @Override
    public Set<String> getDatabaseNames(String filter) throws TException {
        List<SchemaInfo> schemaInfoList = getSchemaInfos(filter);
        Set<String> schemaNames = new HashSet<>();
        for (SchemaInfo si : schemaInfoList) {
            schemaNames.add(si.getName());
        }
        return schemaNames;
    }

    private TException convertToTException(Exception ex) {
        LOG.error("Exception while getting a response from unity catalog", ex);
        return new TException("Exception while getting a response from unity catalog: " + ex.getMessage());
    }

    @Override
    public List<Database> getDatabases(String filter) throws TException {
        List<SchemaInfo> schemaInfoList = getSchemaInfos(filter);
        List<Database> databases = new ArrayList<>();
        for (SchemaInfo s : schemaInfoList) {
            databases.add(getDatabase(s));
        }
        return databases;
    }

    @Override
    public List<Database> getDatabasesByNames(List<String> dbNames) throws TException {
        List<Database> databases = new ArrayList<>();
        for (String dbName : dbNames) {
            databases.add(getDatabase(dbName));
        }
        return databases;
    }

    private List<TableInfo> getTableInfos(String dbName) throws TException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGetWithEntity httpGet = new HttpGetWithEntity(endpoint + "/tables");
            addCommonHeaders(httpGet);
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append(String.format("\"%s\":\"%s\",", "catalog_name", defaultCatalog));
            sb.append(String.format("\"%s\":\"%s\"", "schema_name", dbName));
            sb.append("}");
            httpGet.setEntity(new StringEntity(sb.toString()));
            HttpResponse response = httpClient.execute(httpGet);
            handleErrorResponse(response);
            String json = getJsonResponse(response);
            ListTables listTables = new ObjectMapper()
                    .readerFor(ListTables.class)
                    .readValue(json);
            return listTables.getTableInfos();
        } catch (IOException ex) {
            throw convertToTException(ex);
        }
    }

    @Override
    public Set<String> getTableNames(String dbName, String filter) throws TException {
        List<TableInfo> tableInfos = getTableInfos(dbName);
        Set<String> tableNames = new HashSet<>();
        for (TableInfo ti : tableInfos) {
            tableNames.add(ti.getName().toLowerCase(Locale.ROOT));
        }
        return tableNames;
    }

    @Override
    public List<Table> getTablesByNames(String dbName, List<String> tableNames) throws TException {
        List<Table> tables = new ArrayList<>();
        for (String tableName : tableNames) {
            tables.add(getTable(dbName, tableName));
        }
        return tables;
    }

    @Override
    public boolean createDatabase(String name) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean createDatabase(String name, String description, String location, Map<String, String> params) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean createDatabase(Database db) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean dropDatabase(String dbName) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean createTable(Table table) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean dropTable(String dbName, String tableName) throws TException {
        throw new TException("method not implemented");
    }

    private TableInfo getTableInfo(String dbName, String tableName) throws TException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            String tblFullName = String.format("%s.%s.%s", defaultCatalog, dbName, tableName);
            HttpGet httpGet = new HttpGet(endpoint + "/tables" + "/" + tblFullName);
            addCommonHeaders(httpGet);
            HttpResponse response = httpClient.execute(httpGet);
            handleErrorResponse(response);
            String responseJson = IOUtils.toString(response.getEntity().getContent());
            return new ObjectMapper()
                    .readerFor(TableInfo.class)
                    .readValue(responseJson);
        } catch (IOException ex) {
            throw convertToTException(ex);
        }
    }

    private String getTableType(String type) throws Exception {
        switch (type) {
            case "EXTERNAL":
                return "EXTERNAL_TABLE";
            case "MANAGED":
                return "MANAGED_TABLE";
            case "VIEW":
                return "VIRTUAL_VIEW";
            default:
                throw new Exception("Unrecognized table type " + type);
        }
    }

    private String[] getInputOutputFormat(String provider) throws Exception {
        switch(provider.toLowerCase()) {
            case "parquet":
                return new String[] {
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                };
            default:
                throw new Exception("Unsupported provider " + provider);
        }
    }

    private List<FieldSchema> getFieldSchema(TableInfo tableInfo) throws Exception {
        List<ColumnInfo> columnInfoList = new ArrayList<>(Arrays.asList(tableInfo.getColumnInfo()));
        columnInfoList.sort(Comparator.comparingInt(ColumnInfo::getPosition));
        List<FieldSchema> fieldSchemaList = new ArrayList<>();
        for (ColumnInfo columnInfo : columnInfoList) {
            FieldSchema fs = new FieldSchema();
            fs.setName(columnInfo.getName());
            fs.setType(columnInfo.getTypeName());
            fieldSchemaList.add(fs);
        }
        return fieldSchemaList;
    }

    private Table getTable(TableInfo tableInfo) throws Exception {
        Table table = new Table();
        String[] fullName = tableInfo.getFullName().split("\\.");
        table.setDbName(fullName[1]);
        table.setTableName(fullName[2]);
        table.setParameters(tableInfo.getDeltaRuntimeProperties());
        table.setOwner(tableInfo.getOwner());
        table.setCreateTime((int) (tableInfo.getCreatedAt() / 1000));
        table.setTableType(getTableType(tableInfo.getTableType()));
        table.setPartitionKeys(new ArrayList<>());

        StorageDescriptor sd = new StorageDescriptor();
        table.setSd(sd);
        String location = tableInfo.getStorageLocation();
        location = location.replaceFirst("s3\\:", "s3a\\:");
        sd.setLocation(location);
        sd.setParameters(new HashMap<>());
        String[] ioformat = getInputOutputFormat(tableInfo.getDataSourceFormat());
        sd.setInputFormat(ioformat[0]);
        sd.setOutputFormat(ioformat[1]);
        sd.setNumBuckets(-1);
        for (FieldSchema fs : getFieldSchema(tableInfo)) {
            sd.addToCols(fs);
        }
        sd.setParameters(new HashMap<>());
        sd.setCompressed(false);

        SerDeInfo serDeInfo = new SerDeInfo();
        sd.setSerdeInfo(serDeInfo);
        serDeInfo.setSerializationLib(ioformat[2]);
        serDeInfo.setParameters(new HashMap<>());
        serDeInfo.putToParameters("serialization.format", "1");

        return table;
    }

    @Override
    public Table getTable(String dbName, String tableName) throws TException {
        try {
            TableInfo tableInfo = getTableInfo(dbName, tableName);
            return getTable(tableInfo);
        } catch (NoSuchObjectException ex1) {
            throw ex1;
        } catch (Exception ex) {
            throw convertToTException(ex);
        }
    }

    @Override
    public Partition createPartition(Table table, List<String> values) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public Partition addPartition(Partition partition) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void addPartitions(List<Partition> partitions) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<String> getPartitionNames(String dbName, String tableName, short maxSize) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean dropPartition(String dbName, String tableName, List<String> arguments) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> getPartitions(String dbName, String tableName, short maxSize) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public DropPartitionsResult dropPartitions(String dbName, String tableName, List<String> partNames) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName, List<String> names) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public boolean alterTable(String dbName, String tableName, Table newTable) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void alterPartition(String dbName, String tableName, Partition partition) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void alterPartitions(String dbName, String tableName, List<Partition> partitions) throws TException {
        throw new TException("method not implemented");
    }

    @Override
    public void appendPartition(String dbName, String tableName, List<String> partitionValues) throws TException {
        throw new TException("method not implemented");
    }
}
