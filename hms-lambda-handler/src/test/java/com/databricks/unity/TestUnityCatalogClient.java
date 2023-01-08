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

import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.junit.Assert.*;

public class TestUnityCatalogClient {
    private static final String endpoint = "https://e2-dogfood.staging.cloud.databricks.com/api/2.0/unity-catalog";
    private static final String token = "dummy_token";
    private static final String defaultCatalog = "test_vihang_catalog";
    private final UnityCatalogClient unityCatalogClient = new UnityCatalogClient(defaultCatalog, token, endpoint);

    @Test
    public void testGetSchemas() throws TException {
        Set<String> schemas = unityCatalogClient.getDatabaseNames("");
        assertTrue(schemas.size() == 6);
        assertTrue(schemas.contains("default"));
        assertTrue(schemas.contains("vihangdb_uc_db"));
    }

    @Test
    public void testGetDatabase() throws TException {
        Database database = unityCatalogClient.getDatabase("syncdb");
        assertDbIsValid(database, "syncdb");

        // get not existing database
        Exception ex = null;
        try {
            unityCatalogClient.getDatabase("does-not-exist");
        } catch (NoSuchObjectException e) {
            ex = e;
        }
        assertNotNull(ex);
        assertTrue(ex.getMessage().contains("does not exist"));
    }

    private void assertDbIsValid(Database database, String name) {
        assertNotNull(database);
        if (name != null) {
            assertEquals(name, database.getName());
        } else {
            assertNotNull(database.getName());
        }
        assertNotNull(database.getDescription());
        assertNotNull(database.getOwnerName());
    }

    @Test
    public void testGetDatabases() throws TException {
        List<Database> databases = unityCatalogClient.getDatabases("");
        assertNotNull(databases);
        assertFalse(databases.isEmpty());
        for (Database db : databases) {
            assertDbIsValid(db, null);
        }
    }

    private void assertTableIsValid(Table table, String fullName) {
        if (fullName != null) {
            String[] dbAndTbl = fullName.split("\\.");
            assertEquals(dbAndTbl[0], table.getDbName());
            assertEquals(dbAndTbl[1], table.getTableName());
        } else {
            assertNotNull(table.getDbName());
            assertNotNull(table.getTableName());
        }

        assertEquals("EXTERNAL_TABLE", table.getTableType());
        assertTrue(table.isSetCreateTime());
        assertTrue(table.isSetSd());
        assertSdIsValid(table.getSd());
    }

    private void assertSdIsValid(StorageDescriptor sd) {
        assertEquals("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", sd.getInputFormat());
        assertEquals("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat", sd.getOutputFormat());
        assertEquals("c1", sd.getCols().get(0).getName().toLowerCase(Locale.ROOT));
        assertEquals("int", sd.getCols().get(0).getType().toLowerCase(Locale.ROOT));
        assertSerdeInfoIsValid(sd.getSerdeInfo());
        assertNotNull(sd.getLocation());
        assertTrue(sd.getLocation().startsWith("s3a:"));
        assertFalse(sd.getLocation().isEmpty());
    }

    private void assertSerdeInfoIsValid(SerDeInfo serdeInfo) {
        assertEquals("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", serdeInfo.getSerializationLib());
    }

    @Test
    public void testGetTable() throws TException {
        Table table = unityCatalogClient.getTable("syncdb", "p_table1");
        assertTableIsValid(table, "syncdb.p_table1");

        // get not existing table
        Exception ex = null;
        try {
            unityCatalogClient.getTable("syncdb", "does-not-exist");
        } catch (NoSuchObjectException e) {
            ex = e;
        }
        assertNotNull(ex);
        assertTrue(ex.getMessage().contains("does not exist"));
    }

    @Test
    public void testGetTableNames() throws TException {
        Set<String> names = unityCatalogClient.getTableNames("syncdb", "");
        assertFalse(names.isEmpty());
        assertTrue(names.contains("p_table1"));
    }

    @Test
    public void testGetTables() throws TException {
       List<Table> tables = unityCatalogClient.getTablesByNames("syncdb", Arrays.asList("p_table1", "p_table2"));
        assertEquals(2, tables.size());
       for (Table t : tables) {
           assertTableIsValid(t, null);
       }
    }

    @Test
    public void testDbExists() throws TException {
        assertTrue(unityCatalogClient.dbExists("syncdb"));
        assertFalse(unityCatalogClient.dbExists("does-not-exist"));
    }

    @Test
    public void testTableExists() throws TException {
        assertTrue(unityCatalogClient.tableExists("syncdb", "p_table1"));
        assertFalse(unityCatalogClient.tableExists("syncdb", "does-not-exist"));
    }
}
