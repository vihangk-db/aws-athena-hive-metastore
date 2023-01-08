/*-
 * #%L
 * hms-lambda-handler
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.hms.handler;

import com.amazonaws.athena.hms.GetTableRequest;
import com.amazonaws.athena.hms.GetTableResponse;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TSerializer;

import java.nio.charset.StandardCharsets;

public class GetTableHandler extends BaseHMSHandler<GetTableRequest, GetTableResponse>
{
  public GetTableHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public GetTableResponse handleRequest(GetTableRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to Unity Catalog");
      HiveMetaStoreClient client = getUnityClient();
      context.getLogger().log("Fetching table " + request.getTableName() + " in DB: " + request.getDbName());
      Table table = client.getTable(request.getDbName(), request.getTableName());
      context.getLogger().log("Fetched Unity Catalog table: " + request.getTableName());
      GetTableResponse response = new GetTableResponse();
      if (table != null) {
        TSerializer serializer = new TSerializer(getTProtocolFactory());
        String tblStr = serializer.toString(table, StandardCharsets.UTF_8.name());
        context.getLogger().log("Unity Catalog Table JSON: " + tblStr);
        response.setTableDesc(tblStr);
      }
      /*

      HiveMetaStoreClient hmsClient = getClient();
      context.getLogger().log("Fetch HMS table " + request.getTableName() + "in db: " + request.getDbName());
      Table hmsTable = hmsClient.getTable("testhivedb", "hivetbl1");
      if (hmsTable != null) {
        TSerializer serializer = new TSerializer(getTProtocolFactory());
        String tblStr = serializer.toString(hmsTable, StandardCharsets.UTF_8.name());
        context.getLogger().log("HMS Table JSON: " + tblStr);
        context.getLogger().log("Unity Catalog JSON: " +serializer.toString(table, StandardCharsets.UTF_8.name()));
        context.getLogger().log("Modifying HMS table");
        hmsTable.setTableName(request.getTableName());
        hmsTable.setDbName(request.getDbName());
        // parameters from UC works
        // context.getLogger().log("Modifying HMS table: Changing parameters from " + hmsTable.getParameters() + " to " + table.getParameters());
        // hmsTable.setParameters(table.getParameters());
        hmsTable.setParameters(table.getParameters());
        // Sd from unity catalog works
        context.getLogger().log("Modifying HMS table: Changing SD from " +
                serializer.toString(hmsTable.getSd(), StandardCharsets.UTF_8.name()) + " to " +
                serializer.toString(table.getSd(), StandardCharsets.UTF_8.name()));
        StorageDescriptor prevSd = hmsTable.getSd();
        hmsTable.setSd(table.getSd());
        // replace location to avoid Permission denied error
        hmsTable.getSd().setLocation(prevSd.getLocation());

        // table type
        context.getLogger().log("Modifying the HMS table: Changing table type from " +
                hmsTable.getTableType() + " to " + table.getTableType()
        );
        hmsTable.setTableType(table.getTableType());

        // table owner
        context.getLogger().log("Modifying the HMS table: Changing table owner from " +
                hmsTable.getOwner() + " to " + table.getOwner()
        );
        hmsTable.setOwner(table.getOwner());

        tblStr = serializer.toString(hmsTable, StandardCharsets.UTF_8.name());
        context.getLogger().log("New HMS Table JSON: "+ tblStr);
        // send UC table
        tblStr = serializer.toString(table, StandardCharsets.UTF_8.name());
        context.getLogger().log("Sending Unity Catalog table JSON: "+ tblStr);
        response.setTableDesc(tblStr);
      }
       */
      return response;
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
