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
public class ListSchemas {
    public ListSchemas() {
    }

    @JsonProperty("schemas")
    private SchemaInfo[] schemas;

    public SchemaInfo[] getSchemas() {
        return schemas;
    }

    public void setSchemas(SchemaInfo[] schemas) {
        this.schemas = schemas;
    }

    public List<SchemaInfo> getSchemaInfos() {
        List<SchemaInfo> schemaInfoList = new ArrayList<>();
        if (schemas == null) return schemaInfoList;
        for (SchemaInfo s : schemas) {
            schemaInfoList.add(s);
        }
        return schemaInfoList;
    }
}
