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

import java.util.Locale;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnInfo {
    private String name;

    @JsonProperty("type_name")
    private String typeName;

    private boolean nullable;

    private int position;

    @JsonProperty("type_precision")
    private int typePrecision;

    @JsonProperty("type_scale")
    private int typeScale;

    public ColumnInfo() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName.toLowerCase(Locale.ROOT);
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getTypePrecision() {
        return typePrecision;
    }

    public void setTypePrecision(int typePrecision) {
        this.typePrecision = typePrecision;
    }

    public int getTypeScale() {
        return typeScale;
    }

    public void setTypeScale(int typeScale) {
        this.typeScale = typeScale;
    }
}
