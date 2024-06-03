// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// copied from https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/server/PluginManager.java

package org.apache.doris.connector.spi.metadata;

import org.apache.doris.connector.spi.connector.ConnectorContext;
import org.apache.doris.connector.spi.metadata.handle.ColumnHandle;
import org.apache.doris.connector.spi.metadata.handle.TableHandle;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public interface ConnectorMetadata {
    default List<String> listDatabaseNames(ConnectorContext context) {
        return Collections.emptyList();
    }

    @Nullable
    default TableHandle getTableHandle(ConnectorContext context, String databaseName, String tableName) {
        return null;
    }

    default List<String> listTableNames(ConnectorContext context, String databaseName) {
        return Collections.emptyList();
    }

    default boolean tableExists(ConnectorContext context, String databaseName, String tableName) {
        return false;
    }

    default Map<String, ColumnHandle> getColumnHandles(ConnectorContext context, TableHandle tableHandle) {
        return Collections.emptyMap();
    }
}
