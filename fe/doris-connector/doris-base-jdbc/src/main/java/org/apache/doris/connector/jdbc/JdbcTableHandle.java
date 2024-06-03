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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.spi.metadata.handle.TableHandle;

import java.util.List;
import java.util.Optional;

public final class JdbcTableHandle implements TableHandle {
    private final String databaseName;
    private final String tableName;
    private final Optional<List<JdbcColumnHandle>> columns;

    public JdbcTableHandle(String databaseName, String tableName, Optional<List<JdbcColumnHandle>> columns) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public Optional<List<JdbcColumnHandle>> getColumns() {
        return columns;
    }
}
