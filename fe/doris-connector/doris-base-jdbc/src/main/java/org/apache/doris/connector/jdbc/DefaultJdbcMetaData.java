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

import org.apache.doris.connector.spi.connector.ConnectorContext;
import org.apache.doris.connector.spi.metadata.handle.ColumnHandle;
import org.apache.doris.connector.spi.metadata.handle.TableHandle;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class DefaultJdbcMetaData implements JdbcMetadata {

    private final JdbcClient jdbcClient;

    public DefaultJdbcMetaData(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public List<String> listDatabaseNames(ConnectorContext context) {
        return ImmutableList.copyOf(jdbcClient.getDatabaseNames(context));
    }

    @Nullable
    @Override
    public TableHandle getTableHandle(ConnectorContext context, String databaseName, String tableName) {
        return jdbcClient.getTableHandle(context, Optional.of(databaseName), tableName).orElse(null);
    }

    @Override
    public List<String> listTableNames(ConnectorContext context, String databaseName) {
        return jdbcClient.getTableNames(context, Optional.of(databaseName));
    }

    @Override
    public boolean tableExists(ConnectorContext context, String databaseName, String tableName) {
        return jdbcClient.getTableHandle(context, Optional.of(databaseName), tableName).isPresent();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorContext context, TableHandle tableHandle) {
        List<JdbcColumnHandle> columns = jdbcClient.getColumns(context, (JdbcTableHandle) tableHandle);
        return columns.stream().collect(Collectors.toMap(JdbcColumnHandle::getColumnName, Function.identity()));
    }
}
