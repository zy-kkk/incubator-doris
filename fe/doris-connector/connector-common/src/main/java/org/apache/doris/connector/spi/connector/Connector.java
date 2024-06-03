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

package org.apache.doris.connector.spi.connector;

import org.apache.doris.connector.spi.metadata.ConnectorMetadata;

import com.google.common.collect.Maps;

import java.util.Map;

public interface Connector {

    default ConnectorTransactionHandle beginTransaction(boolean readOnly, boolean autoCommit) {
        throw new UnsupportedOperationException();
    }

    default ConnectorMetadata getMetadata(ConnectorContext context) {
        throw new UnsupportedOperationException();
    }

    default ConnectorSplitManager getSplitManager() {
        throw new UnsupportedOperationException();
    }

    default ConnectorVectorSourceProvider getVectorSourceProvider() {
        throw new UnsupportedOperationException();
    }

    default ConnectorVectorSinkProvider getVectorSinkProvider() {
        throw new UnsupportedOperationException();
    }

    default Map<String, String> getCatalogProperties() {
        return Maps.newHashMap();
    }

    default Map<String, String> getDatabaseProperties() {
        return Maps.newHashMap();
    }

    default Map<String, String> getTableProperties() {
        return Maps.newHashMap();
    }

    default Map<String, String> getColumnProperties() {
        return Maps.newHashMap();
    }

    default void commit(ConnectorTransactionHandle transactionHandle) {
        throw new UnsupportedOperationException();
    }

    default void rollback(ConnectorTransactionHandle transactionHandle) {
        throw new UnsupportedOperationException();
    }

    default boolean isSingleStatementWritesOnly() {
        return true;
    }

    default void shutdown() {
    }
}
