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

package org.apache.doris.datasource.mapping;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class CacheIdentifierMapping implements IdentifierMapping {
    private final LoadingCache<String, Mapping> remoteSchemaNames;
    private final LoadingCache<String, Mapping> remoteTableNames;
    private final IdentifierMapping identifierMapping;

    public CacheIdentifierMapping(IdentifierMapping identifierMapping) {
        this.identifierMapping = Objects.requireNonNull(identifierMapping, "identifierMapping is null");
        this.remoteSchemaNames = identifierMapping.getRemoteSchemaNames();
        this.remoteTableNames = identifierMapping.getRemoteTableNames();
    }

    public void flushCache() {
        this.remoteSchemaNames.invalidateAll();
        this.remoteTableNames.invalidateAll();
    }

    @Override
    public String fromRemoteSchemaName(String remoteSchemaName) {
        return this.identifierMapping.fromRemoteSchemaName(remoteSchemaName);
    }

    @Override
    public String fromRemoteTableName(String remoteSchemaName, String remoteTableName) {
        return this.identifierMapping.fromRemoteTableName(remoteSchemaName, remoteTableName);
    }

    @Override
    public String fromRemoteColumnName(String remoteColumnName) {
        return this.identifierMapping.fromRemoteColumnName(remoteColumnName);
    }

    @Override
    public String toRemoteSchemaName(RemoteIdentifiers remoteIdentifiers, String schemaName) {
        return "";
    }

    @Override
    public String toRemoteTableName(RemoteIdentifiers remoteIdentifiers, String remoteSchema, String tableName) {
        return "";
    }

    @Override
    public String toRemoteColumnName(RemoteIdentifiers remoteIdentifiers, String columnName) {
        return "";
    }

    private static final class Mapping {
        private final Map<String, String> mapping;
        private final Set<String> duplicates;

        public Mapping(Map<String, String> mapping, Set<String> duplicates) {
            this.mapping = ImmutableMap.copyOf(Objects.requireNonNull(mapping, "mapping is null"));
            this.duplicates = ImmutableSet.copyOf(Objects.requireNonNull(duplicates, "duplicates is null"));
        }

        public boolean hasRemoteObject(String remoteName) {
            return mapping.containsKey(remoteName) || duplicates.contains(remoteName);
        }

        @Nullable
        public String get(String remoteName) {
            Preconditions.checkArgument(!duplicates.contains(remoteName), "Ambiguous name: %s", remoteName);
            return mapping.get(remoteName);
        }
    }
}