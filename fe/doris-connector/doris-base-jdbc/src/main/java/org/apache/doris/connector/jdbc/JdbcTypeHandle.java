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

import java.util.Optional;

public class JdbcTypeHandle {
    private final int jdbcType;
    private final Optional<String> jdbcTypeName;
    private final Optional<Integer> columnSize;
    private final Optional<Integer> decimalDigits;
    private final Optional<Integer> arrayDimensions;
    private final Optional<CaseSensitivity> caseSensitivity;

    public JdbcTypeHandle(int jdbcType, Optional<String> jdbcTypeName, Optional<Integer> columnSize,
            Optional<Integer> decimalDigits, Optional<Integer> arrayDimensions,
            Optional<CaseSensitivity> caseSensitivity) {
        this.jdbcType = jdbcType;
        this.jdbcTypeName = jdbcTypeName;
        this.columnSize = columnSize;
        this.decimalDigits = decimalDigits;
        this.arrayDimensions = arrayDimensions;
        this.caseSensitivity = caseSensitivity;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public Optional<String> getJdbcTypeName() {
        return jdbcTypeName;
    }

    public Optional<Integer> getColumnSize() {
        return columnSize;
    }

    public Optional<Integer> getDecimalDigits() {
        return decimalDigits;
    }

    public Optional<Integer> getArrayDimensions() {
        return arrayDimensions;
    }

    public Optional<CaseSensitivity> getCaseSensitivity() {
        return caseSensitivity;
    }
}
