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

#include <fmt/core.h>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_array.h"
#include "vec/exprs/lambda_function/lambda_function.h"
#include "vec/exprs/lambda_function/lambda_function_factory.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

template <bool reverse>
class ArraySplitFunction : public LambdaFunction {
public:
    ~ArraySplitFunction() override = default;

    static constexpr const char* name = reverse ? "array_revert_split" : "array_split";

    static LambdaFunctionPtr create() { return std::make_shared<ArraySplitFunction>(); }

    std::string get_name() const override { return name; }

    doris::Status execute(VExprContext* context, doris::vectorized::Block* block,
                          int* result_column_id, DataTypePtr result_type,
                          const std::vector<VExpr*>& children) override {
        // 获取子表达式的列 ID
        doris::vectorized::ColumnNumbers arguments(children.size());
        for (int i = 0; i < children.size(); ++i) {
            int column_id = -1;
            RETURN_IF_ERROR(children[i]->execute(context, block, &column_id));
            arguments[i] = column_id;
        }

        // 获取 arr1 和 arr2 的列
        auto arr1_column =
                block->get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto arr2_column =
                block->get_by_position(arguments[1]).column->convert_to_full_column_if_const();

        int input_rows = arr1_column->size();

        // 初始化 arr1 的空值映射
        auto arr1_outside_null_map = ColumnUInt8::create(input_rows, 0);
        auto arr1_arg_column = arr1_column;
        if (arr1_arg_column->is_nullable()) {
            arr1_arg_column =
                    assert_cast<const ColumnNullable*>(arr1_column.get())->get_nested_column_ptr();
            const auto& column_array_nullmap =
                    assert_cast<const ColumnNullable*>(arr1_column.get())->get_null_map_column();
            VectorizedUtils::update_null_map(arr1_outside_null_map->get_data(),
                                             column_array_nullmap.get_data());
        }

        // 获取 arr1 的嵌套列
        const ColumnArray& arr1_col_array = assert_cast<const ColumnArray&>(*arr1_arg_column);
        const auto& arr1_off_data =
                assert_cast<const ColumnArray::ColumnOffsets&>(arr1_col_array.get_offsets_column())
                        .get_data();
        const auto& arr1_nested_column = *arr1_col_array.get_data_ptr();

        // 初始化 arr2 的空值映射
        auto arr2_outside_null_map = ColumnUInt8::create(input_rows, 0);
        auto arr2_arg_column = arr2_column;
        if (arr2_arg_column->is_nullable()) {
            arr2_arg_column =
                    assert_cast<const ColumnNullable*>(arr2_column.get())->get_nested_column_ptr();
            const auto& column_array_nullmap =
                    assert_cast<const ColumnNullable*>(arr2_column.get())->get_null_map_column();
            VectorizedUtils::update_null_map(arr2_outside_null_map->get_data(),
                                             column_array_nullmap.get_data());
        }

        // 获取 arr2 的嵌套列
        const ColumnArray& arr2_col_array = assert_cast<const ColumnArray&>(*arr2_arg_column);
        const auto& arr2_off_data =
                assert_cast<const ColumnArray::ColumnOffsets&>(arr2_col_array.get_offsets_column())
                        .get_data();
        const auto& arr2_nested_column = *arr2_col_array.get_data_ptr();

        // 创建结果数据列和结果偏移列
        auto result_data_column = arr1_nested_column.clone_empty();
        auto result_offset_column = ColumnArray::ColumnOffsets::create();
        auto& result_offset_data = result_offset_column->get_data();
        result_offset_data.reserve(input_rows + 1);
        result_offset_data.push_back(0);

        // 遍历所有行，根据 arr2 的值进行拆分
        for (int row = 0; row < input_rows; ++row) {
            size_t prev_offset = result_offset_data.back();
            size_t curr_offset = prev_offset;

            // 当前行的 arr1 和 arr2 的起始和结束偏移
            size_t arr1_offset_start = (row == 0) ? 0 : arr1_off_data[row - 1];
            size_t arr1_offset_end = arr1_off_data[row];
            size_t arr2_offset_start = (row == 0) ? 0 : arr2_off_data[row - 1];
            size_t arr2_offset_end = arr2_off_data[row];

            // 遍历当前行的 arr1 和 arr2
            for (size_t off1 = arr1_offset_start, off2 = arr2_offset_start;
                 off1 < arr1_offset_end && off2 < arr2_offset_end; ++off1, ++off2) {
                // 当前元素需要拆分
                bool should_split = false;

                if (reverse) {
                    if (off2 > arr2_offset_start) {
                        should_split = assert_cast<const ColumnUInt8&>(arr2_nested_column)
                                               .get_data()[off2 - 1] != 0;
                    }
                } else {
                    if (off2 < arr2_offset_end - 1) {
                        should_split = assert_cast<const ColumnUInt8&>(arr2_nested_column)
                                               .get_data()[off2 + 1] != 0;
                    }
                }

                // 在结果数据列中插入当前元素
                result_data_column->insert_from(*arr1_nested_column.get_ptr(), off1);

                // 更新结果偏移列
                if (should_split) {
                    result_offset_data.push_back(++curr_offset);
                } else {
                    ++curr_offset;
                }
            }

            // 在最后一个元素后插入偏移（仅对 array_split_revert 有效）
            if (reverse && row == input_rows - 1) {
                result_offset_data.push_back(curr_offset);
            }
        }

        // 将结果列插入 block
        ColumnWithTypeAndName result_arr;
        if (result_type->is_nullable()) {
            result_arr = {
                    ColumnNullable::create(ColumnArray::create(std::move(result_data_column),
                                                               std::move(result_offset_column)),
                                           std::move(arr1_outside_null_map)),
                    result_type, fmt::format("{}_result", name)};
        } else {
            DCHECK(!arr1_column->is_nullable());
            DCHECK(!arr2_column->is_nullable());
            result_arr = {ColumnArray::create(std::move(result_data_column),
                                              std::move(result_offset_column)),
                          result_type, fmt::format("{}_result", name)};
        }
        block->insert(std::move(result_arr));
        *result_column_id = block->columns() - 1;

        return Status::OK();
    }
};

void register_function_array_split(doris::vectorized::LambdaFunctionFactory& factory) {
    factory.register_function("array_split", &ArraySplitFunction<false>::create);
    factory.register_function("array_revert_split", &ArraySplitFunction<true>::create);
}
} // namespace doris::vectorized
