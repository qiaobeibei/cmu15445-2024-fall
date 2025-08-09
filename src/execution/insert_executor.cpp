//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the insert */
void InsertExecutor::Init() {
  child_executor_->Init();
  inserted_count_ = 0;
  finished_ = false;
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
 * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
 */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info != nullptr, "Table not found");

  // 获得该表全部索引
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  Tuple child_tuple{};
  RID child_rid{};

  // 插入子执行器提供的所有元组
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // 创建表元数据
    TupleMeta tuple_meta{0, false};

    // 插入元组
    auto insert_rid = table_info->table_->InsertTuple(tuple_meta, child_tuple, exec_ctx_->GetLockManager(),
                                                      exec_ctx_->GetTransaction(), plan_->GetTableOid());

    if (insert_rid.has_value()) {
      inserted_count_++;

      // 更新所有索引
      for (auto &index_info : indexes) {
        // 从元组中生成索引键
        auto key_tuple =
            child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(key_tuple, insert_rid.value(), exec_ctx_->GetTransaction());
      }
    }
  }

  // 返回插入的行数
  std::vector<Value> values;
  values.emplace_back(Value(TypeId::INTEGER, inserted_count_));
  *tuple = Tuple{values, &GetOutputSchema()};

  finished_ = true;
  return true;
}

}  // namespace bustub
