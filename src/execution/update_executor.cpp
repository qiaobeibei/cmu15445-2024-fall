//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // 从Catalog获取表信息
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info_ != nullptr, "Table not found");
}

/** Initialize the update */
void UpdateExecutor::Init() {
  child_executor_->Init();
  updated_count_ = 0;
  finished_ = false;
}

/**
 * Yield the next tuple from the update.
 * @param[out] tuple The next tuple produced by the update
 * @param[out] rid The next tuple RID produced by the update (ignore this)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
 */
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }

  // 获取该表的全部索引
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  Tuple child_tuple{};
  RID child_rid{};

  // 更新子执行器提供的所有元组
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // 获取旧元组
    auto [old_meta, old_tuple] = table_info_->table_->GetTuple(child_rid);

    // 该元组是否被标删
    if (old_meta.is_deleted_) {
      continue;
    }

    // 计算新值
    std::vector<Value> new_values{};
    new_values.reserve(plan_->target_expressions_.size());

    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }

    // 创建新元组
    Tuple new_tuple{new_values, &table_info_->schema_};
    // 更新表,时间戳0，未标删
    TupleMeta new_meta{0, false};

    auto new_rid = table_info_->table_->InsertTuple(new_meta, new_tuple, exec_ctx_->GetLockManager(),
                                                    exec_ctx_->GetTransaction(), plan_->GetTableOid());

    if (new_rid.has_value()) {
      // 将旧元组标删
      TupleMeta delete_meta{0, true};
      table_info_->table_->UpdateTupleMeta(delete_meta, child_rid);
      ++updated_count_;

      // 更新全部索引
      for (auto &index_info : indexes) {
        // 从旧元组中提取索引键
        auto old_key =
            old_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        // 移除旧索引条目
        index_info->index_->DeleteEntry(old_key, child_rid, exec_ctx_->GetTransaction());

        // 从新元组中提取索引键
        auto new_key =
            new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        // 插入新索引条目
        index_info->index_->InsertEntry(new_key, new_rid.value(), exec_ctx_->GetTransaction());
      }
    }
  }

  // 返回更新行数
  std::vector<Value> values;
  values.emplace_back(Value(TypeId::INTEGER, updated_count_));
  *tuple = Tuple{values, &GetOutputSchema()};
  finished_ = true;
  return true;
}

}  // namespace bustub
