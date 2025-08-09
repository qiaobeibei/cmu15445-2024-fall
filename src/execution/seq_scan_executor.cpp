//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() {
  // 从 catalog 获取表信息
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info != nullptr, "Table not found");
  // 初始化 table iterator
  table_iter_ = std::make_unique<TableIterator>(table_info->table_->MakeEagerIterator());
}

/**
 * Yield the next tuple from the sequential scan.
 * @param[out] tuple The next tuple produced by the scan
 * @param[out] rid The next tuple RID produced by the scan
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 从 catalog 获取表信息
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info != nullptr, "Table not found");

  // 遍历表，直到找到有效的 tuple 或 end
  while (!table_iter_->IsEnd()) {
    auto [tuple_meta, table_tuple] = table_iter_->GetTuple();
    *rid = table_iter_->GetRID();

    // 移动到下一个 tuple
    ++(*table_iter_);
    // 跳过已删除的元组
    if (tuple_meta.is_deleted_) {
      continue;
    }

    // 是否需要过滤
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(&table_tuple, table_info->schema_);
      if (value.IsNull() || !value.GetAs<bool>()) {  // 过滤不满足条件的 tuple
        continue;
      }
    }

    // 对元组投影，以匹配输出 schema
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());

    for (uint32_t i = 0; i < GetOutputSchema().GetColumnCount(); ++i) {
      values.push_back(table_tuple.GetValue(&table_info->schema_, i));
    }

    *tuple = Tuple(values, &GetOutputSchema());
    return true;
  }

  return false;
}

}  // namespace bustub
