//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  BUSTUB_ASSERT(tree_ != nullptr, "Index is not a B+ tree index");
}

void IndexScanExecutor::Init() {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  BUSTUB_ASSERT(index_info != nullptr, "Index not found");

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  BUSTUB_ASSERT(table_info != nullptr, "Table not found");

  // 若存在谓词键，使用它们进行点查询
  if (!plan_->pred_keys_.empty()) {
    use_iterator_ = false;
    // 计算谓词键获取搜索键
    std::vector<Value> key_values;
    for (const auto &expr : plan_->pred_keys_) {
      // 常量表达式可使用空元组计算
      key_values.push_back(expr->Evaluate(nullptr, index_info->key_schema_));
    }

    Tuple search_key{key_values, &index_info->key_schema_};

    // 扫描匹配的RID
    index_info->index_->ScanKey(search_key, &result_rids_, exec_ctx_->GetTransaction());
  } else {
    // 对于全索引扫描，B+树
    use_iterator_ = true;
    it_ = tree_->GetBeginIterator();
    end_it_ = tree_->GetEndIterator();
    // 使用迭代器不需要result_rids
    result_rids_.clear();
  }

  current_idx_ = 0;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  BUSTUB_ASSERT(table_info != nullptr, "Table not found");

  if (use_iterator_) {
    while (!it_.IsEnd()) {
      auto [key, value] = *it_;
      RID current_rid = value;
      ++it_;

      auto [tuple_meta, table_tuple] = table_info->table_->GetTuple(current_rid);
      if (tuple_meta.is_deleted_) {
        continue;
      }

      // 过滤
      if (plan_->filter_predicate_ != nullptr) {
        auto filter_value = plan_->filter_predicate_->Evaluate(&table_tuple, table_info->schema_);
        if (filter_value.IsNull() || !filter_value.GetAs<bool>()) {
          continue;
        }
      }

      // 对元组投影，使其匹配输出 schema（只保留需要的字段）
      std::vector<Value> values;
      values.reserve(GetOutputSchema().GetColumnCount());
      // 遍历输出schema的每个字段，从原始元组中提取对应的值
      for (uint32_t i = 0; i < GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(table_tuple.GetValue(&table_info->schema_, i));
      }

      *tuple = Tuple{values, &GetOutputSchema()};
      *rid = current_rid;
      return true;
    }

  } else {
    while (current_idx_ < result_rids_.size()) {
      auto current_rid = result_rids_[current_idx_++];
      // 使用索引返回的RID从表中获取元组
      auto [tuple_meta, table_tuple] = table_info->table_->GetTuple(current_rid);

      if (tuple_meta.is_deleted_) {
        continue;
      }

      // 过滤
      if (plan_->filter_predicate_ != nullptr) {
        auto value = plan_->filter_predicate_->Evaluate(&table_tuple, table_info->schema_);
        if (value.IsNull() || !value.GetAs<bool>()) {
          continue;
        }
      }

      // 对元组投影，使其匹配输出 schema（只保留需要的字段）
      std::vector<Value> values;
      values.reserve(GetOutputSchema().GetColumnCount());
      // 遍历输出schema的每个字段，从原始元组中提取对应的值
      for (uint32_t i = 0; i < GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(table_tuple.GetValue(&table_info->schema_, i));
      }

      *tuple = Tuple{values, &GetOutputSchema()};
      *rid = current_rid;
      return true;
    }
  }
  return false;
}
}  // namespace bustub
