//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedIndexJoinExecutor::Init() { 
  child_executor_->Init();

  // 获取第一个外表元组
  outer_tuple_available_ = child_executor_->Next(&outer_tuple_, &outer_rid);
  outer_matched_ = false;
  inner_rids_.clear();
  inner_match_idx_ = 0;

  if (outer_tuple_available_) {
    // 获取索引信息
    auto catalog = exec_ctx_->GetCatalog();
    auto index_info = catalog->GetIndex(plan_->GetIndexOid());

    // 从外表元组中提取索引键
    auto key_value = plan_->KeyPredicate()->Evaluate(&outer_tuple_, child_executor_->GetOutputSchema());

    std::vector<Value> key_values{key_value};
    Tuple index_key{key_values, &index_info->key_schema_};

    // 利用索引查找所有匹配该键的内表元组的RID
    index_info->index_->ScanKey(index_key, &inner_rids_, exec_ctx_->GetTransaction());
  }
 }

auto NestedIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  auto catalog = exec_ctx_->GetCatalog();
  auto inner_table_info = catalog->GetTable(plan_->GetInnerTableOid());
  auto index_info = catalog->GetIndex(plan_->GetIndexOid());

  while (outer_tuple_available_) {
    // 当前外表元组未匹配，且有内表匹配项
    if (inner_match_idx_ < inner_rids_.size()) {
      // 获取当前匹配的内表元组
      auto inner_rid = inner_rids_[inner_match_idx_++];
      auto [tuple_meta, inner_tuple] = inner_table_info->table_->GetTuple(inner_rid);
      if (tuple_meta.is_deleted_) {
        continue;
      }

      outer_matched_ = true;

      // 拼接外表和内表的字段
      std::vector<Value> values;
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(outer_tuple_.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        values.push_back(inner_tuple.GetValue(&plan_->InnerTableSchema(), i));
      }

      *tuple = Tuple{values, &GetOutputSchema()};
      *rid = RID{};

      return true;
    }

    // 处理左外连接，当前外层元组未找到匹配的内表元组
    if (plan_->GetJoinType() == JoinType::LEFT && !outer_matched_) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(outer_tuple_.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        // 内表无匹配，用NULL填充
        auto& column = plan_->InnerTableSchema().GetColumn(i);
        values.push_back(ValueFactory::GetNullValueByType(column.GetType()));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      *rid = RID{};

      // 准备处理下一个外表元组
      outer_tuple_available_ = child_executor_->Next(&outer_tuple_, &outer_rid);
      outer_matched_ = false;
      inner_rids_.clear();
      inner_match_idx_= 0;

      return true;
    }

    // 处理完当前外层元组的所有匹配项，准备处理下一个外表元组
    outer_tuple_available_ = child_executor_->Next(&outer_tuple_, &outer_rid);
    if (!outer_tuple_available_) {
      break;
    }

    outer_matched_ = false;
    inner_rids_.clear();
    inner_match_idx_= 0;

    // 从新外层元组中提取索引key
    auto key_value = plan_->KeyPredicate()->Evaluate(&outer_tuple_, child_executor_->GetOutputSchema());
    std::vector<Value> key_values{key_value};
    Tuple index_key{key_values, &index_info->key_schema_};

    // 利用索引查找所有匹配该键的内表元组的RID
    index_info->index_->ScanKey(index_key, &inner_rids_, exec_ctx_->GetTransaction());
  }

  // 没有更多的外表元组
  return false;
 }

}  // namespace bustub
