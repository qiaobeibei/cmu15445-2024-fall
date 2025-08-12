//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  // 构建阶段：从左表构建hash表
  hash_table_.clear();
  left_tuples_.clear();  // 存储所有左表元组，用于LEFT JOIN

  Tuple left_tuple{};
  RID left_rid{};

  while (left_executor_->Next(&left_tuple, &left_rid)) {
    // 计算左表元组的 hash key
    std::vector<Value> left_key_values;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      left_key_values.emplace_back(expr->Evaluate(&left_tuple, left_executor_->GetOutputSchema()));
    }
    // 构建 hash key 并将其与左表元组关联存储在哈希表中
    HashJoinKey left_key{left_key_values};
    hash_table_[left_key].emplace_back(left_tuple);

    // 对于LEFT JOIN，保存所有左表元组
    if (plan_->GetJoinType() == JoinType::LEFT) {
      left_tuples_.emplace_back(left_tuple);
    }
  }

  // 初始化探测阶段
  current_matches_.clear();
  current_match_idx_ = 0;
  right_tuple_available_ = false;
  left_matched_ = false;

  // 对于LEFT JOIN，需要记录哪些左表元组被匹配了
  if (plan_->GetJoinType() == JoinType::LEFT) {
    left_matched_flags_.clear();
    left_matched_flags_.resize(left_tuples_.size(), false);
    current_left_idx_ = 0;
  }
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join.
 * @param[out] rid The next tuple RID, not used by hash join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 对于LEFT JOIN，需要特殊处理
  if (plan_->GetJoinType() == JoinType::LEFT) {
    // 首先处理所有右表元组的匹配
    while (true) {
      // 若当前右表元组有未处理的左表匹配，优先返回这些匹配结果
      if (current_match_idx_ < current_matches_.size()) {
        // 获取当前要处理的左表元组
        const auto &left_tuple = current_matches_[current_match_idx_++];

        // 拼接左表和右表值
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(current_right_tuple_.GetValue(&right_executor_->GetOutputSchema(), i));
        }

        *tuple = Tuple{values, &GetOutputSchema()};
        *rid = RID{};
        return true;
      }

      // 获取下一个右表元组
      if (!right_tuple_available_) {
        right_tuple_available_ = right_executor_->Next(&current_right_tuple_, &current_right_rid_);
      }
      // 若右表已无更多元组，则处理未匹配的左表元组
      if (!right_tuple_available_) {
        break;
      }

      // 从当前右表元组中计算哈希键
      std::vector<Value> right_key_values;
      for (const auto &expr : plan_->RightJoinKeyExpressions()) {
        right_key_values.emplace_back(expr->Evaluate(&current_right_tuple_, right_executor_->GetOutputSchema()));
      }

      // 构造右表哈希键对象
      HashJoinKey right_key{right_key_values};

      // 探测哈希表，查找与右表连接键匹配的左表元组
      auto it = hash_table_.find(right_key);
      if (it != hash_table_.end()) {
        current_matches_ = it->second;
        current_match_idx_ = 0;
        left_matched_ = true;

        // 标记这些左表元组为已匹配
        for (const auto &matched_left_tuple : it->second) {
          for (size_t i = 0; i < left_tuples_.size(); i++) {
            if (matched_left_tuple.GetValue(&left_executor_->GetOutputSchema(), 0)
                    .CompareEquals(left_tuples_[i].GetValue(&left_executor_->GetOutputSchema(), 0)) ==
                CmpBool::CmpTrue) {
              left_matched_flags_[i] = true;
            }
          }
        }
      } else {
        current_matches_.clear();
        current_match_idx_ = 0;
        left_matched_ = false;
      }

      right_tuple_available_ = false;
    }

    // 处理未匹配的左表元组
    while (current_left_idx_ < left_tuples_.size()) {
      if (!left_matched_flags_[current_left_idx_]) {
        // 输出未匹配的左表元组，右表字段为NULL
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuples_[current_left_idx_].GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          auto &column = right_executor_->GetOutputSchema().GetColumn(i);
          values.emplace_back(ValueFactory::GetNullValueByType(column.GetType()));
        }

        *tuple = Tuple{values, &GetOutputSchema()};
        *rid = RID{};
        current_left_idx_++;
        return true;
      }
      current_left_idx_++;
    }
    return false;
  }

  // 对于INNER JOIN，从右表开始处理
  while (true) {
    // 若当前右表元组有未处理的左表匹配，优先返回这些匹配结果
    if (current_match_idx_ < current_matches_.size()) {
      // 获取当前要处理的左表元组
      const auto &left_tuple = current_matches_[current_match_idx_++];

      // 拼接左表和右表值
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(current_right_tuple_.GetValue(&right_executor_->GetOutputSchema(), i));
      }

      *tuple = Tuple{values, &GetOutputSchema()};
      *rid = RID{};
      return true;
    }

    // 获取下一个右表元组
    if (!right_tuple_available_) {
      right_tuple_available_ = right_executor_->Next(&current_right_tuple_, &current_right_rid_);
    }
    // 若右表已无更多元组，则结束
    if (!right_tuple_available_) {
      return false;
    }

    // 从当前右表元组中计算哈希键
    std::vector<Value> right_key_values;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      right_key_values.emplace_back(expr->Evaluate(&current_right_tuple_, right_executor_->GetOutputSchema()));
    }

    // 构造右表哈希键对象
    HashJoinKey right_key{right_key_values};

    // 探测哈希表，查找与右表连接键匹配的左表元组
    auto it = hash_table_.find(right_key);
    if (it != hash_table_.end()) {
      current_matches_ = it->second;
      current_match_idx_ = 0;
      left_matched_ = true;
    } else {
      current_matches_.clear();
      current_match_idx_ = 0;
      left_matched_ = false;
    }

    right_tuple_available_ = false;
  }
}

}  // namespace bustub
