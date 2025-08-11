//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() { 
  left_executor_->Init();
  right_executor_->Init();

  // 获取第一个左表元组
  left_tuple_available_ = left_executor_->Next(&left_tuple_, &left_rid_);
  left_matched_ = false;
 }

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join
 * @param[out] rid The next tuple RID produced, not used by nested loop join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  while (left_tuple_available_) {
    Tuple right_tuple{};
    RID right_rid{};

    // Try to get next right tuple
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      // 检查是否匹配
      bool join_condition = true;
      if (plan_->Predicate() != nullptr) {
        auto value = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), 
                                                      &right_tuple, right_executor_->GetOutputSchema());
        join_condition = !value.IsNull() && value.GetAs<bool>();
      }

      if (join_condition) {
        left_matched_ = true;

        // 拼接左表和右表元组的值
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }

        *tuple = Tuple(values, &GetOutputSchema());
        *rid = RID{};
        return true;
      }
    }

    // 处理左外连接，当前左表未找到匹配的右表元组
    if (plan_->GetJoinType() == JoinType::LEFT && !left_matched_) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      // 右表无匹配，用NULL填充
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        auto &column = right_executor_->GetOutputSchema().GetColumn(i);
        values.emplace_back(ValueFactory::GetNullValueByType(column.GetType()));
      }

      *tuple = Tuple(values, &GetOutputSchema());
      *rid = RID{};

      // 获取下一个左表元组
      left_tuple_available_ = left_executor_->Next(&left_tuple_, &left_rid_);
      left_matched_ = false;
      right_executor_->Init(); // 重置右表执行器,下次从右表开头迭代

      return true;
    }

    // 获取下一个左表元组
    left_tuple_available_ = left_executor_->Next(&left_tuple_, &left_rid_);
    left_matched_ = false;
    right_executor_->Init(); // 重置右表执行器,下次从右表开头迭代
  }

  return false;
 }

}  // namespace bustub
