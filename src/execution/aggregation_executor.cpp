//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

/** Initialize the aggregation */
void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();

  // 处理子执行器输出的元组
  Tuple child_tuple{};
  RID child_rid{};

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto agg_key = MakeAggregateKey(&child_tuple);
    auto agg_val = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(agg_key, agg_val);
  }

  // 只有当哈希表不为空时才设置迭代器
  if (aht_.Begin() != aht_.End()) {
    aht_iterator_ = aht_.Begin();
    aht_iterator_valid_ = true;
  } else {
    aht_iterator_valid_ = false;
  }
  initial_value_output_ = false;
}

/**
 * Yield the next tuple from the insert.
 * @param[out] tuple The next tuple produced by the aggregation
 * @param[out] rid The next tuple RID produced by the aggregation
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 处理无 GROUP BY 子句且聚合结果为空（输入数据为空）
  if (plan_->GetGroupBys().empty() && aht_.Begin() == aht_.End()) {
    // 检查是否已经输出了初始值
    if (initial_value_output_) {
      return false;
    }

    // 返回初始聚类值（COUNT 为0，MAX等为NULL）
    auto initial_val = aht_.GenerateInitialAggregateValue();
    std::vector<Value> values;
    // 添加 GROUP BY 字段值
    for (const auto &group_by_expr : plan_->GetGroupBys()) {
      (void)group_by_expr;
    }

    // Add aggregate values
    for (const auto &agg_val : initial_val.aggregates_) {
      values.push_back(agg_val);
    }

    *tuple = Tuple{values, &GetOutputSchema()};
    *rid = RID{};  // 聚合结果无实际RID，赋空值

    initial_value_output_ = true;
    return true;
  }

  // 如果有GROUP BY子句或者有聚合结果，使用迭代器
  // 但是要确保迭代器是有效的
  if (aht_.Begin() == aht_.End()) {
    return false;
  }

  // 确保迭代器是有效的
  if (!aht_iterator_valid_) {
    return false;
  }

  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  // 获取当前聚合结果
  auto agg_key = aht_iterator_.Key();
  auto agg_val = aht_iterator_.Val();

  std::vector<Value> values;

  // 添加 GROUP BY 字段值
  for (const auto &group_by_val : agg_key.group_bys_) {
    values.push_back(group_by_val);
  }

  // 添加聚合值
  for (const auto &agg_val_item : agg_val.aggregates_) {
    values.push_back(agg_val_item);
  }

  *tuple = Tuple{values, &GetOutputSchema()};
  *rid = RID{};  // 聚合结果无实际RID，赋空值

  ++aht_iterator_;
  return true;
}

/** Do not use or remove this function, otherwise you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
