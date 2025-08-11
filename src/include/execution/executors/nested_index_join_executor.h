//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedIndexJoinExecutor executes index join operations.
 */
class NestedIndexJoinExecutor : public AbstractExecutor {
 public:
  NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                          std::unique_ptr<AbstractExecutor> &&child_executor);

  /** @return The output schema for the nested index join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;
  /** the child executor */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** current outer tuple */
  Tuple outer_tuple_{};
  /** current outer rid */
  RID outer_rid{};
  /** whether have a valid outer tuple */
  bool outer_tuple_available_{false};
  /** current inner table matches for the current outer tuple */
  std::vector<RID> inner_rids_;
  /** current index in the inner matches */
  size_t inner_match_idx_{0};
  /** whether the current outer tuple has been matched*/
  bool outer_matched_{false};
};
}  // namespace bustub
