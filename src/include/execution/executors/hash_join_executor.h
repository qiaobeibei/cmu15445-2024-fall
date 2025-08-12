//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <unordered_map>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "common/util/hash_util.h"

namespace bustub {

struct HashJoinKey {
  std::vector<Value> keys_;

  explicit HashJoinKey(std::vector<Value> key) : keys_(std::move(key)) {}

  auto operator==(const HashJoinKey &other) const -> bool {
    if (keys_.size() != other.keys_.size()) {
      return false;
    }
    for (size_t i = 0; i < keys_.size(); ++i) {
      if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &key) const -> size_t {
    size_t hash_val = 0;
    for (const auto &val : key.keys_) {
      hash_val = bustub::HashUtil::CombineHashes(hash_val, bustub::HashUtil::HashValue(&val));
    }
    return hash_val;
  }
};
}  // namespace std

namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** keft child executor */
  std::unique_ptr<AbstractExecutor> left_executor_;

  /** right child executor */
  std::unique_ptr<AbstractExecutor> right_executor_;

  /** hash table for join */
  std::unordered_map<HashJoinKey, std::vector<Tuple>> hash_table_;

  /** current matches for the current right tuple */
  std::vector<Tuple> current_matches_;

  /** current index in matches */
  size_t current_match_idx_{0};

  /** current right tuple */
  Tuple current_right_tuple_{};

  /** cirremt right rid */
  RID current_right_rid_{};

  /** whether right tuple is available */
  bool right_tuple_available_{false};

  /** whether current right tuple has been matched */
  bool left_matched_{false};

  /** For LEFT JOIN: current left tuple */
  Tuple current_left_tuple_{};

  /** For LEFT JOIN: current left rid */
  RID current_left_rid_{};

  /** For LEFT JOIN: store all left tuples */
  std::vector<Tuple> left_tuples_;

  /** For LEFT JOIN: track which left tuples are matched */
  std::vector<bool> left_matched_flags_;

  /** For LEFT JOIN: current index in left tuples */
  size_t current_left_idx_{0};
};

}  // namespace bustub
