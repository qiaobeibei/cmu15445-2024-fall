//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

/** 检查表达式是否为等值比较 */
auto IsEquiComparison(const AbstractExpressionRef &expr) -> bool {
  const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  return comp_expr != nullptr && comp_expr->comp_type_ == ComparisonType::Equal;
}

/** 检查表达式是否为列值表达式 */
auto IsColumnExpression(const AbstractExpressionRef &expr) -> bool {
  return dynamic_cast<const ColumnValueExpression *>(expr.get()) != nullptr;
}

/** 从等值比较表达式中提取连接键 */
auto ExtractJoinKeys(const AbstractExpressionRef &predicate, std::vector<AbstractExpressionRef> &left_keys,
                     std::vector<AbstractExpressionRef> &right_keys) -> bool {
  if (predicate == nullptr) {
    return false;
  }

  // AND 递归拆分
  if (const LogicExpression *logic_expr = dynamic_cast<const LogicExpression *>(predicate.get());
      logic_expr != nullptr) {
    if (logic_expr->logic_type_ == LogicType::And) {
      // 递归处理AND的两个子表达式
      return ExtractJoinKeys(logic_expr->GetChildAt(0), left_keys, right_keys) &&
             ExtractJoinKeys(logic_expr->GetChildAt(1), left_keys, right_keys);
    }
    return false;
  }

  // 识别 列=列，且一边来自左表，一边来自右表
  if (IsEquiComparison(predicate)) {
    const auto &comp_expr = dynamic_cast<const ComparisonExpression &>(*predicate);
    auto left_expr = comp_expr.GetChildAt(0);
    auto right_expr = comp_expr.GetChildAt(1);

    // 检查两边都是列表达式
    if (IsColumnExpression(left_expr) && IsColumnExpression(right_expr)) {
      const auto &left_col = dynamic_cast<const ColumnValueExpression &>(*left_expr);
      const auto &right_col = dynamic_cast<const ColumnValueExpression &>(*right_expr);

      // 确保一个来自左表（tuple_idex=0），一个来自右表（tuple_idex=1）
      if (left_col.GetTupleIdx() == 0 && right_col.GetTupleIdx() == 1) {
        left_keys.emplace_back(left_expr);
        right_keys.emplace_back(right_expr);
        return true;
      } else if (left_col.GetTupleIdx() == 1 && right_col.GetTupleIdx() == 0) {
        left_keys.emplace_back(right_expr);
        right_keys.emplace_back(left_expr);
        return true;
      }
    }
  }
  return false;
}

/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
 * with multiple eq conditions.
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for Spring 2025: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  // 递归优化子节点
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // 检查是否为 NLJ
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

    // 支持内连接和左外连接
    if (nlj_plan.GetJoinType() == JoinType::INNER || nlj_plan.GetJoinType() == JoinType::LEFT) {
      std::vector<AbstractExpressionRef> left_keys;
      std::vector<AbstractExpressionRef> right_keys;

      // 尝试从谓词中提取等值连接键
      if (ExtractJoinKeys(nlj_plan.Predicate(), left_keys, right_keys) && !left_keys.empty()) {
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(left_keys), std::move(right_keys),
                                                  nlj_plan.GetJoinType());
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
