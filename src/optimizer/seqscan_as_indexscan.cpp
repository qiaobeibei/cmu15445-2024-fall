//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(P3): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  // 递归优化子节点
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeSeqScanAsIndexScan(child));
  }

  // 检查当前节点是否为SeqScan
  if (plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*plan);

    // 检查是否有过滤谓词
    if (seq_scan_plan.filter_predicate_ != nullptr) {
      // 获取表的所有索引
      auto indexes = catalog_.GetTableIndexes(seq_scan_plan.table_name_);

      for (const auto &index_info : indexes) {
        // 检查索引是否可以用于优化
        if (CanUseIndexForPredicate(seq_scan_plan.filter_predicate_.get(), index_info.get())) {
          // 创建IndexScan计划
          auto index_scan_plan =
              std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_,
                                                  index_info->index_oid_, seq_scan_plan.filter_predicate_);

          return index_scan_plan;
        }
      }
    }
  }

  // 如果不是SeqScan或者无法优化，创建新的计划节点
  return plan->CloneWithChildren(std::move(children));
}

/**
 * @brief 检查索引是否可以用于优化给定的谓词
 */
auto Optimizer::CanUseIndexForPredicate(const AbstractExpression *predicate, const IndexInfo *index_info) -> bool {
  if (predicate == nullptr || index_info == nullptr) {
    return false;
  }

  // 这里实现一个简单的检查逻辑
  // 在实际实现中，需要更复杂的谓词分析

  // 检查索引是否支持等值查询
  // 对于B+树索引，支持等值查询和范围查询
  // 这里我们假设所有索引都支持
  return true;
}

}  // namespace bustub
