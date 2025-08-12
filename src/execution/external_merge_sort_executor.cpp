//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <iostream>
#include <optional>
#include <vector>
#include "common/config.h"
#include "execution/plans/sort_plan.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {}

template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  // 如果已经排序过，重新初始化迭代器
  if (!runs_.empty()) {
    iter_ = runs_[0].Begin();
    return;
  }

  child_executor_->Init();
  runs_.clear();

  // PASS 0: 创建初始排序运行
  CreateInitialRuns();

  // PASS 1,2,3...: 归并排序
  MergeRuns();

  if (!runs_.empty()) {
    iter_ = runs_[0].Begin();
  }
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  if (runs_.empty() || iter_ == runs_[0].End()) {
    return false;
  }
  *tuple = *iter_;
  *rid = tuple->GetRid();
  ++iter_;
  return true;
}

template <size_t K>
void ExternalMergeSortExecutor<K>::CreateInitialRuns() {
  const int initial_page_cnt = 4;
  Tuple child_tuple{};
  RID rid;
  const int tuple_size = static_cast<int>(sizeof(int32_t) + child_executor_->GetOutputSchema().GetInlinedStorageSize());
  const int max_size = (BUSTUB_PAGE_SIZE - SORT_PAGE_HEADER_SIZE) / tuple_size;
  
  while (true) {
    std::vector<page_id_t> pages;
    page_id_t page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
    pages.emplace_back(page_id);
    int cnt = 0;

    WritePageGuard page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(page_id);
    auto sort_page = page_guard.AsMut<SortPage>();
    sort_page->Init(0, max_size, tuple_size);

    std::vector<SortEntry> entries;
    bool tuples_over = false;
    
    // 收集元组进行排序
    while (cnt < max_size * initial_page_cnt) {
      if (child_executor_->Next(&child_tuple, &rid)) {
        entries.push_back({GenerateSortKey(child_tuple, plan_->GetOrderBy(), GetOutputSchema()), std::move(child_tuple)});
        cnt++;
      } else {
        tuples_over = true;
        break;
      }
    }

    if (cnt == 0 && tuples_over) {
      exec_ctx_->GetBufferPoolManager()->DeletePage(page_id);
      break;
    }

    // 排序并写入页面
    std::sort(entries.begin(), entries.end(), cmp_);
    cnt = 0;
    for (const auto &entry : entries) {
      if (cnt == max_size) {
        page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
        pages.emplace_back(page_id);
        page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(page_id);
        sort_page = page_guard.AsMut<SortPage>();
        sort_page->Init(0, max_size, tuple_size);
        cnt = 0;
      }
      sort_page->InsertTuple(entry.second);
      cnt++;
    }
    
    runs_.emplace_back(MergeSortRun(pages, exec_ctx_->GetBufferPoolManager()));
    if (tuples_over) break;
  }
}

template <size_t K>
void ExternalMergeSortExecutor<K>::MergeRuns() {
  while (runs_.size() > 1) {
    std::vector<MergeSortRun> new_runs;
    const int n = runs_.size();
    
    for (int i = 0; i < n; i += 2) {
      if (i + 1 >= n) {
        new_runs.push_back(runs_[i]);
        break;
      }

      // 归并两个运行
      std::vector<page_id_t> new_pages;
      auto iter_a = runs_[i].Begin();
      auto iter_b = runs_[i + 1].Begin();
      
      const int tuple_size = static_cast<int>(sizeof(int32_t) + GetOutputSchema().GetInlinedStorageSize());
      const int max_size = (BUSTUB_PAGE_SIZE - SORT_PAGE_HEADER_SIZE) / tuple_size;
      
      page_id_t new_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      new_pages.emplace_back(new_page_id);
      auto page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(new_page_id);
      auto sort_page = page_guard.AsMut<SortPage>();
      sort_page->Init(0, max_size, tuple_size);

      int cnt = 0;
      
      // 归并两个运行中的元组
      while (iter_a != runs_[i].End() && iter_b != runs_[i + 1].End()) {
        if (cnt >= max_size) {
          new_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
          new_pages.emplace_back(new_page_id);
          page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(new_page_id);
          sort_page = page_guard.AsMut<SortPage>();
          sort_page->Init(0, max_size, tuple_size);
          cnt = 0;
        }

        SortEntry entry_a = {GenerateSortKey((*iter_a), plan_->GetOrderBy(), GetOutputSchema()), *iter_a};
        SortEntry entry_b = {GenerateSortKey((*iter_b), plan_->GetOrderBy(), GetOutputSchema()), *iter_b};
        
        if (cmp_(entry_a, entry_b)) {
          sort_page->InsertTuple(entry_a.second);
          ++iter_a;
        } else {
          sort_page->InsertTuple(entry_b.second);
          ++iter_b;
        }
        cnt++;
      }

      // 处理剩余元组
      while (iter_a != runs_[i].End()) {
        if (cnt >= max_size) {
          new_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
          new_pages.emplace_back(new_page_id);
          page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(new_page_id);
          sort_page = page_guard.AsMut<SortPage>();
          sort_page->Init(0, max_size, tuple_size);
          cnt = 0;
        }
        sort_page->InsertTuple(*iter_a);
        ++iter_a;
        cnt++;
      }
      
      while (iter_b != runs_[i + 1].End()) {
        if (cnt >= max_size) {
          new_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
          new_pages.emplace_back(new_page_id);
          page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(new_page_id);
          sort_page = page_guard.AsMut<SortPage>();
          sort_page->Init(0, max_size, tuple_size);
          cnt = 0;
        }
        sort_page->InsertTuple(*iter_b);
        ++iter_b;
        cnt++;
      }

      // 清理旧页面
      for (auto page_id : runs_[i].GetPages()) {
        exec_ctx_->GetBufferPoolManager()->DeletePage(page_id);
      }
      for (auto page_id : runs_[i + 1].GetPages()) {
        exec_ctx_->GetBufferPoolManager()->DeletePage(page_id);
      }

      new_runs.emplace_back(MergeSortRun(new_pages, exec_ctx_->GetBufferPoolManager()));
    }

    runs_ = std::move(new_runs);
  }
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub