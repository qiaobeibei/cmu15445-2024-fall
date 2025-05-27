//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <exception>
#include "common/exception.h"
#include "common/logger.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  LOG_DEBUG("DiskScheduler 析构函数被调用");
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    LOG_DEBUG("等待工作线程退出...");
    background_thread_->join();
    LOG_DEBUG("工作线程已退出");
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param r The request to be scheduled.
 */
void DiskScheduler::Schedule(DiskRequest r) {
  if (r.data_ == nullptr || r.page_id_ == INVALID_PAGE_ID) {
    LOG_WARN("Invalid DiskRequest: data_ is null or page_id_ is invalid");
    // 设置 promise 为失败状态，避免调用者永久等待
    r.callback_.set_exception(std::make_exception_ptr(Exception("Invalid disk request")));
    return;
  }
  request_queue_.Put(std::optional<DiskRequest>{std::move(r)});
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread() {
  for (;;) {
    auto request_opt = request_queue_.Get();

    // 检查终止信号
    if (!request_opt.has_value()) {
      LOG_DEBUG("DiskScheduler worker thread received termination signal");
      break;
    }

    // 获取Schedule()传入的请求r
    auto request = std::move(request_opt.value());
    LOG_DEBUG("处理请求: page_id=%d, is_write=%d", request.page_id_, request.is_write_);

    try {
      // 调用 disk_manager_的方法往指定页中读或写数据
      if (request.is_write_) {
        disk_manager_->WritePage(request.page_id_, request.data_);
      } else {
        disk_manager_->ReadPage(request.page_id_, request.data_);
      }
      // LOG_DEBUG("请求处理完成，设置 promise 值");
      // 请求处理成功，通过 promise 通知调用者
      request.callback_.set_value(true);
    } catch (...) {
      LOG_ERROR("Disk operation failed with unknown exception");
      request.callback_.set_exception(std::current_exception());
    }
  }
}

}  // namespace bustub
