//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.h
//
// Identification: src/include/storage/disk/disk_scheduler.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <future>  // NOLINT
#include <optional>
#include <thread>  // NOLINT

#include "common/channel.h"
#include "common/config.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * @brief Represents a Write or Read request for the DiskManager to execute.
 */
struct DiskRequest {
  /** Flag indicating whether the request is a write or a read. */
  bool is_write_;

  /**
   *  Pointer to the start of the memory location where a page is either:
   *   1. being read into from disk (on a read).
   *   2. being written out to disk (on a write).
   */
  char *data_;

  /** ID of the page being read from / written to disk. */
  page_id_t page_id_;

  /** Callback used to signal to the request issuer when the request has been completed. */
  std::promise<bool> callback_;

  DiskRequest(bool is_write, char *data, page_id_t page_id, std::promise<bool> callback)
      : is_write_(is_write), data_(data), page_id_(page_id), callback_(std::move(callback)) {}

  DiskRequest(const DiskRequest &other)
      : is_write_(other.is_write_), data_(other.data_), page_id_(other.page_id_), callback_() {
    // 这里创建了一个新的 promise 对象，原 promise 的状态不会被复制,需要使用新的 callback_ 获取完成信号
  }

  auto operator=(const DiskRequest &other) -> DiskRequest & {
    if (this != &other) {
      is_write_ = other.is_write_;
      data_ = other.data_;
      page_id_ = other.page_id_;
      callback_ = std::promise<bool>();  // 创建新的 promise
      // 注意：原 promise 的状态会被丢弃，调用者需要使用新的 callback_
    }
    return *this;
  }

  DiskRequest(DiskRequest &&other) noexcept
      : is_write_(other.is_write_),
        data_(other.data_),
        page_id_(other.page_id_),
        callback_(std::move(other.callback_)) {
    other.data_ = nullptr;  // 避免原对象析构时释放内存
  }

  auto operator=(DiskRequest &&other) noexcept -> DiskRequest & {
    if (this != &other) {
      is_write_ = other.is_write_;
      data_ = other.data_;
      page_id_ = other.page_id_;
      callback_ = std::move(other.callback_);
      other.data_ = nullptr;  // 避免原对象析构时释放内存
    }
    return *this;
  }
};

/**
 * @brief The DiskScheduler schedules disk read and write operations.
 *
 * A request is scheduled by calling DiskScheduler::Schedule() with an appropriate DiskRequest object. The scheduler
 * maintains a background worker thread that processes the scheduled requests using the disk manager. The background
 * thread is created in the DiskScheduler constructor and joined in its destructor.
 */
class DiskScheduler {
 public:
  explicit DiskScheduler(DiskManager *disk_manager);
  ~DiskScheduler();

  void Schedule(DiskRequest r);

  void StartWorkerThread();

  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * @brief Create a Promise object. If you want to implement your own version of promise, you can change this function
   * so that our test cases can use your promise implementation.
   *
   * @return std::promise<bool>
   */
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };

  /**
   * @brief Deallocates a page on disk.
   *
   * Note: You should look at the documentation for `DeletePage` in `BufferPoolManager` before using this method.
   *
   * @param page_id The page ID of the page to deallocate from disk.
   */
  void DeallocatePage(page_id_t page_id) { disk_manager_->DeletePage(page_id); }

 private:
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));
  /** A shared queue to concurrently schedule and process requests. When the DiskScheduler's destructor is called,
   * `std::nullopt` is put into the queue to signal to the background thread to stop execution. */
  Channel<std::optional<DiskRequest>> request_queue_;
  /** The background thread responsible for issuing scheduled requests to the disk manager. */
  std::optional<std::thread> background_thread_;
};
}  // namespace bustub
