//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <future>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/logger.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id)
    : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0), page_id_(INVALID_PAGE_ID) {
  Reset();
}

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  page_id_ = INVALID_PAGE_ID;
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param k_dist The backward k-distance for the LRU-K replacer.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  // LOG_DEBUG("获取新页");
  // 1.加锁
  std::scoped_lock<std::mutex> lk(*bpm_latch_);

  // 2.查找可用内存帧
  frame_id_t frame_id;
  if (!free_frames_.empty()) {
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  } else {
    auto maybe_frame_id = replacer_->Evict();
    if (!maybe_frame_id.has_value()) {
      // LOG_ERROR("淘汰失败，无法分配新页");
      return INVALID_PAGE_ID;  // 淘汰失败，无法分配新页
    }
    // 获取目标帧头
    frame_id = maybe_frame_id.value();
    auto &frame = frames_[frame_id];
    std::lock_guard<std::shared_mutex> wtire_lock(frame->rwlatch_);

    // 目标帧头是否为脏页

    // if (frame->is_dirty_) {
    std::promise<bool> write_promise;
    auto write_future = write_promise.get_future();
    // 调度写请求
    disk_scheduler_->Schedule({true, frame->GetDataMut(), frame->page_id_, std::move(write_promise)});
    // 等待写请求执行
    if (!write_future.get()) {
      LOG_ERROR("Disk write failed during page eviction");
      return INVALID_PAGE_ID;
    }

    frame->is_dirty_ = false;
    //}

    // 从页表中删除被淘汰的页面
    if (frame->page_id_ != INVALID_PAGE_ID) {
      page_table_.erase(frame->page_id_);
      frame->page_id_ = INVALID_PAGE_ID;  // 重置页ID
    }
    // 重置帧状态
    frame->Reset();
  }
  // 3.生成新页面ID（原子操作保证线程安全）
  // TODO(P1): 内存序可能要修改
  const page_id_t new_page_id = next_page_id_.fetch_add(1, std::memory_order_relaxed);

  // 4.绑定新页与帧
  auto &new_frame = frames_[frame_id];
  std::lock_guard<std::shared_mutex> new_write_lk(new_frame->rwlatch_);
  new_frame->pin_count_ = 0;
  new_frame->is_dirty_ = false;
  new_frame->page_id_ = new_page_id;
  page_table_[new_page_id] = frame_id;  // 更新页表

  // 5.将新帧加入替换器
  // replacer_->RecordAccess(frame_id);
  // replacer_->SetEvictable(frame_id, false);

  return new_page_id;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places a page or a page's metadata could be, and use that to guide you on implementing this
 * function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * You should call `DeallocatePage` in the disk scheduler to make the space available for new pages.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // LOG_DEBUG("删除指定页");
  std::unique_lock<std::mutex> lk(*bpm_latch_);

  // 检查页表中是否存在该页面
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 页面不存在于缓冲池中，直接返回true（因为不存在所以可以认为删除成功）
    return true;
  }

  // 获取页面所在的帧头
  frame_id_t frame_id = it->second;
  auto &frame = frames_[frame_id];

  // 该页是否被其他线程访问
  if (frame->pin_count_ > 0) {
    return false;
  }

  // 页面未被锁定，可以删除
  // 先处理脏页，如果是脏页则写回磁盘
  // std::lock_guard<std::shared_mutex> write_lk(frame->rwlatch_);
  // if (frame->is_dirty_) {
  //   std::promise<bool> write_promise;
  //   auto write_future = write_promise.get_future();
  //   // 构造写请求
  //   disk_scheduler_->Schedule({true, frame->GetDataMut(), frame->page_id_, std::move(write_promise)});

  //   if (!write_future.get()) {
  //     LOG_ERROR("Disk write failed during page deletion");
  //     return false;
  //   }

  //   frame->is_dirty_ = false;
  // }
  lk.unlock();
  // 写回脏页
  if (frame->is_dirty_) {
    FlushPage(page_id);
  }
  lk.lock();
  // 重新查找迭代器
  it = page_table_.find(page_id);
  if (it == page_table_.end() || it->second != frame_id) {
    return true;
  }
  if (frame->pin_count_ > 0) {
    return false;
  }
  page_table_.erase(it);             // 从页表中删除该页面的映射
  free_frames_.push_back(frame_id);  // 将该帧标记为空闲
  replacer_->SetEvictable(frame_id, false);
  disk_scheduler_->DeallocatePage(page_id);  // 释放磁盘空间
  frame->Reset();                            // 重置帧的状态

  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are 3 main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  // LOG_DEBUG("获取受保护的写页");
  std::unique_lock<std::mutex> lk(*bpm_latch_);

  // 1.检查页面是否已在缓冲池中
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // 获取内存中的页
    frame_id_t frame_id = it->second;
    auto &frame = frames_[frame_id];

    // 记录访问
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    frame->pin_count_++;

    lk.unlock();
    return std::make_optional(WritePageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_));
  }

  // 2. 页面不在内存中，尝试分配或淘汰帧
  frame_id_t frame_id;
  if (!free_frames_.empty()) {
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  } else {
    auto maybe_evict_id = replacer_->Evict();
    if (!maybe_evict_id.has_value()) {
      return std::nullopt;
    }
    frame_id = maybe_evict_id.value();
    auto &evict_frame = frames_[frame_id];

    // 处理脏页
    std::lock_guard<std::shared_mutex> write_lk(evict_frame->rwlatch_);
    if (evict_frame->is_dirty_) {
      std::promise<bool> write_promise;
      auto write_future = write_promise.get_future();
      disk_scheduler_->Schedule({true, evict_frame->GetDataMut(), evict_frame->page_id_, std::move(write_promise)});
      if (!write_future.get()) {
        LOG_ERROR("Disk write failed during page deletion");
        return std::nullopt;
      }
      evict_frame->is_dirty_ = false;
    }

    // 从页表中删除被淘汰的页面
    page_table_.erase(evict_frame->page_id_);
    evict_frame->Reset();
    replacer_->SetEvictable(frame_id, false);
  }

  // 3. 从磁盘读取目标页面到刚才分配或淘汰的帧中
  auto &target_frame = frames_[frame_id];
  // 获取读锁
  std::unique_lock<std::shared_mutex> read_lk(target_frame->rwlatch_);
  std::promise<bool> read_promise;
  auto read_future = read_promise.get_future();
  disk_scheduler_->Schedule({false, target_frame->GetDataMut(), page_id, std::move(read_promise)});
  if (!read_future.get()) {
    // 读盘失败，释放资源
    LOG_ERROR("读盘失败");
    free_frames_.push_back(frame_id);
    replacer_->SetEvictable(frame_id, true);
    return std::nullopt;
  }

  // 4. 更新缓冲池元数据
  page_table_[page_id] = frame_id;
  target_frame->page_id_ = page_id;
  target_frame->pin_count_ = 1;
  target_frame->is_dirty_ = false;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  read_lk.unlock();
  lk.unlock();
  return std::make_optional(WritePageGuard(page_id, target_frame, replacer_, bpm_latch_, disk_scheduler_));
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  // LOG_DEBUG("获取受保护的读页");
  std::unique_lock<std::mutex> lk(*bpm_latch_);

  // 1.检查页面是否已在缓冲池中
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    auto &frame = frames_[frame_id];

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    frame->pin_count_++;

    lk.unlock();
    return std::make_optional(ReadPageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_));
  }

  // 2.页面不在内存中，尝试分配或淘汰帧
  frame_id_t frame_id;
  if (!free_frames_.empty()) {
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  } else {
    auto maybe_evict_id = replacer_->Evict();
    if (!maybe_evict_id.has_value()) {
      return std::nullopt;
    }

    frame_id = maybe_evict_id.value();
    auto &evict_frame = frames_[frame_id];

    // 处理脏页
    {
      std::lock_guard<std::shared_mutex> write_lk(evict_frame->rwlatch_);

      if (evict_frame->is_dirty_) {
        std::promise<bool> write_promise;
        auto write_future = write_promise.get_future();
        disk_scheduler_->Schedule({true, evict_frame->GetDataMut(), evict_frame->page_id_, std::move(write_promise)});
        if (!write_future.get()) {
          LOG_ERROR("Disk write failed during page deletion");
          return std::nullopt;
        }
        evict_frame->is_dirty_ = false;
      }
    }

    page_table_.erase(evict_frame->page_id_);
    evict_frame->Reset();
    replacer_->SetEvictable(frame_id, false);
  }

  // 3. 从磁盘读取目标页面到刚才分配或淘汰的帧中
  auto &target_frame = frames_[frame_id];
  std::unique_lock<std::shared_mutex> read_lk(target_frame->rwlatch_);
  std::promise<bool> read_promise;
  auto read_future = read_promise.get_future();
  disk_scheduler_->Schedule({false, target_frame->GetDataMut(), page_id, std::move(read_promise)});
  if (!read_future.get()) {
    // 读盘失败，释放资源
    LOG_ERROR("读盘失败");
    free_frames_.push_back(frame_id);
    replacer_->SetEvictable(frame_id, true);
    return std::nullopt;
  }

  // 4. 更新缓冲池元数据
  page_table_[page_id] = frame_id;
  target_frame->page_id_ = page_id;
  target_frame->pin_count_ = 1;
  target_frame->is_dirty_ = false;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  read_lk.unlock();
  lk.unlock();
  return std::make_optional(ReadPageGuard(page_id, target_frame, replacer_, bpm_latch_, disk_scheduler_));
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk unsafely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should not take a lock on the page in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  // LOG_DEBUG("不安全的刷新页");
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = it->second;
  auto &frame = frames_[frame_id];

  // 检查并重置脏页标志（无帧级锁，但在全局锁保护下）
  if (frame->is_dirty_) {
    frame->is_dirty_ = false;

    // 提交写盘请求（异步执行，不等待结果）
    std::promise<bool> write_promise;
    disk_scheduler_->Schedule({true, frame->GetDataMut(), page_id, std::move(write_promise)});
  }

  return true;
}

/**
 * @brief Flushes a page's data out to disk safely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory,
 * this function will return `false`.
 *
 * You should take a lock on the page in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `Flush` in the page guards, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
// auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
//   std::scoped_lock<std::mutex> lk(*bpm_latch_);
//   auto it = page_table_.find(page_id);
//   if (it == page_table_.end()) {
//     return false;
//   }
//   frame_id_t frame_id = it->second;
//   auto &frame = frames_[frame_id];

//   // 获取帧的写锁，确保刷新时独占访问
//   std::lock_guard<std::shared_mutex> write_lk(frame->rwlatch_);

//   if (frame->is_dirty_) {
//     std::promise<bool> write_promise;
//     auto write_future = write_promise.get_future();
//     disk_scheduler_->Schedule({true, frame->GetDataMut(), page_id, std::move(write_promise)});

//     if (write_future.get()) {
//       frame->is_dirty_ = false;  // 清除脏页标志
//       return true;
//     }
//     LOG_ERROR("Failed to flush page %d to disk", page_id);
//     return false;
//   }

//   return true;
// }

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // LOG_DEBUG("刷新页");
  // 先获取全局锁，查找页面所在帧
  std::unique_lock<std::mutex> lk(*bpm_latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // LOG_DEBUG("Page %d not found in page table", page_id);
    return false;
  }

  frame_id_t frame_id = it->second;
  auto &frame = frames_[frame_id];

  lk.unlock();
  // 获取帧的写锁，确保刷新时独占访问
  // std::unique_lock<std::shared_mutex> write_lk(frame->rwlatch_);

  // if (!frame->is_dirty_) {
  //   LOG_DEBUG("Page %d is not dirty, skipping flush", page_id);
  //   return false;
  // }

  // 创建写操作的promise和future
  std::promise<bool> write_promise;
  auto write_future = write_promise.get_future();

  // 提交写操作到磁盘调度器
  try {
    disk_scheduler_->Schedule({true, frame->GetDataMut(), page_id, std::move(write_promise)});
  } catch (const std::exception &e) {
    LOG_ERROR("Failed to schedule disk write for page %d: %s", page_id, e.what());
    return false;
  }

  // 等待写操作完成
  try {
    if (write_future.get()) {
      frame->is_dirty_ = false;
      // LOG_DEBUG("Successfully flushed page %d to disk", page_id);
      return true;
    }
  } catch (const std::exception &e) {
    LOG_ERROR("Exception during disk write for page %d: %s", page_id, e.what());
  }

  LOG_ERROR("Failed to flush page %d to disk", page_id);
  return false;
}

/**
 * @brief Flushes all page data that is in memory to disk unsafely.
 *
 * You should not take locks on the pages in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPagesUnsafe() {
  // LOG_DEBUG("不安全刷新全部页");
  for (const auto &[page_id, frame_id] : page_table_) {
    auto &frame = frames_[frame_id];

    if (frame->is_dirty_) {
      frame->is_dirty_ = false;

      std::promise<bool> write_promise;
      disk_scheduler_->Schedule({true, frame->GetDataMut(), page_id, std::move(write_promise)});
    }
  }
}

/**
 * @brief Flushes all page data that is in memory to disk safely.
 *
 * You should take locks on the pages in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  // LOG_DEBUG("刷新全部页");
  std::scoped_lock<std::mutex> lk(*bpm_latch_);

  for (const auto &[page_id, frame_id] : page_table_) {
    auto &frame = frames_[frame_id];
    {
      // std::unique_lock<std::shared_mutex> write_lock(frame->rwlatch_);

      std::promise<bool> write_promise;
      auto write_future = write_promise.get_future();
      disk_scheduler_->Schedule({true, frame->GetDataMut(), page_id, std::move(write_promise)});

      if (write_future.get()) {
        frame->is_dirty_ = false;
      } else {
        LOG_ERROR("Failed to flush page %d to disk", page_id);
      }
    }
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple
 * threads access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely
 * cause problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds
 * the page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You
 * will still need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists, otherwise `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  // LOG_DEBUG("获取引用计数");
  std::scoped_lock<std::mutex> lk(*bpm_latch_);

  // 页面是否存在于页表中
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }

  frame_id_t frame_id = it->second;
  auto &frame = frames_[frame_id];

  return std::make_optional(static_cast<size_t>(frame->pin_count_.load()));
}

}  // namespace bustub
