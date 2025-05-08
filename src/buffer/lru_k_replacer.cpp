//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <cstdlib>
#include <iterator>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // for (frame_id_t i = 0; i < static_cast<frame_id_t>(num_frames); ++i) {
  //   node_store_[i] = LRUKNode(k, i);
  // }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  if (curr_size_ == 0) {  // LRUKReplacer 中可用来淘汰页的数量
    return std::nullopt;
  }

  std::function<bool(const frame_id_t &a, const frame_id_t &b)> cmp = [this](const frame_id_t &a,
                                                                             const frame_id_t &b) -> bool {
    const LRUKNode &node_a = node_store_.at(a);  // 帧id对应的页
    const LRUKNode &node_b = node_store_.at(b);
    size_t dist_a = CalculateBackwardKDistance(node_a);  // 计算页的反向K距离
    size_t dist_b = CalculateBackwardKDistance(node_b);
    if (dist_a != dist_b) {  // 若两帧的反向K不同，反向 K 距离大的帧优先级高
      return dist_a < dist_b;
    }
    // 若两帧的反向K相同，比较两个帧的最早访问时间，最早访问时间早的帧优先级高
    return node_a.history_.front() > node_b.history_.front();
  };

  {
    std::lock_guard<std::mutex> lk(latch_);
    std::priority_queue<frame_id_t, std::vector<frame_id_t>, decltype(cmp)> pq(cmp);  // 创建使用cmp为比较函数的优先队列
    for (const auto &pair : node_store_) {
      const LRUKNode &node = pair.second;
      if (node.is_evictable_) {
        pq.push(pair.first);
      }
    }

    if (!pq.empty()) {
      frame_id_t victim_frame = pq.top();  // 队首元素是优先级最高的帧
      node_store_.erase(victim_frame);
      curr_size_--;
      return victim_frame;
    }
  }
  return std::nullopt;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // 帧id有效性
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_), "frame_id is out of valid range.");

  // 为给定帧id创建LRUKNode对象
  {
    std::lock_guard<std::mutex> lk(latch_);
    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
      node_store_[frame_id] = LRUKNode(k_, frame_id);
    }
  }

  size_t timestamp;
  {
    std::lock_guard<std::mutex> lk(latch_);
    timestamp = current_timestamp_++;
    LRUKNode &node = node_store_.at(frame_id);
    node.history_.push_back(timestamp);
    // 如果访问历史长度超过 k，移除最早的访问记录
    if (node.history_.size() > k_) {
      node.history_.pop_front();
    }
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // 帧id有效性
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_), "frame_id is out of valid range.");

  {
    std::lock_guard<std::mutex> lk(latch_);
    // 检查帧是否存在
    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
      return;
    }

    LRUKNode &node = it->second;
    if (set_evictable && !node.is_evictable_) {
      curr_size_++;
    } else if (!set_evictable && node.is_evictable_) {
      curr_size_--;
    }

    node.is_evictable_ = set_evictable;
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  // 帧id有效性
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_), "frame_id is out of valid range.");

  {
    std::lock_guard<std::mutex> lk(latch_);
    // 检查帧是否存在
    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
      return;
    }

    // 检查帧是否可淘汰
    LRUKNode &node = it->second;
    BUSTUB_ASSERT(node.is_evictable_, "Frame is not evictable.");

    // 移除帧及其访问历史
    node_store_.erase(it);
  }

  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::CalculateBackwardKDistance(const LRUKNode &node) -> size_t {
  if (node.history_.size() < k_) {
    return std::numeric_limits<size_t>::max();
  }
  auto it = node.history_.begin();
  // 移动到倒数第 k 次访问时间
  std::advance(it, node.history_.size() - k_);
  return current_timestamp_ - *it;
}

}  // namespace bustub
