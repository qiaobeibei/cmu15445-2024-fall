//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.h
//
// Identification: src/include/primer/hyperloglog_presto.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <bitset>
#include <memory>
#include <mutex>  // NOLINT
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"

/** @brief Dense bucket size. */
#define DENSE_BUCKET_SIZE 4
/** @brief Overflow bucket size. */
#define OVERFLOW_BUCKET_SIZE 3

/** @brief Total bucket size. */
#define TOTAL_BUCKET_SIZE (DENSE_BUCKET_SIZE + OVERFLOW_BUCKET_SIZE)

#define BITSET_CAPACITY 64

namespace bustub {

template <typename KeyType>
class HyperLogLogPresto {
  /**
   * INSTRUCTIONS: Testing framework will use the GetDenseBucket and GetOverflow function,
   * hence SHOULD NOT be deleted. It's essential to use the dense_bucket_
   * data structure.
   */

  /** @brief Constant for HLL. */
  static constexpr double CONSTANT = 0.79402;

 public:
  /** @brief Disabling default constructor. */
  HyperLogLogPresto() = delete;

  explicit HyperLogLogPresto(int16_t n_leading_bits);

  /** @brief Returns the dense_bucket_ data structure. */
  auto GetDenseBucket() const -> std::vector<std::bitset<DENSE_BUCKET_SIZE>> { return dense_bucket_; }

  /** @brief Returns overflow bucket of a specific given index. */
  auto GetOverflowBucketofIndex(uint16_t idx) { return overflow_bucket_[idx]; }

  /** @brief Retusn the cardinality of the set. */
  auto GetCardinality() const -> uint64_t { return cardinality_; }

  auto AddElem(KeyType val) -> void;

  auto ComputeCardinality() -> void;

 private:
  /** @brief Structure holding dense buckets (or also known as registers). */
  std::vector<std::bitset<DENSE_BUCKET_SIZE>> dense_bucket_;

  /** @brief Structure holding overflow buckets. */
  std::unordered_map<uint16_t, std::bitset<OVERFLOW_BUCKET_SIZE>> overflow_bucket_;

  /** @brief Storing cardinality value */
  uint64_t cardinality_;

  // TODO(student) - can add more data structures as required
  size_t num_registers_;  // 桶数量
  std::mutex m_;          // 保护线程安全
  const int16_t n_bits_;  // 记录b的数值大小

  /** @brief Calculate Hash.
   *
   * @param[in] val
   *
   * @returns hash value
   */
  inline auto CalculateHash(KeyType val) -> hash_t {
    Value val_obj;
    if constexpr (std::is_same<KeyType, std::string>::value) {
      val_obj = Value(VARCHAR, val);
      return bustub::HashUtil::HashValue(&val_obj);
    }
    if constexpr (std::is_same<KeyType, int64_t>::value) {
      return static_cast<hash_t>(val);
    }
    return 0;
  }

  /** @brief Converts a decimal hash value to a 64-bit binary representation. */
  auto ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY>;

  auto GetBucketValue(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t;

  auto PositionOfRightmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint16_t;

  auto CombineBucketsToDecimal(const uint64_t &b_index) -> uint8_t {
    std::bitset<7> combined_bitset;

    // 先从 dense_bucket_ 中获取低 4 位
    if (b_index < dense_bucket_.size()) {
      std::bitset<DENSE_BUCKET_SIZE> dense_bits = dense_bucket_[b_index];
      for (size_t i = 0; i < DENSE_BUCKET_SIZE; ++i) {
        combined_bitset[i] = dense_bits[i];
      }
    }

    // 再从 overflow_bucket_ 中获取高 3 位
    auto it = overflow_bucket_.find(static_cast<uint16_t>(b_index));
    if (it != overflow_bucket_.end()) {
      std::bitset<OVERFLOW_BUCKET_SIZE> overflow_bits = it->second;
      for (size_t i = 0; i < OVERFLOW_BUCKET_SIZE; ++i) {
        combined_bitset[i + DENSE_BUCKET_SIZE] = overflow_bits[i];
      }
    }

    // 将组合后的 7 位 bitset 转换为十进制
    return static_cast<uint8_t>(combined_bitset.to_ulong());
  }

  /**
   * @brief Function that insert p to bucket.
   *
   * @param[in] b_index - the index of bucket
   * @param[in] p - the value of bucket
   * @returns void
   */
  auto InsertPToBucket(const uint64_t &b_index, const uint16_t &p) -> void {
    {
      std::lock_guard<std::mutex> lk(m_);
      uint8_t old_p = CombineBucketsToDecimal(b_index);
      if (old_p < p) {
        if (p > 15) {
          std::bitset<7> p_bitset(p);
          // 提取低四位
          std::bitset<DENSE_BUCKET_SIZE> low_four_bits;
          for (size_t i = 0; i < DENSE_BUCKET_SIZE; ++i) {
            low_four_bits[i] = p_bitset[i];
          }
          // 存储低四位到 dense_bucket_ 中 b_index 对应的位置
          if (b_index >= dense_bucket_.size()) {
            dense_bucket_.resize(b_index + 1);
          }
          dense_bucket_[b_index] = low_four_bits;

          // 提取高三位
          std::bitset<OVERFLOW_BUCKET_SIZE> high_three_bits;
          for (size_t i = 0; i < OVERFLOW_BUCKET_SIZE; ++i) {
            high_three_bits[i] = p_bitset[i + 4];
          }
          // 存储高三位到 overflow_bucket_ 中 b_index 对应的位置
          overflow_bucket_[b_index] = high_three_bits;
        } else {
          // 如果 p 不大于 15，直接将 p 存储到 dense_bucket_ 中 b_index 对应的位置
          if (b_index >= dense_bucket_.size()) {
            dense_bucket_.resize(b_index + 1);
          }
          dense_bucket_[b_index] = std::bitset<DENSE_BUCKET_SIZE>(p);
        }
      }
    }
  }

};  // namespace bustub

}  // namespace bustub
