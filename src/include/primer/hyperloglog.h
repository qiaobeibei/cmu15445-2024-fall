//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.h
//
// Identification: src/include/primer/hyperloglog.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <bits/c++config.h>
#include <bitset>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <utility>
#include <vector>
#include "common/util/hash_util.h"

/** @brief Capacity of the bitset stream. */
#define BITSET_CAPACITY 64

namespace bustub {

template <typename KeyType>
class HyperLogLog {
  /** @brief Constant for HLL. */
  static constexpr double CONSTANT = 0.79402;

 public:
  /** @brief Disable default constructor. */
  HyperLogLog() = delete;

  explicit HyperLogLog(int16_t n_bits);

  /**
   * @brief Getter value for cardinality.
   *
   * @returns cardinality value
   */
  auto GetCardinality() { return cardinality_; }

  auto AddElem(KeyType val) -> void;

  auto ComputeCardinality() -> void;

 private:
  /** @brief Cardinality value. */
  size_t cardinality_;

  /** @todo (student) can add their data structures that support HyperLogLog */
  std::vector<uint8_t> registers_;
  std::mutex m_;          // 保护线程安全
  const int16_t n_bits_;  // 记录b的数值大小
  size_t num_registers_;  // 桶数量

  /**
   * @brief Calculates Hash of a given value.
   *
   * @param[in] val - value
   * @returns hash integer of given input value
   */
  inline auto CalculateHash(KeyType val) -> hash_t {
    Value val_obj;
    if constexpr (std::is_same<KeyType, std::string>::value) {
      val_obj = Value(VARCHAR, val);
    } else {
      val_obj = Value(BIGINT, val);
    }
    return bustub::HashUtil::HashValue(&val_obj);
  }

  auto ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY>;

  auto PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint8_t;

  auto GetBucketValue(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t;

  /**
   * @brief Function that insert p to bucket.
   *
   * @param[in] b_index - the index of bucket
   * @param[in] p - the value of bucket
   * @returns void
   */
  inline void InsertPToBucket(const uint64_t &b_index, const uint8_t &p) {
    {
      std::lock_guard<std::mutex> lk(m_);
      uint64_t actual_index = b_index % registers_.size();
      registers_[actual_index] = std::max(registers_[actual_index], p);
    }
  }
};

}  // namespace bustub
