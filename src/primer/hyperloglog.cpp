//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog.h"
#include <cstddef>
#include <stdexcept>

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0), n_bits_(n_bits) {
  if (n_bits < 0) {
    // throw std::invalid_argument("n_bits must be greater than or equal to 0");
    return;
  }
  num_registers_ = 1 << n_bits;
  registers_.resize(num_registers_, 0);  // 初始化桶数
}

/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  // hash_t 的类型是 std::size_t
  return std::bitset<BITSET_CAPACITY>{hash};  // 用 std::bitset 的构造函数返回64位二进制数
}

/**
 * @brief Function that computes leading zeros.
 *
 * @param[in] bset - binary values of a given bitset
 * @returns leading zeros of given binary set
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint8_t {
  /** @TODO(student) Implement this function! */
  for (size_t i = BITSET_CAPACITY - n_bits_; i > 0; --i) {
    if (bset[i - 1]) {
      return BITSET_CAPACITY - i - n_bits_;
    }
  }
  return BITSET_CAPACITY - n_bits_;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::GetBucketValue(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  uint64_t value = 0;
  for (int i = 0; i < n_bits_; ++i) {
    value <<= 1;  // 左移一位
    value |= static_cast<uint8_t>(bset[BITSET_CAPACITY - 1 - i]);
  }
  return value;
}

/**
 * @brief Adds a value into the HyperLogLog.
 *
 * @param[in] val - value that's added into hyperloglog
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  const hash_t hash = CalculateHash(val);                    // 生成哈希值
  std::bitset<BITSET_CAPACITY> b_set = ComputeBinary(hash);  // 哈希值转换为二进制
  uint64_t b_index = GetBucketValue(b_set);                  // 桶索引
  uint8_t p = PositionOfLeftmostOne(b_set) + 1;              // 最高位1的索引
  InsertPToBucket(b_index, p);                               // 在对应桶中插入相应值
}

/**
 * @brief Function that computes cardinality.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  // 计算求和部分
  double sum = 0.0;
  for (auto p_max : registers_) {
    sum += std::pow(2.0, -static_cast<double>(p_max));
  }
  {
    std::lock_guard<std::mutex> lk(m_);
    // 计算估计值
    cardinality_ = static_cast<size_t>(CONSTANT * num_registers_ * num_registers_ / sum);
  }
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
