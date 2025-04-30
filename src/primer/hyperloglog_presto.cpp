//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"
#include <bits/stdint-uintn.h>
#include <cstddef>

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits)
    : cardinality_(0), num_registers_(0), n_bits_(n_leading_bits) {
  if (n_leading_bits < 0) {
    return;
  }
  num_registers_ = 1 << n_leading_bits;
  dense_bucket_.resize(num_registers_, std::bitset<DENSE_BUCKET_SIZE>{0000});
}

/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  // hash_t 的类型是 std::size_t
  if (hash == 262144) {
    std::cout << (std::bitset<BITSET_CAPACITY>{hash});
  }
  return std::bitset<BITSET_CAPACITY>{hash};  // 用 std::bitset 的构造函数返回64位二进制数
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::GetBucketValue(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  uint64_t value = 0;
  for (int i = 0; i < n_bits_; ++i) {
    value <<= 1;  // 左移一位
    value |= static_cast<uint8_t>(bset[BITSET_CAPACITY - 1 - i]);
  }
  return value;
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::PositionOfRightmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint16_t {
  uint16_t cnt = 0;
  for (size_t i = 0; i < BITSET_CAPACITY; ++i) {
    if (bset[i]) {
      return cnt;
    }
    ++cnt;
  }
  return BITSET_CAPACITY - n_bits_;  // 全为0，那么p为63
}

/** @brief Element is added for HLL calculation. */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  const hash_t hash = CalculateHash(val);                    // 生成哈希值
  std::bitset<BITSET_CAPACITY> b_set = ComputeBinary(hash);  // 哈希值转换为64位二进制
  uint64_t b_index = GetBucketValue(b_set);                  // 桶索引
  uint16_t p = PositionOfRightmostOne(b_set);                // 右最长连续零个数
  InsertPToBucket(b_index, p);                               // 在对应桶中插入相应值
}

/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  if (num_registers_ == 0) {
    return;
  }
  // 计算求和部分
  double sum = 0.0;

  for (size_t i = 0; i < num_registers_; ++i) {
    uint8_t p_max = CombineBucketsToDecimal(i);
    sum += std::pow(2.0, -static_cast<double>(p_max));
  }
  {
    std::lock_guard<std::mutex> lk(m_);
    // 计算估计值
    cardinality_ = static_cast<size_t>(CONSTANT * num_registers_ * num_registers_ / sum);
  }
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
