//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"

#include "storage/index/generic_key.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Clear() {
  for (auto iter = begin(occupied_); iter != end(occupied_); iter++) {
    *iter = 0;
  }
  for (auto iter = begin(readable_); iter != end(readable_); iter++) {
    *iter = 0;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
slot_offset_t HASH_TABLE_BLOCK_TYPE::SlotsNum() const {
  return BLOCK_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::check_byte_bit(char a_byte, size_t bit_offset) const {
  std::bitset<8> a_bitset(a_byte);
  return a_bitset.test(bit_offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
  return array_[bucket_ind].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  return array_[bucket_ind].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
  auto byte_offset = bucket_ind / 8;
  auto bit_offset = bucket_ind % 8;
  char x = 1;
  occupied_[byte_offset].fetch_or(x << bit_offset);
  if (check_byte_bit(readable_[byte_offset].fetch_or(x << bit_offset), bit_offset)) {
    return false;
  }
  array_[bucket_ind] = std::make_pair(key, value);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
  if (IsOccupied(bucket_ind)) {
    auto byte_offset = bucket_ind / 8;
    auto bit_offset = bucket_ind % 8;
    signed char x = 1;
    readable_[byte_offset].fetch_and(~(x << bit_offset));
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
  auto byte_offset = bucket_ind / 8;
  auto bit_offset = bucket_ind % 8;
  return check_byte_bit(occupied_[byte_offset], bit_offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
  if (!IsOccupied(bucket_ind)) {
    return false;
  }
  auto byte_offset = bucket_ind / 8;
  auto bit_offset = bucket_ind % 8;
  return check_byte_bit(readable_[byte_offset], bit_offset);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
