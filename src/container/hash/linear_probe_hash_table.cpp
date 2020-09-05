//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/linear_probe_hash_table.h"

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::get_bucket_id_value(u_int64_t prob, const KeyType &key, std::vector<ValueType> *result) {
  auto *hp = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
  auto block_page_id = hp->GetBlockPageId(prob / BLOCK_ARRAY_SIZE);
  auto *bp = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->FetchPage(block_page_id)->GetData());
  auto slot_index = prob % BLOCK_ARRAY_SIZE;
  if (!bp->IsOccupied(slot_index)) {
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    buffer_pool_manager_->UnpinPage(block_page_id, false);
    return false;
  }
  if (bp->IsReadable(slot_index) && comparator_(key, bp->KeyAt(slot_index)) == 0) {
    result->push_back(bp->ValueAt(slot_index));
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  buffer_pool_manager_->UnpinPage(block_page_id, false);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
int HASH_TABLE_TYPE::insert_bucket_id_kv(u_int64_t prob, const KeyType &key, const ValueType &value) {
  auto *hp = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
  auto block_page_id = hp->GetBlockPageId(prob / BLOCK_ARRAY_SIZE);
  auto *bp = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->FetchPage(block_page_id)->GetData());
  auto slot_index = prob % BLOCK_ARRAY_SIZE;
  if (bp->IsReadable(slot_index) && comparator_(key, bp->KeyAt(slot_index)) == 0 && bp->ValueAt(slot_index) == value) {
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    buffer_pool_manager_->UnpinPage(block_page_id, false);
    return 0;
  }
  if (bp->Insert(slot_index, key, value)) {
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    buffer_pool_manager_->UnpinPage(block_page_id, true);
    return 1;
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  buffer_pool_manager_->UnpinPage(block_page_id, false);
  return -1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
int HASH_TABLE_TYPE::remove_bucket_id_kv(u_int64_t prob, const KeyType &key, const ValueType &value) {
  auto *hp = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
  auto block_page_id = hp->GetBlockPageId(prob / BLOCK_ARRAY_SIZE);
  auto *bp = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->FetchPage(block_page_id)->GetData());
  auto slot_index = prob % BLOCK_ARRAY_SIZE;
  if (!bp->IsOccupied(slot_index)) {
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    buffer_pool_manager_->UnpinPage(block_page_id, false);
    return -1;
  }
  if (bp->IsReadable(slot_index) && comparator_(key, bp->KeyAt(slot_index)) == 0 && bp->ValueAt(slot_index) == value) {
    bp->Remove(slot_index);
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    buffer_pool_manager_->UnpinPage(block_page_id, true);
    return 1;
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  buffer_pool_manager_->UnpinPage(block_page_id, false);
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  auto *hp = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_)->GetData());
  *hp = HashTableHeaderPage();
  hp->SetPageId(header_page_id_);
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
  Resize(num_buckets / 2 + 1);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  auto bucket_id = hash_fn_.GetHash(key) % GetSize();
  auto prob = bucket_id;

  do {
    if (!get_bucket_id_value(prob, key, result)) {
      break;
    }
    prob = (prob + 1) % GetSize();
  } while (prob != bucket_id);

  table_latch_.RUnlock();
  return !(result->empty());
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
start:
  table_latch_.RLock();

  auto bucket_id = hash_fn_.GetHash(key) % GetSize();
  auto prob = bucket_id;
  int status;  // -1代表没有插入，1代表成功插入，0代表该kv已经存在
  do {
    status = insert_bucket_id_kv(prob, key, value);
    if (status != -1) {
      break;
    }
    prob = (prob + 1) % GetSize();
  } while (prob != bucket_id);
  if (status == -1) {
    table_latch_.RUnlock();
    Resize(GetSize());
    goto start;
  }

  table_latch_.RUnlock();
  return status == 1;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  auto bucket_id = hash_fn_.GetHash(key) % GetSize();
  auto prob = bucket_id;
  int status;  // -1代表找不到该kv，0代表还未找到该kv，1代表找到该kv并删除
  do {
    status = remove_bucket_id_kv(prob, key, value);
    if (status != 0) {
      break;
    }
    prob = (prob + 1) % GetSize();
  } while (prob != bucket_id);

  table_latch_.RUnlock();
  return status == 1;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  table_latch_.WLock();

  page_id_t new_hp_id;
  auto *new_hp = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&new_hp_id)->GetData());
  new_hp->SetPageId(new_hp_id);
  new_hp->SetSize(2 * initial_size);
  while (new_hp->NumBlocks() * BLOCK_ARRAY_SIZE < new_hp->GetSize()) {
    page_id_t new_block_page_id;
    auto *new_bp =
        reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->NewPage(&new_block_page_id)->GetData());
    new_bp->Clear();
    new_hp->AddBlockPageId(new_block_page_id);
    buffer_pool_manager_->UnpinPage(new_block_page_id, true);
  }
  buffer_pool_manager_->UnpinPage(new_hp_id, true);

  page_id_t old_hp_id = header_page_id_;
  header_page_id_ = new_hp_id;
  auto *old_hp = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(old_hp_id)->GetData());
  for (decltype(old_hp->NumBlocks()) i = 0; i < old_hp->NumBlocks(); i++) {
    auto old_bp_id = old_hp->GetBlockPageId(i);
    auto *old_bp = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->FetchPage(old_hp_id));
    for (decltype(BLOCK_ARRAY_SIZE) j = 0; j < BLOCK_ARRAY_SIZE; j++) {
      if (old_bp->IsReadable(j)) {
        auto key = old_bp->KeyAt(j);
        auto value = old_bp->ValueAt(j);
        auto bucket_id = hash_fn_.GetHash(key) % GetSize();
        auto prob = bucket_id;
        int status;  // -1代表没有插入，1代表成功插入，0代表该kv已经存在
        do {
          status = insert_bucket_id_kv(prob, key, value);
          prob = (prob + 1) % GetSize();
        } while (status == -1);
      }
    }
    buffer_pool_manager_->UnpinPage(old_bp_id, false);
    buffer_pool_manager_->DeletePage(old_bp_id);
  }
  buffer_pool_manager_->UnpinPage(old_hp_id, false);
  buffer_pool_manager_->DeletePage(old_hp_id);

  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  auto *hp = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
  auto size = hp->NumBlocks() * BLOCK_ARRAY_SIZE;
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return size;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
