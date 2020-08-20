//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManager::get_free_page(bool has_latch) -> frame_id_t {
  if (!has_latch) {
    latch_.lock();
  }
  frame_id_t frame_id;
  if (free_list_.empty()) {
    if (!replacer_->Victim(&frame_id)) {
      if (!has_latch) {
        latch_.unlock();
      }
      return -1;
    }
    DeletePageImpl(pages_[frame_id].page_id_, true);
  }
  frame_id = free_list_.back();
  free_list_.pop_back();
  replacer_->Pin(frame_id);
  if (!has_latch) {
    latch_.unlock();
  }
  return frame_id;
}

auto BufferPoolManager::FetchPageImpl(page_id_t page_id, bool has_latch) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if (!has_latch) {
    latch_.lock();
  }
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->Pin(frame_id);
    }
    pages_[frame_id].pin_count_ += 1;
    if (!has_latch) {
      latch_.unlock();
    }
    return &pages_[frame_id];
  }
  auto frame_id = get_free_page(true);
  if (frame_id == -1) {
    if (!has_latch) {
      latch_.unlock();
    }
    return nullptr;
  }
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  page_table_[page_id] = frame_id;
  if (!has_latch) {
    latch_.unlock();
  }
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty, bool has_latch) -> bool {
  if (!has_latch) {
    latch_.lock();
  }
  assert(page_table_.find(page_id) != page_table_.end());
  auto frame_id = page_table_[page_id];
  pages_[frame_id].is_dirty_ = is_dirty || pages_[frame_id].is_dirty_;
  if (pages_[frame_id].GetPinCount() <= 0) {
    if (!has_latch) {
      latch_.unlock();
    }
    return false;
  }
  pages_[frame_id].pin_count_ -= 1;
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->Unpin(frame_id);
  }
  if (!has_latch) {
    latch_.unlock();
  }
  return true;
}

auto BufferPoolManager::FlushPageImpl(page_id_t page_id, bool has_latch) -> bool {
  // Make sure you call DiskManager::WritePage!
  if (!has_latch) {
    latch_.lock();
  }
  assert(page_id != INVALID_PAGE_ID);
  if (page_table_.find(page_id) == page_table_.end()) {
    if (!has_latch) {
      latch_.unlock();
    }
    return false;
  }
  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  }
  if (!has_latch) {
    latch_.unlock();
  }
  return true;
}

auto BufferPoolManager::NewPageImpl(page_id_t *page_id, bool has_latch) -> Page * {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  if (!has_latch) {
    latch_.lock();
  }
  frame_id_t frame_id = get_free_page(true);
  if (frame_id == -1) {
    if (!has_latch) {
      latch_.unlock();
    }
    return nullptr;
  }
  *page_id = disk_manager_->AllocatePage();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();
  page_table_[*page_id] = frame_id;
  if (!has_latch) {
    latch_.unlock();
  }
  return &pages_[frame_id];
}

auto BufferPoolManager::DeletePageImpl(page_id_t page_id, bool has_latch) -> bool {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (!has_latch) {
    latch_.lock();
  }
  if (page_table_.find(page_id) == page_table_.end()) {
    if (!has_latch) {
      latch_.unlock();
    }
    return true;
  }
  if (pages_[page_table_[page_id]].GetPinCount() != 0) {
    if (!has_latch) {
      latch_.unlock();
    }
    return false;
  }
  disk_manager_->DeallocatePage(page_id);
  FlushPageImpl(page_id, true);
  auto frame_id = page_table_[page_id];
  page_table_.erase(page_id);
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  free_list_.push_back(frame_id);
  if (!has_latch) {
    latch_.unlock();
  }
  return true;
}

void BufferPoolManager::FlushAllPagesImpl(bool has_latch) {
  if (!has_latch) {
    latch_.lock();
  }
  for (auto &iter : page_table_) {
    auto page_id = iter.first;
    FlushPageImpl(page_id, true);
  }
  if (!has_latch) {
    latch_.unlock();
  }
}

}  // namespace bustub
