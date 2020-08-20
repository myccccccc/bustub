//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/clock_replacer.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManager {
 public:
  enum class CallbackType { BEFORE, AFTER };
  using bufferpool_callback_fn = void (*)(enum CallbackType, const page_id_t page_id);

  /**
   * Creates a new BufferPoolManager.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable logging)
   */
  BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager = nullptr);

  /**
   * Destroys an existing BufferPoolManager.
   */
  ~BufferPoolManager();

  /** Grading function. Do not modify! */
  auto FetchPage(page_id_t page_id, bufferpool_callback_fn callback = nullptr) -> Page * {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto *result = FetchPageImpl(page_id);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  auto UnpinPage(page_id_t page_id, bool is_dirty, bufferpool_callback_fn callback = nullptr) -> bool {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto result = UnpinPageImpl(page_id, is_dirty);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  auto FlushPage(page_id_t page_id, bufferpool_callback_fn callback = nullptr) -> bool {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto result = FlushPageImpl(page_id);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  auto NewPage(page_id_t *page_id, bufferpool_callback_fn callback = nullptr) -> Page * {
    GradingCallback(callback, CallbackType::BEFORE, INVALID_PAGE_ID);
    auto *result = NewPageImpl(page_id);
    GradingCallback(callback, CallbackType::AFTER, *page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  auto DeletePage(page_id_t page_id, bufferpool_callback_fn callback = nullptr) -> bool {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto result = DeletePageImpl(page_id);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  void FlushAllPages(bufferpool_callback_fn callback = nullptr) {
    GradingCallback(callback, CallbackType::BEFORE, INVALID_PAGE_ID);
    FlushAllPagesImpl();
    GradingCallback(callback, CallbackType::AFTER, INVALID_PAGE_ID);
  }

  /** @return pointer to all the pages in the buffer pool */
  auto GetPages() -> Page * { return pages_; }

  /** @return size of the buffer pool */
  auto GetPoolSize() const -> size_t { return pool_size_; }

 protected:
  /**
   * Grading function. Do not modify!
   * Invokes the callback function if it is not null.
   * @param callback callback function to be invoked
   * @param callback_type BEFORE or AFTER
   * @param page_id the page id to invoke the callback with
   */
  static void GradingCallback(bufferpool_callback_fn callback, CallbackType callback_type, page_id_t page_id) {
    if (callback != nullptr) {
      callback(callback_type, page_id);
    }
  }

  /**
   * Fetch the requested page from the buffer pool.
   * @param page_id id of page to be fetched
   * @return the requested page
   */
  auto FetchPageImpl(page_id_t page_id, bool has_latch = false) -> Page *;

  /**
   * Unpin the target page from the buffer pool.
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page pin count is <= 0 before this call, true otherwise
   */
  auto UnpinPageImpl(page_id_t page_id, bool is_dirty, bool has_latch = false) -> bool;

  /**
   * Flushes the target page to disk.
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  auto FlushPageImpl(page_id_t page_id, bool has_latch = false) -> bool;

  /**
   * Creates a new page in the buffer pool.
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  auto NewPageImpl(page_id_t *page_id, bool has_latch = false) -> Page *;

  /**
   * Deletes a page from the buffer pool.
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  auto DeletePageImpl(page_id_t page_id, bool has_latch = false) -> bool;

  /**
   * Flushes all the pages in the buffer pool to disk.
   */
  void FlushAllPagesImpl(bool has_latch = false);

  /** Number of pages in the buffer pool. */
  size_t pool_size_;
  /** Array of buffer pool pages. */
  Page *pages_;
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));
  /** Pointer to the log manager. */
  LogManager *log_manager_ __attribute__((__unused__));
  /** Page table for keeping track of buffer pool pages. */
  std::unordered_map<page_id_t, frame_id_t> page_table_;
  /** Replacer to find unpinned pages for replacement. */
  Replacer *replacer_;
  /** List of free pages. */
  std::list<frame_id_t> free_list_;
  /** This latch protects shared data structures. We recommend updating this comment to describe what it protects. */
  std::mutex latch_;

 private:
  /**
   * Get a free page from free_list_ or replacer_, this func is used by NewPageImpl and FetchPageImpl
   * @return frame_id id of the free page that we found, -1 otherwise
   */
  auto get_free_page(bool has_latch = false) -> frame_id_t;
};
}  // namespace bustub
