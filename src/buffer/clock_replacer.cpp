//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages)
    : clock_vector(decltype(clock_vector)(num_pages)),
      ClockReplacer_frame_counter(0),
      clock_hand(clock_vector.begin()) {}

ClockReplacer::~ClockReplacer() = default;

void ClockReplacer::move_clock_hand() {
  if (clock_hand + 1 == clock_vector.end()) {
    clock_hand = clock_vector.begin();
  } else {
    clock_hand += 1;
  }
}

auto ClockReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lockGuard(latch_);
  if (ClockReplacer_frame_counter == 0) {
    return false;
  }
  while (true) {
    if ((!clock_hand->pin_flag) && (!clock_hand->ref_flag)) {
      *frame_id = clock_hand - clock_vector.begin();
      if (!((clock_vector.begin() + *frame_id)->pin_flag)) {
        ClockReplacer_frame_counter -= 1;
      }
      (clock_vector.begin() + *frame_id)->pin_flag = true;
      (clock_vector.begin() + *frame_id)->ref_flag = true;
      break;
    }
    if ((!clock_hand->pin_flag) && (clock_hand->ref_flag)) {
      clock_hand->ref_flag = false;
      move_clock_hand();
    } else {
      move_clock_hand();
    }
  }
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lockGuard(latch_);
  if (!((clock_vector.begin() + frame_id)->pin_flag)) {
    ClockReplacer_frame_counter -= 1;
  }
  (clock_vector.begin() + frame_id)->pin_flag = true;
  (clock_vector.begin() + frame_id)->ref_flag = true;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lockGuard(latch_);
  if (((clock_vector.begin() + frame_id)->pin_flag)) {
    ClockReplacer_frame_counter += 1;
  }
  (clock_vector.begin() + frame_id)->pin_flag = false;
}

auto ClockReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lockGuard(latch_);
  return ClockReplacer_frame_counter;
}

}  // namespace bustub
