#include <string>
#include <iostream>

#include "rocksdb/db.h"


namespace {

ROCKSDB_NAMESPACE::Status CalcKeyRanges(
                                    ROCKSDB_NAMESPACE::DB* db,
                                    ROCKSDB_NAMESPACE::ColumnFamilyHandle* column_family,
                                    size_t num_ranges,
                                    std::vector<std::string>& range_split_keys)
{
  if (num_ranges == 1 || num_ranges == 0) {
    return ROCKSDB_NAMESPACE::Status();
  }

  ROCKSDB_NAMESPACE::ColumnFamilyDescriptor cf_desc;
  ROCKSDB_NAMESPACE::Status s = column_family->GetDescriptor(&cf_desc);
  if (!s.ok()) {
    return s;
  }

  ROCKSDB_NAMESPACE::ColumnFamilyMetaData cf_metadata;
  db->GetColumnFamilyMetaData(column_family, &cf_metadata);
  auto range_average_size = cf_metadata.size / num_ranges;

  std::vector<
      std::pair<std::vector<ROCKSDB_NAMESPACE::SstFileMetaData>::const_iterator, size_t>> per_level_files;

  // Create vector of level iterators. Each level entry holds a pair of iterators -
  // the begin and the end iterators of the files vector (L0 files are excluded)
  for (auto& level: cf_metadata.levels) {
    if (level.level != 0 && level.files.size()) {
      per_level_files.push_back(std::pair(level.files.begin(), level.files.size()));
    }
  }

  std::vector<size_t> range_sizes;

  // Calculate ranges
  for (size_t i = 0; i < num_ranges && per_level_files.size(); i++) {

    size_t range_size = 0;
    std::string range_max_key;

    // Collect files into the range till we pass the range_average_size
    while (range_size < range_average_size) {

      // Remove entries with exhausted iterators
      per_level_files.erase(
        std::remove_if(per_level_files.begin(), per_level_files.end(),
          [](const auto& param) -> bool { return !param.second; }),
        per_level_files.end()
      );

      if (!per_level_files.size()) {
          break;
      }

      // Find file with the next minimal largest key, so we expand the range with smallest step
      auto file_with_min_largestkey = std::min_element(per_level_files.begin(), per_level_files.end(),
        [&](const auto& arg1, const auto& arg2) -> bool {
          return (0 > cf_desc.options.comparator->Compare(arg1.first->largestkey, arg2.first->largestkey));
        }
      );

      range_max_key = (*file_with_min_largestkey).first->largestkey;
      range_size += (*file_with_min_largestkey).first->size;
      (*file_with_min_largestkey).first++;
      (*file_with_min_largestkey).second--;
    }

    // Add new range if we found one
    if (range_size) {
      range_split_keys.push_back(range_max_key);
      range_sizes.push_back(range_size);
    }
  }

  // Removing the last key, because it is the last range end point
  if (range_split_keys.size()) {
    range_split_keys.pop_back();
  }

  return ROCKSDB_NAMESPACE::Status();
}
}
