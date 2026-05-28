#include "common/string_pattern_utils.h"

#include <string_view>
#include <vector>

constexpr char LikeWildcardAny = '%';
constexpr char LikeWildcardSingle = '_';

static bool LikeMatchesPercentOnly(const std::string_view value, const std::string_view pattern) {
    std::vector<std::string_view> segments;

    for (size_t pos = 0; pos < pattern.size();) {
        while (pos < pattern.size() && pattern[pos] == LikeWildcardAny) {
            ++pos;
        }

        const size_t start = pos;
        while (pos < pattern.size() && pattern[pos] != LikeWildcardAny) {
            ++pos;
        }

        if (start != pos) {
            segments.push_back(pattern.substr(start, pos - start));
        }
    }

    if (segments.empty()) {
        return true;
    }

    size_t value_pos = 0;
    size_t segment_index = 0;

    if (pattern.front() != LikeWildcardAny) {
        if (!value.starts_with(segments.front())) {
            return false;
        }
        value_pos = segments.front().size();
        segment_index = 1;
    }

    for (; segment_index < segments.size(); ++segment_index) {
        const std::string_view segment = segments[segment_index];
        const size_t found = value.find(segment, value_pos);
        if (found == std::string_view::npos) {
            return false;
        }
        value_pos = found + segment.size();
    }

    return pattern.back() == LikeWildcardAny || value.ends_with(segments.back());
}

static bool LikeMatchesWithUnderscore(const std::string_view value, const std::string_view pattern) {
    size_t value_pos = 0;
    size_t pattern_pos = 0;
    size_t last_percent = std::string_view::npos;
    size_t retry_value_pos = 0;

    while (value_pos < value.size()) {
        if (pattern_pos < pattern.size() &&
            (pattern[pattern_pos] == LikeWildcardSingle || pattern[pattern_pos] == value[value_pos])) {
            ++value_pos;
            ++pattern_pos;
        } else if (pattern_pos < pattern.size() && pattern[pattern_pos] == LikeWildcardAny) {
            last_percent = pattern_pos++;
            retry_value_pos = value_pos;
        } else if (last_percent != std::string_view::npos) {
            pattern_pos = last_percent + 1;
            value_pos = ++retry_value_pos;
        } else {
            return false;
        }
    }

    while (pattern_pos < pattern.size() && pattern[pattern_pos] == LikeWildcardAny) {
        ++pattern_pos;
    }

    return pattern_pos == pattern.size();
}

bool LikeMatches(const std::string_view value, const std::string_view pattern) {
    if (pattern.find(LikeWildcardSingle) == std::string_view::npos) {
        return LikeMatchesPercentOnly(value, pattern);
    }

    return LikeMatchesWithUnderscore(value, pattern);
}
