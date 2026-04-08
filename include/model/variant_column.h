/* (What is this? — База для колонок переменной длины) */

#pragma once

#include <limits>
#include <utility>
#include <vector>

#include "column.h"
#include "error.h"
#include "stream.h"

template <class Derived, ColumnType TypeValue>
class VariantColumn : public Column {
   public:
    VariantColumn() : Column(TypeValue) {}
    VariantColumn(const VariantColumn&) = default;
    VariantColumn(VariantColumn&&) noexcept = default;
    VariantColumn& operator=(const VariantColumn&) = default;
    VariantColumn& operator=(VariantColumn&&) noexcept = default;
    ~VariantColumn() override = default;

    size_t Size() const override { return values_.size(); }
    void Reserve(const size_t n) override { values_.reserve(n); }
    void Clear() override { values_.clear(); }

    uint64_t EstimateSizeFromString(const std::string_view value) const override { return value.size(); }

    std::unique_ptr<Column> Clone() const override {
        return std::make_unique<Derived>(static_cast<const Derived&>(*this));
    }

    void WriteTo(std::ostream& out) const override {
        if constexpr (std::endian::native == std::endian::little) {
            size_t total_size = 0;
            for (const auto& value : values_) {
                if (value.size() > std::numeric_limits<uint32_t>::max()) {
                    throw Error::Overflow(Derived::ModuleName(), "value exceeds supported size");
                }
                total_size += sizeof(uint32_t) + value.size();
            }

            std::string buffer(total_size, '\0');
            char* dst = buffer.data();

            for (const auto& value : values_) {
                const uint32_t length = value.size();
                std::memcpy(dst, &length, sizeof(length));
                dst += sizeof(length);

                if (!value.empty()) {
                    std::memcpy(dst, value.data(), value.size());
                    dst += value.size();
                }
            }

            WriteBytes(out, buffer);
            return;
        }

        for (const auto& value : values_) {
            if (value.size() > std::numeric_limits<uint32_t>::max()) {
                throw Error::Overflow(Derived::ModuleName(), "value exceeds supported size");
            }
            WriteStream<uint32_t>(out, value.size());
            WriteBytes(out, value);
        }
    }

    void ReadFrom(std::istream& in, const uint32_t row_count, const uint64_t size) override {
        values_.clear();
        values_.reserve(row_count);

        uint64_t consumed = 0;
        for (uint32_t row_index = 0; row_index < row_count; ++row_index) {
            const uint32_t length = ReadStream<uint32_t>(in);
            consumed += sizeof(uint32_t);

            std::string value(length, '\0');
            ReadBytes(in, value.data(), value.size());

            consumed += length;
            values_.push_back(std::move(value));
        }

        if (consumed != size) {
            throw Error::Mismatch(Derived::ModuleName(), "column chunk size mismatch");
        }
    }

   protected:
    void AppendValue(std::string value) { values_.push_back(std::move(value)); }

    const std::string& ValueAt(const size_t row) const {
        CheckRowIndex(Derived::ModuleName(), row, values_.size());
        return values_[row];
    }

    std::vector<std::string> values_;
};
