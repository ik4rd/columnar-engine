#include <cstdint>
#include <limits>
#include <sstream>

#include "gtest/gtest.h"
#include "io/stream.h"

TEST(stream, roundtrip_signed_integrals) {
    std::stringstream buffer;

    WriteStream<int64_t>(buffer, std::numeric_limits<int64_t>::min());
    WriteStream<int64_t>(buffer, -1);
    WriteStream<int64_t>(buffer, 0);
    WriteStream<int64_t>(buffer, std::numeric_limits<int64_t>::max());

    EXPECT_EQ(ReadStream<int64_t>(buffer), std::numeric_limits<int64_t>::min());
    EXPECT_EQ(ReadStream<int64_t>(buffer), -1);
    EXPECT_EQ(ReadStream<int64_t>(buffer), 0);
    EXPECT_EQ(ReadStream<int64_t>(buffer), std::numeric_limits<int64_t>::max());
}
