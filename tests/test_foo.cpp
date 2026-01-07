#include "engine/foo.hpp"
#include "gtest/gtest.h"

TEST(foo, sum_works) {
    EXPECT_EQ(sum(2, 3), 5);
    EXPECT_EQ(sum(1, -1), 0);
}
