#include <timeplus/client_pool.h>
#include <timeplus/exceptions.h>

#include <gtest/gtest.h>

using namespace timeplus;

TEST(ClientPool, AcquireClient) {
    ClientOptions client_options;
    client_options.host = "localhost";
    ClientPool pool{client_options, /*pool_size=*/3};

    auto client1 = pool.Acquire(1000);
    ASSERT_NE(client1, nullptr);

    auto client2 = pool.Acquire(1000);
    ASSERT_NE(client2, nullptr);

    auto client3 = pool.Acquire(1000);
    ASSERT_NE(client3, nullptr);

    ASSERT_THROW(pool.Acquire(100), TimeoutError);

    pool.Release(std::move(client1), /*valid=*/true);
    auto client5 = pool.Acquire(1000);
    ASSERT_EQ(client1, nullptr);
    ASSERT_NE(client5, nullptr);

    pool.Release(std::move(client2), /*valid=*/false);
    auto client6 = pool.Acquire(1000);
    ASSERT_EQ(client2, nullptr);
    ASSERT_NE(client6, nullptr);

    pool.Release(std::move(client3), true);
    pool.Release(std::move(client5), true);
    pool.Release(std::move(client6), true);
}

TEST(ClientPool, BadHost) {
    ClientOptions client_options;
    client_options.host = "badhost";
    client_options.retry_timeout = std::chrono::seconds(0);
    ClientPool pool{client_options, /*pool_size=*/3};

    ASSERT_THROW(pool.Acquire(1000), std::system_error);
}
