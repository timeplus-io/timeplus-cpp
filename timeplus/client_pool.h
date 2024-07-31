#pragma once

#include <timeplus/blocking_queue.h>
#include <timeplus/client.h>
#include <timeplus/exceptions.h>
#include <timeplus/macros.h>

#include <memory>

namespace timeplus {

/// `ClientPool` is a blocking queue to store idle clients and their validness. An invalid client need reconnect before used for other
/// operations.
class ClientPool {
public:
    using ClientPtr = std::unique_ptr<Client>;
    using Clients = BlockingQueue<std::pair<ClientPtr, bool>>;

    ClientPool(ClientOptions client_options, size_t pool_size)
        : client_options_(std::move(client_options)), pool_size_(pool_size), clients_(pool_size_) {
        for (size_t i = 0; i < pool_size_; ++i) {
            clients_.emplace(nullptr, false);
        }
    }

    ~ClientPool() { assert(clients_.size() == pool_size_); }

    ClientPtr Acquire(int64_t timeout_ms);

    void Release(ClientPtr client, bool valid) { clients_.emplace(std::move(client), valid); }

    /// Mostly RAII to return client and its validness to pool on destruction.
    class GuardedClient {
    public:
        GuardedClient() = default;
        GuardedClient(ClientPool* pool, ClientPtr client, bool valid) : client(std::move(client)), valid(valid), pool_(pool) {}

        ~GuardedClient() {
            if (pool_) {
                pool_->Release(std::move(client), valid);
            }
        }

        GuardedClient(GuardedClient&& other) noexcept : client(std::move(other.client)), valid(other.valid), pool_(other.pool_) {
            other.pool_ = nullptr;
        }

        GuardedClient& operator=(GuardedClient&& other) noexcept {
            if (this != &other) {
                if (pool_ && client) {
                    pool_->Release(std::move(client), valid);
                }
                pool_ = other.pool_;
                other.pool_ = nullptr;
                client = std::move(other.client);
                valid = other.valid;
            }
            return *this;
        };

        void TestConnection() noexcept;

        ClientPtr client;
        bool valid;

    private:
        ClientPool* pool_{nullptr};
    };

    GuardedClient GetGuardedClient(int64_t timeout_ms) {
        auto client = Acquire(timeout_ms);
        return GuardedClient{this, std::move(client), true};
    }

private:
    ClientOptions client_options_;
    size_t pool_size_;
    Clients clients_;
};

}  // namespace timeplus
