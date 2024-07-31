#include <timeplus/client_pool.h>

namespace timeplus {

ClientPool::ClientPtr ClientPool::Acquire(int64_t timeout_ms) {
    auto maybe_client = clients_.take(timeout_ms);
    if (!maybe_client.has_value()) {
        throw TimeoutError("Can not acquire client before timeout");
    }

    auto [client, valid] = std::move(maybe_client).value();
    try {
        /// Lazy init client
        if (client == nullptr) {
            client = std::make_unique<Client>(client_options_);
            valid = true;
        }

        if (!valid) {
            client->ResetConnectionEndpoint();
            valid = true;
        }
    } catch (const std::exception& ex) {
        /// Client can not connect to server.
        Release(std::move(client), false);
        throw;
    }

    return std::move(client);
}

void ClientPool::GuardedClient::TestConnection() noexcept {
    try {
        client->Ping();
        valid = true;
    } catch (...) {
        valid = false;
    }

    TRACE("test connection: host=%s port=%d valid=%s", client->GetCurrentEndpoint()->host.c_str(), client->GetCurrentEndpoint()->port,
          valid ? "true" : "false");
}

}  // namespace timeplus
