#include <timeplus/timeplus.h>


namespace timeplus
{

class Timeplus::Impl
{
public:
    explicit Impl(TimeplusConfig config) : client_options(std::move(config.client_options)), client(client_options) { }

    uint64_t Insert(std::string table, Block block, std::string idempotent_id, InsertCallback callback)
    {
        if (idempotent_id.empty())
            client.Insert(table, block);
        else
            client.Insert(table, block, idempotent_id);

        InsertResult res;
        callback(0, res);
        return 0;
    }

private:
    ClientOptions client_options;
    Client client;
};

Timeplus::Timeplus(TimeplusConfig config) : impl(std::make_unique<Impl>(std::move(config)))
{
}

Timeplus::~Timeplus() = default;

void Timeplus::InsertSync(std::string table, Block block)
{
    InsertSync(std::move(table), std::move(block), "");
}

void Timeplus::InsertSync(std::string table, Block block, std::string idempotent_id)
{
    std::optional<InsertResult> maybe_result;
    std::mutex mtx;
    std::condition_variable cv;

    impl->Insert(
        std::move(table), std::move(block), std::move(idempotent_id), [&maybe_result, &mtx, &cv](uint64_t, const auto & insert_result) {
            {
                const std::lock_guard<std::mutex> lk(mtx);
                maybe_result = insert_result;
            }
            cv.notify_one();
        });

    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [&maybe_result] { return maybe_result.has_value(); });

    if (!maybe_result->ok())
        throw;
}

uint64_t Timeplus::Insert(std::string table, Block block, InsertCallback callback) noexcept
{
    return impl->Insert(std::move(table), std::move(block), "", std::move(callback));
}

uint64_t Timeplus::Insert(std::string table, const Block block, std::string idempotent_id, InsertCallback callback) noexcept
{
    return impl->Insert(std::move(table), std::move(block), std::move(idempotent_id), std::move(callback));
}

}
