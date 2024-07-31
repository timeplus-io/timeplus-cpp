#include <timeplus/inserter.h>


namespace timeplus
{

class Inserter::Impl
{
public:
    explicit Impl(Options options_) : options(std::move(options_)), client(options.client_options) { }

    void Insert(InsertData data, InsertCallback callback)
    {
        if (data.idempotent)
            client.Insert(data.table_name, data.block, /*idempotent_key=*/data.id);
        else
            client.Insert(data.table_name, data.block);

        InsertResult res;
        res.id = data.id;
        callback(res);
    }

private:
    Options options;
    Client client;
};

Inserter::Inserter(Options options) : impl(std::make_unique<Impl>(std::move(options)))
{
}

Inserter::~Inserter() = default;

void Inserter::Insert(InsertData data, InsertCallback callback)
{
    impl->Insert(std::move(data), std::move(callback));
}

InsertResult Inserter::InsertSync(InsertData data)
{
    std::optional<InsertResult> maybe_result;
    std::mutex mtx;
    std::condition_variable cv;

    impl->Insert(std::move(data), [&maybe_result, &mtx, &cv](const InsertResult & result) {
        const std::lock_guard<std::mutex> guard(mtx);
        maybe_result = result;
        cv.notify_one();
    });

    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [&maybe_result] { return maybe_result.has_value(); });

    return std::move(maybe_result).value();
}
}
