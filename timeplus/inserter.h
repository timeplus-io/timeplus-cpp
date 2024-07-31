#pragma once

#include <timeplus/block.h>
#include <timeplus/client.h>
#include <timeplus/error_codes.h>

#include <functional>
#include <memory>


namespace timeplus
{

class InsertData
{
public:
    InsertData(std::string id_, bool idempotent_, std::string table_name_, Block block_)
        : id(std::move(id_)), idempotent(idempotent_), table_name(std::move(table_name_)), block(std::move(block_))
    {
    }

    /// The unique ID to identify the insert data. For idempotent insert, the ID is used as idempotent key.
    std::string id;
    bool idempotent;

    std::string table_name;
    Block block;
};

struct InsertResult
{
    /// `InsertData` ID
    std::string id;

    ErrorCodes err_code = ErrorCodes::OK;
    std::string err_msg;
};

using InsertCallback = std::function<void(const InsertResult &)>;

/// `Inserter` is a high-level class used to ingest data. It internally has a queue to store the ingestion data
///  and managed one or multiples `Client` to send insert queries.
/// `Inserter` could be sharely used by multiple threads and its methods are thread safe.
class Inserter
{
public:
    class Options
    {
    public:
        explicit Options(ClientOptions client_options_, int max_concurrency_ = 1)
            : client_options(std::move(client_options_)), max_concurrency(max_concurrency_)
        {
        }

        ClientOptions client_options;
        int max_concurrency;
    };

    explicit Inserter(Options options);
    ~Inserter();

    Inserter(const Inserter &) = delete;
    Inserter & operator=(const Inserter &) = delete;
    Inserter(Inserter &&) noexcept = delete;
    Inserter & operator=(Inserter &&) noexcept = delete;

    InsertResult InsertSync(InsertData data);
    void Insert(InsertData data, InsertCallback callback);

private:
    class Impl;
    std::unique_ptr<Impl> impl;
};

}
