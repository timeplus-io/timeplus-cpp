#pragma once

#include <timeplus/block.h>
#include <timeplus/client.h>
#include <timeplus/error_codes.h>

#include <functional>
#include <memory>


namespace timeplus
{

struct TimeplusConfig
{
    ClientOptions client_options;
    int max_concurrency;
};

struct BaseResult
{
    bool ok() const { return err_code == ErrorCodes::OK; }

    ErrorCodes err_code = ErrorCodes::OK;
    std::string err_msg;
};

using InsertResult = BaseResult;

/// `Timeplus` is a high-level class wraps the  to ingest data. It internally has a queue to store the ingestion data
///  and managed one or multiples `Client` to send insert queries.
class Timeplus
{
public:
    explicit Timeplus(TimeplusConfig config);
    ~Timeplus();

    Timeplus(const Timeplus &) = delete;
    Timeplus & operator=(const Timeplus &) = delete;
    Timeplus(Timeplus &&) noexcept = delete;
    Timeplus & operator=(Timeplus &&) noexcept = delete;

    void InsertSync(std::string table, Block block);
    void InsertSync(std::string table, Block block, std::string idempotent_id);

    using InsertCallback = std::function<void(uint64_t id, const InsertResult &)>;
    uint64_t Insert(std::string table, Block block, InsertCallback callback) noexcept;
    uint64_t Insert(std::string table, Block block, std::string idempotent_id, InsertCallback callback) noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl;
};

}
