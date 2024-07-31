#pragma once

#include <timeplus/block.h>
#include <timeplus/client.h>
#include <timeplus/error_codes.h>
#include <timeplus/timeplus_config.h>

#include <functional>
#include <memory>

namespace timeplus {

struct BaseResult {
    virtual ~BaseResult() = default;

    bool ok() const { return err_code == ErrorCodes::OK; }

    int err_code = ErrorCodes::OK;
    std::string err_msg;
};

struct InsertResult : public BaseResult {
    InsertResult(std::string table_name, BlockPtr block, std::string idempotent_id)
        : table_name(std::move(table_name)), block(std::move(block)), idempotent_id(std::move(idempotent_id)) {}

    std::string table_name;
    BlockPtr block;
    std::string idempotent_id;
};

using Callback = std::function<void(const BaseResult&)>;

/// `Timeplus` class provides high-level thread-safe APIs for Timeplus operations.
/// Internally, the class maintains a pool of clients and their connection to the server. For every user request,
/// the idle client is acquired from pool, executes the operation and finally put back to pool.
///
/// The class provides both synchronous and asynchronous APIs for the most operations.
/// It has an internal queue to store the asynchronou requests and starts a number of threads (configured by
/// `TimeplusConfig::task_executors`) to execute asynchronous requests.
class Timeplus {
public:
    explicit Timeplus(TimeplusConfig config);
    ~Timeplus();

    /**
     * Data ingestion APIs
     */

    /// `Insert` methods insert data to the stream in the synchronous way. The methods return the
    /// result when block is successfully insert or excceeds max retry times. User need to check the result
    /// and handle the error; usually resend the same data block again with the same idempotent id.
    InsertResult Insert(std::string table_name, BlockPtr block);
    InsertResult Insert(std::string table_name, BlockPtr block, std::string idempotent_id);

    /// `InsertAsync` methods insert data to the stream asynchronously. The methods store the insert data in
    /// an internal queue and return the insert operation ID to the user. When the insert is completed,
    /// the callback function will be invoked with the insert result to notify the user.
    void InsertAsync(std::string table_name, BlockPtr block, Callback callback) noexcept;
    void InsertAsync(std::string table_name, BlockPtr block, std::string idempotent_id, Callback callback) noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace timeplus
