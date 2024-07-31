#include <timeplus/timeplus.h>

#include <timeplus/client_pool.h>
#include <timeplus/macros.h>

#include <thread>

namespace timeplus {

class Timeplus::Impl {
public:
    explicit Impl(TimeplusConfig&& config)
        : config_(std::move(config)), client_pool_(CreateClientOptions(), config_.max_connections), tasks_(config_.task_queue_capacity) {
        /// Start executors for asynchronous tasks execution.
        task_executors_.reserve(config_.task_executors);
        for (size_t i = 0; i < config_.task_executors; ++i) {
            task_executors_.emplace_back(&Impl::TaskExecutionFunc, this);
        }
    }

    ~Impl() {
        {
            std::lock_guard lk{tasks_mutex_};
            tasks_stopped_ = true;
        }
        tasks_cv_.notify_all();

        for (auto& exec : task_executors_) {
            if (exec.joinable()) {
                exec.join();
            }
        }
    }

    InsertResult Insert(std::string table_name, BlockPtr block, std::string idempotent_id) {
        InsertResult result{std::move(table_name), std::move(block), std::move(idempotent_id)};
        ExecuteWithRetries([&result](Client& client) { client.Insert(result.table_name, *result.block, result.idempotent_id); }, result);
        return result;
    }

    void InsertAsync(std::string&& table_name, BlockPtr&& block, std::string&& idempotent_id, Callback&& callback) {
        auto task = std::make_shared<InsertTask>(std::move(table_name), std::move(block), std::move(idempotent_id), std::move(callback));
        tasks_.add(std::move(task));
        tasks_cv_.notify_all();
    }

private:
    ClientOptions CreateClientOptions() {
        ClientOptions client_options = config_.client_options;
        client_options.rethrow_exceptions = true;
        client_options.ping_before_query = false;
        return client_options;
    }

    void ExecuteWithRetries(std::function<void(Client&)> func, BaseResult& result) {
        ClientPool::GuardedClient guarded_client;
        for (int retries = config_.max_retries; retries >= 0; --retries) {
            try {
                if (guarded_client.client == nullptr) {
                    guarded_client = client_pool_.GetGuardedClient(config_.client_acquire_timeout_ms);
                }

                if (!guarded_client.valid) {
                    /// Invalid client need to reconnect before using.
                    guarded_client.client->ResetConnectionEndpoint();
                    guarded_client.valid = true;
                }

                func(*guarded_client.client);

                return;
            } catch (const ProtocolError& ex) {
                /// Client failed to communicate with server.
                guarded_client.TestConnection();
                result.err_code = ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER;
                result.err_msg = ex.what();
            } catch (const ServerError& ex) {
                guarded_client.TestConnection();
                result.err_code = ex.GetCode();
                result.err_msg = ex.what();
                switch (ex.GetCode()) {
                    case ErrorCodes::TIMEOUT_EXCEEDED:
                    case ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT:
                        break;
                    default:
                        /// Non-retriable exceptions returned from server
                        return;
                }
            } catch (const std::system_error& ex) {
                /// Client can not connect to server.
                guarded_client.valid = false;
                result.err_code = ErrorCodes::NETWORK_ERROR;
                result.err_msg = std::string("Failed to send request to server: ") + ex.what();
            } catch (const std::exception& ex) {
                /// Other non-retriabl exceptions
                guarded_client.TestConnection();
                result.err_code = ErrorCodes::UNKNOWN_EXCEPTION;
                result.err_msg = ex.what();
                return;
            }
            TRACE("execution failed: code=%d msg=%s remaining_retries=%d", result.err_code, result.err_msg.c_str(), retries);
            /// Sleep before next retry.
            std::this_thread::sleep_for(std::chrono::milliseconds(config_.wait_time_before_retry_ms));
        }
    }

    void TaskExecutionFunc() {
        while (true) {
            {
                std::unique_lock lk{tasks_mutex_};
                tasks_cv_.wait(lk, [this] { return !tasks_.empty() || tasks_stopped_; });
                if (tasks_stopped_) {
                    return;
                }
            }

            auto maybe_task = tasks_.take(1000);
            if (!maybe_task.has_value()) {
                continue;
            }

            auto& task = maybe_task.value();
            switch (task->type()) {
                case TaskType::Insert: {
                    auto insert_task = std::static_pointer_cast<InsertTask>(task);
                    auto result =
                        Insert(std::move(insert_task->table_name), std::move(insert_task->block), std::move(insert_task->idempotent_id));
                    insert_task->callback(result);
                    break;
                }
            }
        }
    }

    TimeplusConfig config_;
    ClientPool client_pool_;

    /**
     *Asynchronous task members
     */

    enum class TaskType {
        Insert,
    };

    struct Task {
        explicit Task(Callback callback) : callback(std::move(callback)) {}
        virtual ~Task() = default;

        virtual TaskType type() const = 0;
        Callback callback;
    };

    struct InsertTask : public Task {
        InsertTask(std::string table_name, BlockPtr block, std::string idempotent_id, Callback callback)
            : Task(std::move(callback)),
              table_name(std::move(table_name)),
              block(std::move(block)),
              idempotent_id(std::move(idempotent_id)) {}

        TaskType type() const override { return TaskType::Insert; }

        std::string table_name;
        BlockPtr block;
        std::string idempotent_id;
    };

    BlockingQueue<std::shared_ptr<Task>> tasks_;
    std::vector<std::thread> task_executors_;
    std::atomic<uint64_t> next_task_id_{0};

    mutable std::mutex tasks_mutex_;
    std::condition_variable tasks_cv_;
    bool tasks_stopped_{false};
};

Timeplus::Timeplus(TimeplusConfig config) : impl_(std::make_unique<Impl>(std::move(config))) {
}

Timeplus::~Timeplus() = default;

InsertResult Timeplus::Insert(std::string table_name, BlockPtr block) {
    return impl_->Insert(std::move(table_name), std::move(block), /*idempotent_id=*/"");
}

InsertResult Timeplus::Insert(std::string table_name, BlockPtr block, std::string idempotent_id) {
    return impl_->Insert(std::move(table_name), std::move(block), std::move(idempotent_id));
}

void Timeplus::InsertAsync(std::string table_name, BlockPtr block, Callback callback) noexcept {
    impl_->InsertAsync(std::move(table_name), std::move(block), /*idempotent_id=*/"", std::move(callback));
}

void Timeplus::InsertAsync(std::string table_name, BlockPtr block, std::string idempotent_id, Callback callback) noexcept {
    impl_->InsertAsync(std::move(table_name), std::move(block), std::move(idempotent_id), std::move(callback));
}

}  // namespace timeplus
