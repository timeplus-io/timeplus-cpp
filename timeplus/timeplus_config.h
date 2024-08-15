#pragma once

#include <timeplus/client.h>

namespace timeplus {

struct TimeplusConfig {
    /// Options used in creating internal clients. Some of the options may be overwritten for better performance.
    ClientOptions client_options;

    /// Max number of connections maintained in pool.
    size_t max_connections = 1;
    /// Max waiting time to acquire a idle connection from pool.
    int64_t client_acquire_timeout_ms = 10000;

    /// Number of retries before retriable error returned to user.
    uint32_t max_retries = 5;
    /// Amount of time to wait before next retry.
    int64_t wait_time_before_retry_ms = 5000;

    /// Capacity of the queue to store asynchronous requests.
    size_t task_queue_capacity = 128;
    /// Number of threads to execute asynchronous requests.
    size_t task_executors = 1;
};

}  // namespace timeplus
