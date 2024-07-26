#include <benchmark/benchmark.h>

#include <timeplus/client.h>
#include <ut/utils.h>

namespace timeplus {

Client g_client(ClientOptions()
        .SetHost(           getEnvOrDefault("TIMEPLUS_HOST",     "localhost"))
        .SetPort( std::stoi(getEnvOrDefault("TIMEPLUS_PORT",     "8463")))
        .SetUser(           getEnvOrDefault("TIMEPLUS_USER",     "default"))
        .SetPassword(       getEnvOrDefault("TIMEPLUS_PASSWORD", ""))
        .SetDefaultDatabase(getEnvOrDefault("TIMEPLUS_DB",       "default"))
        .SetPingBeforeQuery(false));

static void SelectNumber(benchmark::State& state) {
    while (state.KeepRunning()) {
        g_client.Select("SELECT number, number, number FROM system.numbers LIMIT 1000",
            [](const Block& block) { block.GetRowCount(); }
        );
    }
}
BENCHMARK(SelectNumber);

static void SelectNumberMoreColumns(benchmark::State& state) {
    // Mainly test performance on type name parsing.
    while (state.KeepRunning()) {
        g_client.Select("SELECT "
                "number, number, number, number, number, number, number, number, number, number "
                "FROM system.numbers LIMIT 100",
            [](const Block& block) { block.GetRowCount(); }
        );
    }
}
BENCHMARK(SelectNumberMoreColumns);

}

BENCHMARK_MAIN();
