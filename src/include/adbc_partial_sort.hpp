#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace adbc {

void RegisterAdbcPartialSort(ExtensionLoader &loader);

} // namespace adbc
} // namespace duckdb
