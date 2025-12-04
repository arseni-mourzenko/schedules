#include <algorithm>
#include <chrono>
#include <iostream>
#include <map>
#include <pqxx/pqxx>
#include <ranges>

#include "implementations/avx2.h"
#include "implementations/int64.h"
#include "implementations/plain.h"
#include "implementations/sse.h"
#include "implementations/threads.h"

inline std::map<int, int> match(std::string &type, pqxx::work &db) {
    if (type == "plain") {
        return avx2::Matcher().match(db);
    }

    if (type == "sse") {
        return sse::Matcher().match(db);
    }

    if (type == "avx2") {
        return avx2::Matcher().match(db);
    }

    if (type == "threads") {
        return threads::Matcher().match(db);
    }

    if (type == "int64") {
        return int64::Matcher().match(db);
    }

    throw std::out_of_range("The specified type is not supported.");
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <type>" << std::endl;
        return 1; // Exit with an error code
    }

    std::string type { argv[1] };

    pqxx::connection connection { "dbname=schedules" };
    pqxx::work db { connection };

    const auto counters { match(type, db) };

    for (const auto &[eventId, countMatches] : counters) {
        std::cout << "." << eventId << ":" << countMatches << std::endl;
    }
}
