#include <algorithm>
#include <chrono>
#include <emmintrin.h>
#include <immintrin.h>
#include <iostream>
#include <map>
#include <pqxx/pqxx>
#include <ranges>

namespace plain {
constexpr size_t SlotsLength { 42 };
using Slots = std::array<std::byte, SlotsLength>;

class Matcher {
 public:
    std::map<int, int> match(pqxx::work &db) {
        const auto startUsers { std::chrono::steady_clock::now() };
        const auto users { loadUsers(db) };
        const auto endUsers { std::chrono::steady_clock::now() };
        const auto usersDuration { std::chrono::duration_cast<std::chrono::milliseconds>(endUsers - startUsers).count() };

        std::cout << users.size() << " users loaded in " << usersDuration << " ms." << std::endl;

        const auto startMatch { std::chrono::steady_clock::now() };
        std::map<int, int> counters { };

        pqxx::result result { db.exec("select id, slots from events") };

        for (auto row : result) {
            const auto eventId { row[0].as<int>() };
            const auto eventSlots { byteaToSlots(row[1]) };
            counters[eventId] = matches(eventSlots, users);
        }

        const auto endMatch { std::chrono::steady_clock::now() };
        const auto matchDuration { std::chrono::duration_cast<std::chrono::milliseconds>(endMatch - startMatch).count() };
        std::cout << "Matches computed in " << matchDuration << " ms." << std::endl;
        return counters;
    }

 private:
    static constexpr size_t SlotsLength { 42 };

    static Slots byteaToSlots(pqxx::field const &field) {
        pqxx::binarystring blob { field };
        assert(blob.size() - 1 == SlotsLength);

        const std::byte* source { reinterpret_cast<const std::byte*>(blob.data()) };

        Slots result;
        std::copy_n(source, SlotsLength, result.begin());
        return result;
    }

    static std::vector<Slots> loadUsers(pqxx::work &db) {
        std::vector<Slots> slots { };
        pqxx::result result { db.exec("select slots from users;") };
        for (auto row : result) {
            slots.push_back(byteaToSlots(row[0]));
        }

        return slots;
    }

    static bool matches(const Slots &eventSlots, const Slots &userSlots) {
        for (size_t i { 0 }; i < SlotsLength; ++i) {
            const auto es { eventSlots[i] };
            const auto us { userSlots[i] };
            if ((us & es) != es) {
                return false;
            }
        }

        return true;
    }

    static int matches(const Slots &eventSlots, const std::vector<Slots> &users) {
        auto counter { 0 };

        for (const auto &userSlots : users) {
            if (matches(eventSlots, userSlots)) {
                ++counter;
            }
        }

        return counter;
    }
};
}
