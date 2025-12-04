#include <algorithm>
#include <chrono>
#include <emmintrin.h>
#include <immintrin.h>
#include <iostream>
#include <map>
#include <pqxx/pqxx>
#include <ranges>

namespace sse {
class Slots {
 public:
    explicit Slots(const pqxx::binarystring &blob) :
        one { _mm_loadu_si128(reinterpret_cast<const __m128i*>(blob.data())) },
        two { _mm_loadu_si128(reinterpret_cast<const __m128i*>(blob.data() + 16)) },
        three { _mm_loadu_si128(reinterpret_cast<const __m128i*>(blob.data() + 26)) } { }

    bool matches(const Slots &other) const {
        return
            are_m128i_equal(_mm_and_si128(this->one, other.one), this->one) &&
            are_m128i_equal(_mm_and_si128(this->two, other.two), this->two) &&
            are_m128i_equal(_mm_and_si128(this->three, other.three), this->three);
    }

 private:
    const __m128i one;
    const __m128i two;
    const __m128i three;

    static bool are_m128i_equal(const __m128i& a, const __m128i& b) {
        const auto cmp { _mm_cmpeq_epi8(a, b) };
        const auto mask { _mm_movemask_epi8(cmp) };
        return mask == 0xFFFF;
    }
};

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

    static Slots byteaToSlots(pqxx::field const &field)
    {
        pqxx::binarystring blob { field };
        assert(blob.size() - 1 == SlotsLength);
        return Slots { blob };
    }

    static std::vector<Slots> loadUsers(pqxx::work &db) {
        std::vector<Slots> slots { };
        pqxx::result result { db.exec("select slots from users;") };
        for (auto row : result) {
            slots.push_back(byteaToSlots(row[0]));
        }

        return slots;
    }

    static int matches(const Slots &eventSlots, const std::vector<Slots> &users) {
        auto counter { 0 };

        for (const auto &userSlots : users) {
            if (eventSlots.matches(userSlots)) {
                ++counter;
            }
        }

        return counter;
    }
};
}
