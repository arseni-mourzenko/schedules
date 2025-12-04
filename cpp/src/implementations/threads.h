#include <algorithm>
#include <chrono>
#include <emmintrin.h>
#include <immintrin.h>
#include <iostream>
#include <map>
#include <pqxx/pqxx>
#include <ranges>
#include <thread>
#include <future>

namespace threads {
class Slots {
 public:
    explicit Slots(const pqxx::binarystring &blob) :
        head { _mm256_loadu_si256(reinterpret_cast<const __m256i*>(blob.data())) },
        tail { _mm_loadu_si128(reinterpret_cast<const __m128i*>(blob.data() + 26)) } { }

    bool matches(const Slots &other) const {
        return
            are_m256i_equal(_mm256_and_si256(this->head, other.head), this->head) &&
            are_m128i_equal(_mm_and_si128(this->tail, other.tail), this->tail);
    }

 private:
    const __m256i head;
    const __m128i tail;

    static bool are_m128i_equal(const __m128i& a, const __m128i& b) {
        const auto cmp { _mm_cmpeq_epi8(a, b) };
        const auto mask { _mm_movemask_epi8(cmp) };
        return mask == 0xFFFF;
    }

    static bool are_m256i_equal(const __m256i& a, const __m256i& b) {
        const auto cmp { _mm256_cmpeq_epi8(a, b) };
        const auto mask { _mm256_movemask_epi8(cmp) };
        return mask == 0xFFFFFFFF;
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

        constexpr auto Users { 5000 };
        constexpr auto Pages { 10 };
        constexpr auto PageSize { Users / Pages };
        static_assert(Users % Pages == 0);

        std::vector<std::future<std::map<int, int>>> tasks { };

        for (auto page { 0 }; page < Pages; ++page) {
            tasks.emplace_back(std::async(std::launch::async, matchPage, &users, page * PageSize, PageSize));
        }

        for (auto &task : tasks) {
            const auto c { task.get() };
            counters.insert(c.begin(), c.end());
        }

        const auto endMatch { std::chrono::steady_clock::now() };
        const auto matchDuration { std::chrono::duration_cast<std::chrono::milliseconds>(endMatch - startMatch).count() };
        std::cout << "Matches computed in " << matchDuration << " ms." << std::endl;
        return counters;
    }

 private:
    static constexpr size_t SlotsLength { 42 };

    static std::map<int, int> matchPage(const std::vector<Slots> *users, int skip, int take) {
        pqxx::connection connection { "dbname=schedules" };
        pqxx::work db { connection };

        connection.prepare("p", "select id, slots from events offset $1 limit $2");

        const auto result { db.exec_prepared("p", skip, take) };
        std::map<int, int> counters { };
        for (auto row : result) {
            const auto eventId { row[0].as<int>() };
            const auto eventSlots { byteaToSlots(row[1]) };
            counters[eventId] = matches(eventSlots, *users);
        }

        return counters;
    }

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
