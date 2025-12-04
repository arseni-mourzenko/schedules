#include <algorithm>
#include <chrono>
#include <iostream>
#include <map>
#include <pqxx/pqxx>
#include <ranges>

namespace int64 {
constexpr size_t SlotsLength { 42 };

class Slots {
 public:
    explicit Slots(const pqxx::binarystring &blob) {
        const auto data { blob.data() };
        std::memcpy(&head[0], data, 8 * 5);
        std::memcpy(&tail, data + (8 * 5), 2);
    }

    bool matches(const Slots &other) const {
        if ((this->tail & other.tail) != this->tail) {
            return false;
        }

        for (auto i { 0 }; i < 5; ++i) {
            const auto h { this->head[i] };
            if ((h & other.head[i]) != h) {
                return false;
            }
        }

        return true;
    }

 private:
    std::array<uint64_t, 5> head { 0 };
    uint16_t tail { 0 };
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

    static Slots byteaToSlots(pqxx::field const &field) {
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
