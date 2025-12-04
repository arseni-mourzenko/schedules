#include <chrono>
#include <iostream>
#include <map>
#include <pqxx/pqxx>
#include <hip/hip_runtime.h>

constexpr size_t SlotsLength { 42 };
using Slots = std::array<std::byte, SlotsLength>;

__global__ void bitwiseAndMatchKernel(
        const std::byte* eventsData,
        const std::byte* usersData,
        int* matchesData,
        const size_t countUsers,
        const size_t countEvents) {
    size_t userIndex { blockIdx.x * blockDim.x + threadIdx.x };
    size_t eventIndex { blockIdx.y * blockDim.y + threadIdx.y };

    if (userIndex >= countUsers || eventIndex >= countEvents) {
        return;
    }

    const std::byte* eventSlots { eventsData + eventIndex * SlotsLength };
    const std::byte* userSlots { usersData + userIndex * SlotsLength };

    bool match { true };
    for (size_t i { 0 }; i < SlotsLength; ++i) {
        if ((eventSlots[i] & userSlots[i]) != eventSlots[i]) {
            match = false;
            break;
        }
    }

    matchesData[eventIndex * countUsers + userIndex] = match ? 1 : 0;
}

__global__ void reduceMatchesKernel(
        const int* matchesData,
        int* resultData,
        const int countUsers,
        const int countEvents) {
    extern __shared__ int shared[];

    const size_t eventIndex { blockIdx.y };
    const size_t threadIndex { threadIdx.x };
    const size_t i { blockIdx.x * blockDim.x + threadIdx.x };

    if (eventIndex >= countEvents) {
        return;
    }

    const int* eventMatches { matchesData + eventIndex * countUsers };
    shared[threadIndex] = (i < countUsers) ? eventMatches[i] : 0;
    __syncthreads();

    for (size_t s { blockDim.x / 2 }; s > 0; s >>= 1) {
        if (threadIndex < s) {
            shared[threadIndex] += shared[threadIndex + s];
        }
        __syncthreads();
    }

    if (threadIndex == 0) {
        atomicAdd(&resultData[eventIndex], shared[0]);
    }
}

class Matcher {
 public:
    std::map<int, int> match(pqxx::work &db) {
        std::byte* usersData;
        std::byte* eventsData;
        std::byte* pinnedUsers;
        std::byte* pinnedEvents;

        const int countUsers { Matcher::prepareSlots(loadUsers(db), usersData, pinnedUsers) };
        const int countEvents { Matcher::prepareSlots(loadEvents(db), eventsData, pinnedEvents) };

        const auto startMatch { std::chrono::steady_clock::now() };

        int* matchesData;
        hipMalloc(&matchesData, countUsers * countEvents * sizeof(int));

        int* resultData;
        hipMalloc(&resultData, countEvents * sizeof(int));
        hipMemset(resultData, 0, countEvents * sizeof(int));

        std::cout << "Prepared in " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startMatch).count() << " ms." << std::endl;

        const auto startKernel { std::chrono::steady_clock::now() };
        matchesAllEvents(usersData, eventsData, matchesData, resultData, countUsers, countEvents);
        const auto endKernel { std::chrono::steady_clock::now() };

        std::vector<int> hostResults(countEvents);
        hipMemcpy(hostResults.data(), resultData, countEvents * sizeof(int), hipMemcpyDeviceToHost);

        std::map<int, int> counters;
        for (int i { 0 }; i < countEvents; ++i) {
            counters[i + 1] = hostResults[i];
        }

        assert(hipFree(usersData) == hipSuccess);
        assert(hipFree(eventsData) == hipSuccess);
        assert(hipFree(pinnedUsers) == hipSuccess);
        assert(hipFree(pinnedEvents) == hipSuccess);
        assert(hipFree(matchesData) == hipSuccess);
        assert(hipFree(resultData) == hipSuccess);

        const auto endMatch { std::chrono::steady_clock::now() };
        const auto matchDuration { std::chrono::duration_cast<std::chrono::milliseconds>(endMatch - startMatch).count() };
        const auto kernelDuration { std::chrono::duration_cast<std::chrono::milliseconds>(endKernel - startKernel).count() };
        std::cout << "Matches computed in " << matchDuration << " ms." << std::endl;
        std::cout << "Kernels used " << kernelDuration << " ms." << std::endl;
        return counters;
    }

 private:
    static Slots byteaToSlots(pqxx::field const &field) {
        pqxx::binarystring blob { field };
        assert(blob.size() - 1 == SlotsLength);

        const std::byte* source { reinterpret_cast<const std::byte*>(blob.data()) };

        Slots result;
        std::copy_n(source, SlotsLength, result.begin());
        return result;
    }

    static int prepareSlots(const std::vector<Slots> &slots, std::byte*& data, std::byte*& pinned) {
        const auto startUsers { std::chrono::steady_clock::now() };
        const auto countUsers { static_cast<int>(slots.size()) };
        const auto usersLength { countUsers * SlotsLength };

        hipHostMalloc(&pinned, usersLength);
        std::memcpy(pinned, slots.data(), usersLength);

        hipMalloc(&data, usersLength);
        hipMemcpy(data, pinned, usersLength, hipMemcpyHostToDevice);
        hipDeviceSynchronize();

        const auto endUsers { std::chrono::steady_clock::now() };
        const auto usersDuration { std::chrono::duration_cast<std::chrono::milliseconds>(endUsers - startUsers).count() };

        std::cout << slots.size() << " records loaded in " << usersDuration << " ms." << std::endl;

        return countUsers;
    }

    static std::vector<Slots> loadUsers(pqxx::work &db) {
        std::vector<Slots> slots { };
        pqxx::result result { db.exec("select slots from users;") };
        for (auto row : result) {
            slots.push_back(byteaToSlots(row[0]));
        }
        return slots;
    }

    static std::vector<Slots> loadEvents(pqxx::work &db) {
        std::vector<Slots> slots { };
        pqxx::result result { db.exec("select slots from events;") };
        for (auto row : result) {
            slots.push_back(byteaToSlots(row[0]));
        }
        return slots;
    }

    void matchesAllEvents(
            std::byte* usersData,
            std::byte* eventsData, 
            int* matchesData,
            int* resultData, 
            const int countUsers,
            const int countEvents) {
        const int blockSizeX { 256 };
        const int blockSizeY { 1 };
        const int countBlocksX { (countUsers + blockSizeX - 1) / blockSizeX };
        const int countBlocksY { countEvents };

        dim3 blockDim(blockSizeX, blockSizeY);
        dim3 gridDim(countBlocksX, countBlocksY);

        hipLaunchKernelGGL(
            bitwiseAndMatchKernel,
            gridDim,
            blockDim,
            0,
            0,
            eventsData,
            usersData,
            matchesData,
            countUsers,
            countEvents);

        assert(hipGetLastError() == hipSuccess);

        size_t sharedMemorySize { blockSizeX * sizeof(int) };
        hipLaunchKernelGGL(
            reduceMatchesKernel,
            gridDim,
            blockDim,
            sharedMemorySize,
            0,
            matchesData,
            resultData,
            countUsers,
            countEvents);

        assert(hipGetLastError() == hipSuccess);
        assert(hipDeviceSynchronize() == hipSuccess);
    }
};

void warmup() {
    auto start = std::chrono::steady_clock::now();

    std::byte b { 0xFF };
    std::byte* a;
    hipMalloc(&a, 1);
    hipMemcpy(a, &b, 1, hipMemcpyHostToDevice);
    hipDeviceSynchronize();

    auto end = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0;
    std::cout << "Warmup: " << ms << " ms" << std::endl;
}

int main() {
    warmup();

    pqxx::connection connection { "dbname=schedules" };
    pqxx::work db { connection };

    const auto counters { Matcher().match(db) };

    for (const auto &[eventId, countMatches] : counters) {
        std::cout << "." << eventId << ":" << countMatches << std::endl;
    }
}
