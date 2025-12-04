#!/bin/bash

echo "HIP"; time ./cpphip/bin/schedules > /tmp/schedules-hip

echo "C++ plain"; time ./cpp/bin/schedules plain > /tmp/schedules-cpp-plain
echo "C++ SSE"; time ./cpp/bin/schedules sse > /tmp/schedules-cpp-sse
echo "C++ AVX2"; time ./cpp/bin/schedules avx2 > /tmp/schedules-cpp-avx2
echo "C++ threads"; time ./cpp/bin/schedules threads > /tmp/schedules-cpp-threads
echo "C++ int64_t"; time ./cpp/bin/schedules int64 > /tmp/schedules-cpp-int64

echo "Python plain"; time ./python/run.py plain > /tmp/schedules-python-plain
echo "Python pandas"; time ./python/run.py pandas > /tmp/schedules-python-pandas
echo "Python map-reduce"; time ./python/run.py mapreduce > /tmp/schedules-python-mapreduce
echo "Python map reduce shared memory"; time ./python/run.py mapreduce-shared > /tmp/schedules-python-mapreduce-shared
echo "Python map reduce shared file"; time ./python/run.py mapreduce-file > /tmp/schedules-python-mapreduce-file
#echo "Python in database"; time ./python/run.py indb > /tmp/schedules-python-indb
