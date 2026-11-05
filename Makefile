CXX ?= g++
CXXFLAGS ?= -O3 -g -std=c++20 -Wall -Wextra -Wpedantic -pthread
C_ALLOC_FLAGS = -DPULSEFEED_WRAP_C_ALLOC=1 -Wl,--wrap=malloc,--wrap=calloc,--wrap=realloc,--wrap=aligned_alloc
.PHONY: all test-core test sanitize benchmark clean proto compose
all: test-core
build/core_tests: OrderBook.cpp OrderBook.hpp SpscRing.hpp tests/core_tests.cpp
	mkdir -p build
	$(CXX) $(CXXFLAGS) -I. OrderBook.cpp tests/core_tests.cpp -o $@
build/allocation_tests: OrderBook.cpp OrderBook.hpp SpscRing.hpp tests/allocation_tests.cpp
	mkdir -p build
	$(CXX) $(CXXFLAGS) $(C_ALLOC_FLAGS) -I. OrderBook.cpp tests/allocation_tests.cpp -o $@
test-core: build/core_tests build/allocation_tests
	./build/core_tests
	./build/allocation_tests
test: test-core
	cd gateway && go test -race ./...
sanitize:
	mkdir -p build/sanitize
	$(CXX) -O1 -g -std=c++20 -Wall -Wextra -pthread -fsanitize=address,undefined -fno-omit-frame-pointer -I. OrderBook.cpp tests/core_tests.cpp -o build/sanitize/core_tests
	$(CXX) -O1 -g -std=c++20 -Wall -Wextra -pthread -fsanitize=address,undefined -fno-omit-frame-pointer $(C_ALLOC_FLAGS) -I. OrderBook.cpp tests/allocation_tests.cpp -o build/sanitize/allocation_tests
	./build/sanitize/core_tests
	./build/sanitize/allocation_tests
benchmark:
	cmake -S . -B build/bench -DCMAKE_BUILD_TYPE=Release -DPULSEFEED_BUILD_BENCHMARKS=ON
	cmake --build build/bench --parallel
	./build/bench/benchmarks
proto:
	./scripts/generate-go.sh
compose:
	docker compose up --build -d
clean:
	rm -rf build
