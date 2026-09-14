CXX = g++
CXXFLAGS = -std=c++17 -O2
TARGET = build/solution_14827_baseline
TARGET_OPT = build/solution_optimized

.PHONY: all clean

all: $(TARGET) $(TARGET_OPT)

$(TARGET): src/solution_14827_baseline.cpp
	mkdir -p build
	$(CXX) $(CXXFLAGS) -o $(TARGET) src/solution_14827_baseline.cpp

$(TARGET_OPT): src/solution_optimized.cpp
	mkdir -p build
	$(CXX) $(CXXFLAGS) -o $(TARGET_OPT) src/solution_optimized.cpp

clean:
	rm -rf build