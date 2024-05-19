# Project Kataklysmos

The (hopefully) fastest Pixelflut server in the world, based on the Seastar framework.

Work in progress, stay tuned!


## Building

Recommended build steps:
```
git clone --recursive git@github.com:robinchrist/Kataklysmos.git

cd ./Kataklysmos/external/seastar

sudo ./install-dependencies.sh

./configure.py --mode=release --enable-dpdk --c++-standard 20 --compiler clang++ --c-compiler clang

ninja -C build/release

cd ../..

mkdir build && cd build

cmake -DCMAKE_CXX_COMPILER="clang++" -DCMAKE_C_COMPILER="clang" -DCMAKE_PREFIX_PATH="$(pwd)/../external/seastar/build/release;$(pwd)/../external/seastar/build/release/_cooking/installed" -DCMAKE_MODULE_PATH="$(pwd)/../external/seastar/cmake" -DCMAKE_BUILD_TYPE="Release" -GNinja -DCMAKE_EXPORT_COMPILE_COMMANDS="TRUE" ..
```

Notes:
Clang 16 does not seem to work with C++23 - if you want to use Clang (which you should!), you must specify `--c++-standard 20`

Build configs which I tested:
```
What is working:
- Build with two different compilers (gcc 13.2.0 builds Seastar with C++20, Clang 16 builds the app with C++20 and vice versa, Clang 16 builds Seastar with C++20 and gcc 13.2.0 builds the app with C++20)
- Build with gcc and two different C++ versions (Seastar with C++23, app forced to C++20 via cmake - for both gcc 13.2.0)
- Build Seastar & app with Clang 16 and C++20

What is NOT working:
- Build Seastar with Clang 16 and C++23
- Build with two different compiler and C++ versions (Seastar with gcc 13.2.0 and C++23, app with Clang and C++20)
```