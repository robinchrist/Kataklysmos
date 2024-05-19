# Make sure we use a relatively recent version - 3.27 is Nov 2023
cmake_minimum_required(VERSION 3.27)

project ("Kataklysmos" CXX)

# DO NOT CHANGE - Seastar build process is very brittle (due to usage of new stuff like coroutine and source location)
# C++ Version of Kataklysmos MUST match C++ version with which Seastar is built.
set(CMAKE_CXX_STANDARD 20)

find_package (Seastar REQUIRED)


add_executable (my_app
  src/main.cpp
  src/canvas_server.cpp
)


target_link_libraries (my_app PUBLIC Seastar::seastar)

target_include_directories(my_app PUBLIC "include")