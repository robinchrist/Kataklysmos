#pragma once

#include <iostream>
#include <algorithm>
#include <seastar/core/sharded.hh>
#include <unordered_map>
#include <queue>
#include <bitset>
#include <limits>
#include <cctype>
#include <vector>
#include <boost/intrusive/list.hpp>
#include <seastar/http/request_parser.hh>
#include <seastar/http/request.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/util/std-compat.hh>
#include <seastar/util/modules.hh>
#include <seastar/http/routes.hh>
#include <seastar/net/tls.hh>
#include <seastar/core/shared_ptr.hh>

namespace kataklysmos {

    namespace pixelflut {

        class pixelflut_request_parser {

        };

        //Help text
        //Pixel colour at position
        //Error response
        class pixelflut_response {

        };

        struct canvas_properties {
            unsigned int width;
            unsigned int height;
            //unsigned int num_shards_horiz;
            //unsigned int num_shards_vert;
        };

        struct Pixel {
            unsigned char R, G, B;
        };

        class canvas_server;

        class pixelflut_stats;

        using namespace std::chrono_literals;

        class pixelflut_stats {
            seastar::metrics::metric_groups _metric_groups;
        public:
            pixelflut_stats(canvas_server& server, const seastar::sstring& name);
        };

        class connection : public boost::intrusive::list_base_hook<> {
            canvas_server& _server;
            seastar::connected_socket _fd;
            seastar::input_stream<char> _read_buf;
            seastar::output_stream<char> _write_buf;
            seastar::socket_address _client_addr;
            seastar::socket_address _server_addr;
            static constexpr size_t limit = 4096;
            using tmp_buf = seastar::temporary_buffer<char>;
            pixelflut_request_parser _parser;
            std::unique_ptr<seastar::http::request> _req;

            //The current response in process, set by do_response_loop
            std::unique_ptr<pixelflut_response> _currentResponse;

            // null element indicates that no furhter replies can / will be sent
            // if do_response_loop encounters such a null element, it will exit. 
            seastar::queue<std::unique_ptr<pixelflut_response>> _replies { 10 };

            bool _done = false;
        public:

            connection(
                canvas_server& server,
                seastar::connected_socket&& fd
            ):
                _server(server),
                _fd(std::move(fd)),
                _read_buf(_fd.input()),
                _write_buf(_fd.output()),
                _client_addr(_fd.remote_address()),
                _server_addr(_fd.local_address()
            ) {
                on_new_connection();
            }

            connection(
                canvas_server& server,
                seastar::connected_socket&& fd,
                seastar::socket_address client_addr,
                seastar::socket_address server_addr
            ): 
                _server(server),
                _fd(std::move(fd)),
                _read_buf(_fd.input()),
                _write_buf(_fd.output()),
                _client_addr(std::move(client_addr)),
                _server_addr(std::move(server_addr)
            ) {
                on_new_connection();
            }

            ~connection();
            void on_new_connection();

            seastar::future<> process();
            void shutdown();
            seastar::future<> read();
            seastar::future<> read_command_batch();
            seastar::future<> respond();
            seastar::future<> do_response_loop();

            //sends currentResponse_ and resets it once done
            seastar::future<> sendCurrentResponse();

            void generate_error_response_and_close(const seastar::sstring& msg);

            seastar::output_stream<char>& out();
        };

        class canvas_server_tester;

        class canvas_server : public seastar::peering_sharded_service<canvas_server> {
            std::vector<seastar::server_socket> _listeners;
            pixelflut_stats _stats;
            uint64_t _total_connections = 0;
            uint64_t _current_connections = 0;
            uint64_t _requests_served = 0;
            uint64_t _read_errors = 0;
            uint64_t _respond_errors = 0;

            size_t _content_length_limit = std::numeric_limits<size_t>::max();
            bool _content_streaming = false;
            seastar::gate _task_gate;

            unsigned int _numShards;
            bool _canvasInited = false;
            canvas_properties _canvas_properties;

            //For Matrix sharding
            // unsigned int _minNumColsPerShard;
            // unsigned int _minNumRowsPerShard;

            // //Number of remaining cols / horizontal pixels when splitting canvas to _canvas_properties.num_shards_horiz shards
            // // = _canvas_properties.width % _canvas_properties.num_shards_horiz;
            // //those remaining cols are distributed fairly to the shards
            // unsigned int _numRemainderCols;
            // //Same principle as numRemainderCols
            // unsigned int _numRemainderRows;

            // unsigned int _thisShardHorizIndex;
            // unsigned int _thisShardVertIndex;

            // unsigned int _thisShardColMin;
            // //Not inclusive
            // unsigned int _thisShardColMax;

            // unsigned int _thisShardRowMin;
            // //Not inclusive
            // unsigned int _thisShardRowMax;

            //Row Major Sharding
            unsigned _minNumPixelsPerShard;
            unsigned _numRemainingPixels;
            unsigned _thisShardMinPixelIndex;
            unsigned _thisShardMaxPixelIndex;
            unsigned _numPixelsInShard;

            std::unique_ptr<Pixel[]> _canvasShard;

        public:
            
            using connection = kataklysmos::pixelflut::connection;

            explicit canvas_server(
                const seastar::sstring& name,
                unsigned numShards
            ) :
                _stats(*this, name),
                _numShards(numShards)
            {
                
            }

            size_t get_content_length_limit() const;

            void set_content_length_limit(size_t limit);

            bool get_content_streaming() const;

            void set_content_streaming(bool b);

            const canvas_properties& getCanvasProperties() const;
            void initialiseCanvas(canvas_properties properties);
            

            unsigned int getShardIndexForCoordinate(unsigned int horiz, unsigned int vert);

            seastar::future<> listen(seastar::socket_address addr, seastar::listen_options lo);
            seastar::future<> listen(seastar::socket_address addr);
            seastar::future<> stop();

            seastar::future<> do_accepts(int which);

            uint64_t total_connections() const;
            uint64_t current_connections() const;
            uint64_t requests_served() const;
            uint64_t read_errors() const;
            uint64_t reply_errors() const;

        private:
            seastar::future<> do_accept_one(int which);
            boost::intrusive::list<connection> _connections;
            

            friend class kataklysmos::pixelflut::connection;
            friend class canvas_server_tester;
        };

        class canvas_server_tester {
        public:
            static std::vector<seastar::server_socket>& listeners(canvas_server& server) {
                return server._listeners;
            }
        };

        /*
        * A helper class to start, set and listen an http server
        * typical use would be:
        *
        *     auto server = new canvas_server_control();
        * 
        *     server->start().then([server] {
        *         server->set_routes(set_routes);
        *     }).then([server, port] {
        *         server->listen(port);
        *     }).then([port] {
        *         std::cout << "Seastar HTTP server listening on port " << port << " ...\n";
        *     });
        */
        class canvas_server_control {
            std::unique_ptr<seastar::sharded<canvas_server>> _server_sharded;
        private:
            static seastar::sstring generate_server_name();
        public:
            canvas_server_control(): 
                _server_sharded(new seastar::sharded<canvas_server>) 
            {

            }

            seastar::future<> start(seastar::sstring name, unsigned int numShards);
            seastar::future<> stop();
            seastar::future<> initialiseCanvas(canvas_properties properties);
            seastar::future<> listen(seastar::socket_address addr);
            seastar::future<> listen(seastar::socket_address addr, seastar::listen_options lo);
            seastar::sharded<canvas_server>& server();
        };

    }

}