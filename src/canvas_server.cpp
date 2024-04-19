/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2015 Cloudius Systems
 */

#include <memory>
#include <algorithm>
#include <bitset>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <limits>
#include <queue>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shard_id.hh>
#include <stdexcept>
#include <unordered_map>
#include <vector>
#include <kataklysmos/canvas_server.hpp>

#include <seastar/core/sstring.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/print.hh>
#include <seastar/http/internal/content_source.hh>
#include <seastar/http/reply.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/util/log.hh>
#include <seastar/util/string_utils.hh>



using namespace std::chrono_literals;

namespace kataklysmos {

    seastar::logger hlogger("canvas");

    namespace pixelflut {


        pixelflut_stats::pixelflut_stats(canvas_server& server, const seastar::sstring& name) {
            namespace sm = seastar::metrics;
            std::vector<sm::label_instance> labels;

            labels.push_back(sm::label_instance("service", name));
            _metric_groups.add_group("httpd", {
                    sm::make_counter("connections_total", [&server] { return server.total_connections(); }, sm::description("The total number of connections opened"), labels),
                    sm::make_gauge("connections_current", [&server] { return server.current_connections(); }, sm::description("The current number of open  connections"), labels),
                    sm::make_counter("read_errors", [&server] { return server.read_errors(); }, sm::description("The total number of errors while reading http requests"), labels),
                    sm::make_counter("reply_errors", [&server] { return server.reply_errors(); }, sm::description("The total number of errors while replying to http"), labels),
                    sm::make_counter("requests_served", [&server] { return server.requests_served(); }, sm::description("The total number of http requests served"), labels)
            });
        }

        seastar::future<> connection::do_response_loop() {
            return _replies.pop_eventually().then(
                [this] (std::unique_ptr<kataklysmos::pixelflut::pixelflut_response> resp) {
                    if (!resp) {
                        // eof
                        return seastar::make_ready_future<>();
                    }
                    _currentResponse = std::move(resp);
                    return sendCurrentResponse().then([this] {
                                return do_response_loop();
                            });
                });
        }

        seastar::future<> connection::sendCurrentResponse() {
            /*if (_currentResponse->_body_writer) {
                return _currentResponse->write_reply_to_connection(*this).then_wrapped([this] (auto f) {
                    if (f.failed()) {
                        // In case of an error during the write close the connection
                        _server._respond_errors++;
                        _done = true;
                        _replies.abort(std::make_exception_ptr(std::logic_error("Unknown exception during body creation")));
                        _replies.push(std::unique_ptr<seastar::http::reply>());
                        f.ignore_ready_future();
                        return seastar::make_ready_future<>();
                    }
                    return _write_buf.write("0\r\n\r\n", 5);
                }).then_wrapped([this ] (auto f) {
                    if (f.failed()) {
                        // We could not write the closing sequence
                        // Something is probably wrong with the connection,
                        // we should close it, so the client will disconnect
                        _done = true;
                        _replies.abort(std::make_exception_ptr(std::logic_error("Unknown exception during body creation")));
                        _replies.push(std::unique_ptr<seastar::http::reply>());
                        f.ignore_ready_future();
                        return seastar::make_ready_future<>();
                    } else {
                        return _write_buf.flush();
                    }
                }).then_wrapped([this] (auto f) {
                    if (f.failed()) {
                        // flush failed. just close the connection
                        _done = true;
                        _replies.abort(std::make_exception_ptr(std::logic_error("Unknown exception during body creation")));
                        _replies.push(std::unique_ptr<seastar::http::reply>());
                        f.ignore_ready_future();
                    }
                    _currentResponse.reset();
                    return seastar::make_ready_future<>();
                });
            }
            set_headers(*_currentResponse);
            _currentResponse->_headers["Content-Length"] = seastar::to_sstring(
                    _currentResponse->_content.size());
            return _write_buf.write(_currentResponse->_response_line.data(), _currentResponse->_response_line.size()).then([this] {
                return _currentResponse->write_reply_headers(*this);
            }).then([this] {
                return _write_buf.write("\r\n", 2);
            }).then([this] {
                return _write_buf.write(_currentResponse->_content.data(), _currentResponse->_content.size());
            }).then([this] {
                return _write_buf.flush();
            }).then([this] {
                _currentResponse.reset();
            });*/
            //TODO: Implement sending response
            _currentResponse.reset();

            return seastar::make_ready_future<>();
        }

        connection::~connection() {
            --_server._current_connections;
            _server._connections.erase(_server._connections.iterator_to(*this));
        }

        void connection::on_new_connection() {
            ++_server._total_connections;
            ++_server._current_connections;
            _fd.set_nodelay(true);
            _server._connections.push_back(*this);
        }

        seastar::future<> connection::read() {
            return seastar::do_until([this] {return _done;}, [this] {
                return read_command_batch();
            }).then_wrapped([this] (seastar::future<> f) {
                // swallow error
                if (f.failed()) {
                    _server._read_errors++;
                }
                f.ignore_ready_future();

                //Will close the do_response_loop / respond, leading to when_all(read(), respond()) to exit
                //and eventually the connection to be terminated
                return _replies.push_eventually( {});
            });
        }

        static seastar::future<std::unique_ptr<seastar::http::request>>
        set_request_content(std::unique_ptr<seastar::http::request> req, seastar::input_stream<char>* content_stream, bool streaming) {
            req->content_stream = content_stream;

            if (streaming) {
                return seastar::make_ready_future<std::unique_ptr<seastar::http::request>>(std::move(req));
            } else {
                // Read the entire content into the request content string
                return seastar::util::read_entire_stream_contiguous(*content_stream).then([req = std::move(req)] (seastar::sstring content) mutable {
                    req->content = std::move(content);
                    return seastar::make_ready_future<std::unique_ptr<seastar::http::request>>(std::move(req));
                });
            }
        }

        void connection::generate_error_response_and_close(const seastar::sstring& msg) {
            auto resp = std::make_unique<kataklysmos::pixelflut::pixelflut_response>();
            
            _done = true;
            _replies.push(std::move(resp));
        }

        seastar::future<> connection::read_command_batch() {
            //_parser.init();

            //parser: Parse as much as allowed from _read_buf
            //process 

            //parser: lookahead min(max_bytes, max_num_commands * max_command_length)
            //parser: find all newlines
            //push stuff after last newline back into buffer (return stop_consuming from parser)
            //parser: parse commands, process commands, err on error
            

            // return _read_buf.consume(_parser).then([this] () mutable {
            //     if (_parser.eof()) {
            //         _done = true;
            //         return make_ready_future<>();
            //     }
            //     ++_server._requests_served;
            //     std::unique_ptr<seastar::http::request> req = _parser.get_parsed_request();

            //     req->_server_address = this->_server_addr;
            //     req->_client_address = this->_client_addr;


            //     if (_parser.failed()) {
            //         if (req->_version.empty()) {
            //             // we might have failed to parse even the version
            //             req->_version = "1.1";
            //         }
            //         generate_error_reply_and_close(std::move(req), http::reply::status_type::bad_request, "Can't parse the request");
            //         return seastar::make_ready_future<>();
            //     }

            //     size_t content_length_limit = _server.get_content_length_limit();
            //     seastar::sstring length_header = req->get_header("Content-Length");
            //     req->content_length = strtol(length_header.c_str(), nullptr, 10);

            //     if (req->content_length > content_length_limit) {
            //         auto msg = seastar::format("Content length limit ({}) exceeded: {}", content_length_limit, req->content_length);
            //         generate_error_reply_and_close(std::move(req), seastar::http::reply::status_type::payload_too_large, std::move(msg));
            //         return seastar::make_ready_future<>();
            //     }

            //     seastar::sstring encoding = req->get_header("Transfer-Encoding");
            //     if (encoding.size() && !seastar::internal::case_insensitive_cmp()(encoding, "chunked")){
            //         //TODO: add "identity", "gzip"("x-gzip"), "compress"("x-compress"), and "deflate" encodings and their combinations
            //         generate_error_reply_and_close(std::move(req), seastar::http::reply::status_type::not_implemented, format("Encodings other than \"chunked\" are not implemented (received encoding: \"{}\")", encoding));
            //         return seastar::make_ready_future<>();
            //     }

            //     auto maybe_reply_continue = [this, req = std::move(req)] () mutable {
            //         if (req->_version == "1.1" && seastar::internal::case_insensitive_cmp()(req->get_header("Expect"), "100-continue")){
            //             return _replies.not_full().then([req = std::move(req), this] () mutable {
            //                 auto continue_reply = std::make_unique<seastar::http::reply>();
            //                 set_headers(*continue_reply);
            //                 continue_reply->set_version(req->_version);
            //                 continue_reply->set_status(seastar::http::reply::status_type::continue_).done();
            //                 this->_replies.push(std::move(continue_reply));
            //                 return seastar::make_ready_future<std::unique_ptr<seastar::http::request>>(std::move(req));
            //             });
            //         } else {
            //             return seastar::make_ready_future<std::unique_ptr<seastar::http::request>>(std::move(req));
            //         }
            //     };

            //     return maybe_reply_continue().then([this] (std::unique_ptr<seastar::http::request> req) {
            //         return do_with(
            //             _read_buf, //make_content_stream(req.get(), _read_buf) -> previously limited size of stream
            //             seastar::sstring(req->_version),
            //             std::move(req), 
            //         [this] (seastar::input_stream<char>& content_stream, seastar::sstring& version, std::unique_ptr<seastar::http::request>& req) {
                        
            //             return set_request_content(std::move(req), &content_stream, _server.get_content_streaming())
            //                 .then([this, &content_stream] (std::unique_ptr<seastar::http::request> req) {

            //                     return _replies.not_full().then([this, req = std::move(req)] () mutable {
            //                         auto resp = std::make_unique<seastar::http::reply>();
            //                         resp->set_version(req->_version);
            //                         set_headers(*resp);
            //                         bool keep_alive = req->should_keep_alive();
            //                         if (keep_alive && req->_version == "1.0") {
            //                             resp->_headers["Connection"] = "Keep-Alive";
            //                         }

            //                         seastar::sstring url = req->parse_query_param();
            //                         seastar::sstring version = req->_version;
            //                         return _server._routes.handle(url, std::move(req), std::move(resp)).
            //                         // Caller guarantees enough room
            //                         then([this, keep_alive , version = std::move(version)](std::unique_ptr<seastar::http::reply> rep) {
            //                             rep->set_version(version).done();
            //                             this->_replies.push(std::move(rep));
            //                             return seastar::make_ready_future<bool>(!keep_alive);
            //                         });
            //                     }).then([this, &content_stream](bool done) {
            //                         _done = done;
            //                         // If the handler did not read the entire request
            //                         // content, this connection cannot be reused so we
            //                         // need to close it (via "_done = true"). But we can't
            //                         // just check content_stream.eof(): It may only become
            //                         // true after read(). Issue #907.
            //                         return content_stream.read().then([this] (seastar::temporary_buffer<char> buf) {
            //                             if (!buf.empty()) {
            //                                 _done = true;
            //                             }
            //                         });
            //                     });

            //             }).handle_exception_type([this, &version] (const seastar::httpd::base_exception& e) mutable {
            //                 // If the request had a "Transfer-Encoding: chunked" header and content streaming wasn't enabled, we might have failed
            //                 // before passing the request to handler - when we were parsing chunks
            //                 auto err_req = std::make_unique<seastar::http::request>();
            //                 err_req->_version = version;
            //                 generate_error_reply_and_close(std::move(err_req), e.status(), e.str());
            //             });
            //         });
            //     });
            // });
            return seastar::make_ready_future<>();
        }

        seastar::future<> connection::process() {
            // Launch read and write "threads" simultaneously
            // Once the future returned by this function returns, connection will be closed
            return when_all(read(), respond()).then(
                    [] (std::tuple<seastar::future<>, seastar::future<>> joined) {
                try {
                    std::get<0>(joined).get();
                } catch (...) {
                    hlogger.debug("Read exception encountered: {}", std::current_exception());
                }
                try {
                    std::get<1>(joined).get();
                } catch (...) {
                    hlogger.debug("Response exception encountered: {}", std::current_exception());
                }
                return seastar::make_ready_future<>();
            }).finally([this]{
                return _read_buf.close().handle_exception([](std::exception_ptr e) {
                    hlogger.debug("Close exception encountered: {}", e);
                });
            });
        }
        void connection::shutdown() {
            _fd.shutdown_input();
            _fd.shutdown_output();
        }

        seastar::output_stream<char>& connection::out() {
            return _write_buf;
        }

        seastar::future<> connection::respond() {
            return do_response_loop().then_wrapped([this] (seastar::future<> f) {
                // swallow error
                if (f.failed()) {
                    _server._respond_errors++;
                }
                f.ignore_ready_future();
                return _write_buf.close();
            });
        }


        size_t canvas_server::get_content_length_limit() const {
            return _content_length_limit;
        }

        void canvas_server::set_content_length_limit(size_t limit) {
            _content_length_limit = limit;
        }

        bool canvas_server::get_content_streaming() const {
            return _content_streaming;
        }

        void canvas_server::set_content_streaming(bool b) {
            _content_streaming = b;
        }

        const canvas_properties& canvas_server::getCanvasProperties() const {
            return _canvas_properties;
        }

        void canvas_server::initialiseCanvas(canvas_properties properties) {
            if(_canvasInited) {
                throw std::runtime_error("Cannot set canvas properties more than once!");
            }

            _canvas_properties = properties;
            if(seastar::this_shard_id() == 0) {
                hlogger.debug("width = {}, height = {}", _canvas_properties.width, _canvas_properties.height);
            }

            //Row Major Sharding:
            if(_numShards > _canvas_properties.height * _canvas_properties.width) {
                throw std::out_of_range("number of shards is greater than number of pixels on canvas!");
            }

            unsigned int totalNumPixels = _canvas_properties.height * _canvas_properties.width;

            _minNumPixelsPerShard = totalNumPixels / _numShards;
            _numRemainingPixels = totalNumPixels % _numShards;

            _thisShardMinPixelIndex = 
                seastar::this_shard_id() * _minNumPixelsPerShard + 
                std::min(seastar::this_shard_id(), _numRemainingPixels);

            _thisShardMaxPixelIndex = _thisShardMinPixelIndex + _minNumPixelsPerShard + (seastar::this_shard_id() < _numRemainingPixels ? 1 : 0);

            _numPixelsInShard = _thisShardMaxPixelIndex - _thisShardMinPixelIndex;

            hlogger.debug("I will cover len [{}, {}) = {} Pixels", _thisShardMinPixelIndex, _thisShardMaxPixelIndex, _numPixelsInShard);

            _canvasShard = std::unique_ptr<Pixel[]>( new Pixel[_numPixelsInShard]);

            // For Matrix Sharding:
            // unsigned int totalNumShards = _canvas_properties.num_shards_horiz * _canvas_properties.num_shards_vert;
            
            // if(totalNumShards != _numShards) {
            //     throw std::runtime_error(seastar::format("We have {} shards, but canvas properties results in {} shards", _numShards, totalNumShards));
            // }

            // //container().

            // //TODO: Implement memory allocation etc

            // //Integer division rounds down
            // _minNumColsPerShard = _canvas_properties.width / _canvas_properties.num_shards_horiz;
            // if(seastar::this_shard_id() == 0) hlogger.debug("_minNumColsPerShard = {}", _minNumColsPerShard);

            // //Don't allow the edge case that some workers wouldn't have work
            // if(_minNumColsPerShard == 0) {
            //     throw std::runtime_error(seastar::format("Number of horizontal shards {} exceeds canvas width {}", _canvas_properties.num_shards_horiz, _canvas_properties.width));
            // }

            // _numRemainderCols = _canvas_properties.width % _canvas_properties.num_shards_horiz;
            // if(seastar::this_shard_id() == 0) hlogger.debug("_numRemainderCols = {}", _numRemainderCols);


            // _minNumRowsPerShard = _canvas_properties.height / _canvas_properties.num_shards_vert;
            // if(seastar::this_shard_id() == 0) hlogger.debug("_minNumRowsPerShard = {}", _minNumRowsPerShard);
            // //Don't allow the edge case that some workers wouldn't have work
            // if(_minNumRowsPerShard == 0) {
            //     throw std::runtime_error(seastar::format("Number of vertical shards {} exceeds canvas height {}", _canvas_properties.num_shards_vert, _canvas_properties.height));
            // }

            // _numRemainderRows = _canvas_properties.height % _canvas_properties.num_shards_vert;
            // if(seastar::this_shard_id() == 0) hlogger.debug("_numRemainderRows = {}", _numRemainderRows);

            // //Essentially row major
            // _thisShardHorizIndex = seastar::this_shard_id() % _canvas_properties.num_shards_horiz;
            // _thisShardVertIndex = seastar::this_shard_id() / _canvas_properties.num_shards_horiz;

            // _thisShardColMin = _thisShardHorizIndex * _minNumColsPerShard + std::min(_thisShardHorizIndex, _numRemainderCols);
            // //Not inclusive
            // _thisShardColMax = _thisShardColMin + _minNumColsPerShard + ((_thisShardHorizIndex < _numRemainderCols) ? 1 : 0);

            // _thisShardRowMin = _thisShardVertIndex * _minNumRowsPerShard + std::min(_thisShardVertIndex, _numRemainderRows);
            // //Not inclusive
            // _thisShardRowMax = _thisShardRowMin + _minNumRowsPerShard + ((_thisShardVertIndex < _numRemainderRows) ? 1 : 0);


            // hlogger.debug("I am at (h = {}, v = {}) and care of horiz [{}, {}), vert [{}, {})", _thisShardHorizIndex, _thisShardVertIndex, _thisShardColMin, _thisShardColMax, _thisShardRowMin, _thisShardRowMax);

            _canvasInited = true;
        }

        unsigned int canvas_server::getShardIndexForCoordinate(unsigned int horiz, unsigned int vert) {
            if(!_canvasInited) throw std::runtime_error("Cannot get shard Index for coordinate before canvas is initialised!");

            // For Matrix Sharding:
            // if(horiz >= _canvas_properties.width) throw std::out_of_range("Horiz Pixel Index out of range!");
            // if(vert >= _canvas_properties.height) throw std::out_of_range("Vert Pixel Index out of range!");

            // unsigned int horizIndex;
            // if(horiz > _numRemainderCols * (_minNumColsPerShard + 1)) {

            //     unsigned int temp = horiz - _numRemainderCols * (_minNumColsPerShard + 1);
                
            //     horizIndex = _numRemainderCols + temp / _minNumColsPerShard;
            // } else {
            //     horizIndex = horiz / (_minNumColsPerShard + 1);
            // }
            // if(horizIndex >= _canvas_properties.num_shards_horiz) {
            //     throw std::runtime_error(fmt::format("Internal error: horizIndex >= _canvas_properties.num_shards_horiz, horiz = {}, vert = {}, horizIndex = {}, numShards = {}", horiz, vert, horizIndex, _numShards));
            // }

            // unsigned int vertIndex;
            // if(vert > _numRemainderRows * (_minNumRowsPerShard + 1)) {

            //     unsigned int temp = vert - _numRemainderRows * (_minNumRowsPerShard + 1);
                
            //     vertIndex = _numRemainderCols + temp / _minNumRowsPerShard;
            // } else {
            //     vertIndex = vert / (_minNumRowsPerShard + 1);
            // }

            // unsigned int shardIndexForCoordinate = vertIndex * _canvas_properties.num_shards_horiz + horizIndex;
            // if(shardIndexForCoordinate >= _numShards) {
            //     throw std::runtime_error(fmt::format("Internal error: shardIndexForCoordinate >= _numShards, horiz = {}, vert = {}, horizIndex = {}, vertIndex = {}, shardIndexForCoordinate = {}, numShards = {}", horiz, vert, horizIndex, vertIndex, shardIndexForCoordinate, _numShards));
            // }

            if(horiz >= _canvas_properties.width) throw std::out_of_range("Horiz Pixel Index out of range!");
            if(vert >= _canvas_properties.height) throw std::out_of_range("Vert Pixel Index out of range!");
            
            unsigned int pixelIndex = vert * _canvas_properties.width + horiz;

            unsigned int shardIndex;
            if(pixelIndex > _numRemainingPixels * (_minNumPixelsPerShard + 1)) {
                unsigned int temp = pixelIndex - _numRemainingPixels * (_minNumPixelsPerShard + 1);

                shardIndex = _numRemainingPixels + temp / _minNumPixelsPerShard;
            } else {
                shardIndex = pixelIndex / (_minNumPixelsPerShard + 1);
            }
            if(shardIndex >= _numShards) throw std::runtime_error("Internal error: shardIndex >= _numShards");

            return shardIndex;
        }

        seastar::future<> canvas_server::listen(seastar::socket_address addr, seastar::listen_options lo) {
            if(!_canvasInited) {
                return seastar::make_exception_future(std::runtime_error("Cannot listen before Canvas was Initialised!"));
            }

            _listeners.push_back(seastar::listen(addr, lo));
            return do_accepts(_listeners.size() - 1);
        }

        seastar::future<> canvas_server::listen(seastar::socket_address addr) {
            seastar::listen_options lo;
            lo.reuse_address = true;
            return listen(addr, lo);
        }

        seastar::future<> canvas_server::stop() {
            seastar::future<> tasks_done = _task_gate.close();
            for (auto&& l : _listeners) {
                l.abort_accept();
            }
            for (auto&& c : _connections) {
                c.shutdown();
            }
            return tasks_done;
        }


        seastar::future<> canvas_server::do_accepts(int which){
            (void)seastar::try_with_gate(_task_gate, [this, which] {
                return seastar::keep_doing([this, which] {
                    return seastar::try_with_gate(_task_gate, [this, which] {
                        return do_accept_one(which);
                    });
                }).handle_exception_type([](const seastar::gate_closed_exception& e) {});
            }).handle_exception_type([](const seastar::gate_closed_exception& e) {});
            return seastar::make_ready_future<>();
        }

        seastar::future<> canvas_server::do_accept_one(int which) {
            return _listeners[which].accept().then([this] (seastar::accept_result ar) mutable {
                auto local_address = ar.connection.local_address();

                auto conn = std::make_unique<connection>(*this, std::move(ar.connection), std::move(ar.remote_address), std::move(local_address));

                (void)try_with_gate(_task_gate, [conn = std::move(conn)]() mutable {
                    return conn->process().handle_exception([conn = std::move(conn)] (std::exception_ptr ex) {
                        hlogger.error("request error: {}", ex);
                    });
                }).handle_exception_type([] (const seastar::gate_closed_exception& e) {});

            }).handle_exception_type([] (const std::system_error &e) {
                // We expect a ECONNABORTED when http_server::stop is called,
                // no point in warning about that.
                if (e.code().value() != ECONNABORTED) {
                    hlogger.error("accept failed: {}", e);
                }

            }).handle_exception([] (std::exception_ptr ex) {
                hlogger.error("accept failed: {}", ex);
            });
        }

        uint64_t canvas_server::total_connections() const {
            return _total_connections;
        }
        uint64_t canvas_server::current_connections() const {
            return _current_connections;
        }
        uint64_t canvas_server::requests_served() const {
            return _requests_served;
        }
        uint64_t canvas_server::read_errors() const {
            return _read_errors;
        }
        uint64_t canvas_server::reply_errors() const {
            return _respond_errors;
        }


        seastar::future<> canvas_server_control::start(seastar::sstring name, unsigned int numShards) {
            hlogger.set_level(seastar::log_level::debug);
            return _server_sharded->start(name, numShards);
        }

        seastar::future<> canvas_server_control::stop() {
            return _server_sharded->stop();
        }

        seastar::future<> canvas_server_control::initialiseCanvas(canvas_properties canvas_properties) {
            return _server_sharded->invoke_on_all([canvas_properties](canvas_server& server) {
                return seastar::futurize_invoke([canvas_properties](canvas_server& server){ server.initialiseCanvas(canvas_properties); }, server);
            });
        }

        seastar::future<> canvas_server_control::listen(seastar::socket_address addr) {
            return _server_sharded->invoke_on_all<seastar::future<> (canvas_server::*)(seastar::socket_address)>(&canvas_server::listen, addr);
        }

        seastar::future<> canvas_server_control::listen(seastar::socket_address addr, seastar::listen_options lo) {
            return _server_sharded->invoke_on_all<seastar::future<> (canvas_server::*)(seastar::socket_address, seastar::listen_options)>(&canvas_server::listen, addr, lo);
        }

        seastar::distributed<canvas_server>& canvas_server_control::server() {
            return *_server_sharded;
        }

    }

}