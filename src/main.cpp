#include <kataklysmos/canvas_server.hpp>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>

seastar::logger applog("Kataklysmos");

int main(int argc, char** argv) {

    
    applog.info("Starting up!");
    seastar::app_template app;



    app.run(argc, argv, []() {

        return seastar::async([]() {

            seastar::promise<> abortProm;
            auto abortFut = abortProm.get_future();

            seastar::engine().handle_signal(SIGTERM, [&abortProm] {
                applog.info("Received SIGTERM, Exiting...");
                //What happens if we set the value multiple times? Who knows...
                abortProm.set_value();
            });
            seastar::engine().handle_signal(SIGINT, [&abortProm] {
                applog.info("Received SIGINT, Exiting...");
                //What happens if we set the value multiple times? Who knows...
                abortProm.set_value();
            });

            auto all_cpus = seastar::smp::all_cpus();
            applog.info("All CPUs: num {}, front {}, back {}", all_cpus.size(), all_cpus.front(), all_cpus.back());

            seastar::sstring servername = "kataklysmos-server";

            kataklysmos::pixelflut::canvas_server_control server_control;
            auto startFut = server_control.start(servername, all_cpus.size());
            startFut.wait();
            
            if(startFut.failed()) {
                applog.error("Fatal error when allocating Pixelflut Server Shards: {}", startFut.get_exception());

                return;
            }
            startFut.ignore_ready_future();
            applog.info("Succesfully allocated Pixelflut Server Shards");


            kataklysmos::pixelflut::canvas_properties canvas_properties {
                123,
                456,
                //4,
                //4
            };

            auto canvas_init_fut = server_control.initialiseCanvas(canvas_properties);
            canvas_init_fut.wait();

            if(canvas_init_fut.failed()) {
                applog.error("Fatal error when setting canvas properties: {}", canvas_init_fut.get_exception());

                server_control.stop().wait();
                return;
            }
            canvas_init_fut.ignore_ready_future();
            applog.info("Succesfully initialised Canvas!");

            //Debug: Show Pixel to Shard assignment
            // auto datFut = server_control.server().invoke_on(0, [](kataklysmos::pixelflut::canvas_server& server) {
                
            //         const auto& canvas_properties = server.getCanvasProperties();

            //         for(unsigned int vert = 0; vert < canvas_properties.height; ++vert) {
            //             for(unsigned int horiz = 0; horiz < canvas_properties.width; ++horiz) {
            //                 std::cerr << server.getShardIndexForCoordinate(horiz, vert) << "\t";
            //             }
            //             std::cerr << "\n";
            //         }
               
            // });
            // if(datFut.failed()) {
            //     applog.error("Fatal error in test: {}", datFut.get_exception());

            //     server_control.stop().wait();
            //     return;
            // }
            
            auto listen_fut = server_control.listen(seastar::make_ipv4_address({1234}));
            listen_fut.wait();

            if(listen_fut.failed()) {
                applog.error("Fatal error when starting listening: {}", listen_fut.get_exception());

                server_control.stop().wait();
                return;
            }

            applog.info("Successfully started listening!");
            // server->start().then([server] {
            //     server->set_routes(set_routes);
            // }).then([server, port] {
            //     server->listen(port);
            // }).then([port] {
            //     std::cout << "Seastar HTTP server listening on port " << port << " ...\n";
            // });

            
            abortFut.wait();

            applog.info("Waiting for graceful shutdown of workers...");

            server_control.stop().wait();

            applog.info("All workers exited!");
        });

    });

    
}
