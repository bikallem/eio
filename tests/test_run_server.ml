open Eio

let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, 8081)

let read_all flow =
  let b = Buffer.create 100 in
  Eio.Flow.copy flow (Eio.Flow.buffer_sink b);
  Buffer.contents b

let eio_run_server ~max_conn env sw =
  let connection_handler clock flow _addr =
    traceln "Server accepted connection from client";
    Fun.protect (fun () ->
      let msg = read_all flow in
      traceln "Server received: %S" msg;
      Eio.Time.sleep clock 0.01
    ) ~finally:(fun () -> Eio.Flow.copy_string "Bye" flow)
  in
  let server_sock = Eio.Net.listen ~reuse_addr:true ~backlog:128 ~sw env#net addr in
  let connection_handler = connection_handler env#clock in
  let server () = 
    Eio.Net.run_server ~max_connections:max_conn ~additional_domains:(env#domain_mgr, 10)
      server_sock connection_handler
  in

(*
    let server () =
      traceln "starting server ..."; 
      Eio.Net.accept_fork ~sw server_sock ~on_error:raise connection_handler 
    in
*)
  server ()

let () =
  Printexc.record_backtrace true ;
  Eio_main.run @@ fun env ->
  Switch.run @@ fun sw ->
  eio_run_server ~max_conn:10 env sw
