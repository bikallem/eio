open Eio

let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, 8081)

let read_all flow =
  let b = Buffer.create 100 in
  Eio.Flow.copy flow (Eio.Flow.buffer_sink b);
  Buffer.contents b

let run_client env sw id () =
  traceln "client: Connecting to server ...%d" id;
  let flow = Eio.Net.connect ~sw env#net addr in
  Eio.Flow.copy_string "Hello from client" flow;
  Eio.Flow.shutdown flow `Send;
  let msg = read_all flow in
  traceln "client received: %S" msg
  
let () = 
  let clients = ref 60 in
  Arg.parse
    ["-clients", Arg.Set_int clients, " count of clients to create"]
    ignore "test_run_server";

  Eio_main.run @@ fun env ->
  Switch.run @@ fun sw ->
  let clients = List.init !clients (fun id -> run_client env sw (id+1)) in
  Fiber.all clients
