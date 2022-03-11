(*
 * Copyright (C) 2020-2021 Anil Madhavapeddy
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

let src = Logs.Src.create "eio_linux" ~doc:"Effect-based IO system for Linux/io-uring"
module Log = (val Logs.src_log src : Logs.LOG)

open Eio.Std
module Effect = Eio.Private.Effect

module Fiber_context = Eio.Private.Fiber_context
module Ctf = Eio.Private.Ctf

module Suspended = Eio_utils.Suspended
module Zzz = Eio_utils.Zzz
module Lf_queue = Eio_utils.Lf_queue

(* SIGPIPE makes no sense in a modern application. *)
let () = Sys.(set_signal sigpipe Signal_ignore)

type amount = Exactly of int | Upto of int

let system_thread = Ctf.mint_id ()

let wrap_errors path fn =
  try fn () with
  | Unix.Unix_error(Unix.EEXIST, _, _) as ex -> raise @@ Eio.Dir.Already_exists (path, ex)
  | Unix.Unix_error(Unix.ENOENT, _, _) as ex -> raise @@ Eio.Dir.Not_found (path, ex)
  | Unix.Unix_error(Unix.EXDEV, _, _)  as ex -> raise @@ Eio.Dir.Permission_denied (path, ex)
  | Eio.Dir.Permission_denied _        as ex -> raise @@ Eio.Dir.Permission_denied (path, ex)

type _ Effect.t += Close : Unix.file_descr -> int Effect.t

module FD = struct
  type t = {
    seekable : bool;
    close_unix : bool;                          (* Whether closing this also closes the underlying FD. *)
    mutable release_hook : Eio.Switch.hook;     (* Use this on close to remove switch's [on_release] hook. *)
    mutable fd : [`Open of Unix.file_descr | `Closed]
  }

  let get op = function
    | { fd = `Open fd; _ } -> fd
    | { fd = `Closed ; _ } -> invalid_arg (op ^ ": file descriptor used after calling close!")

  let is_open = function
    | { fd = `Open _; _ } -> true
    | { fd = `Closed; _ } -> false

  let close t =
    Ctf.label "close";
    let fd = get "close" t in
    t.fd <- `Closed;
    Eio.Switch.remove_hook t.release_hook;
    if t.close_unix then (
      let res = Effect.perform (Close fd) in
      Log.debug (fun l -> l "close: woken up");
      if res < 0 then
        raise (Unix.Unix_error (Uring.error_of_errno res, "close", string_of_int (Obj.magic fd : int)))
    )

  let ensure_closed t =
    if is_open t then close t

  let is_seekable fd =
    match Unix.lseek fd 0 Unix.SEEK_CUR with
    | (_ : int) -> true
    | exception Unix.Unix_error(Unix.ESPIPE, "lseek", "") -> false

  let to_unix op t =
    let fd = get "to_unix" t in
    match op with
    | `Peek -> fd
    | `Take ->
      t.fd <- `Closed;
      Eio.Switch.remove_hook t.release_hook;
      fd

  let of_unix_no_hook ~seekable ~close_unix fd =
    { seekable; close_unix; fd = `Open fd; release_hook = Eio.Switch.null_hook }

  let of_unix ~sw ~seekable ~close_unix fd =
    let t = of_unix_no_hook ~seekable ~close_unix fd in
    t.release_hook <- Switch.on_release_cancellable sw (fun () -> ensure_closed t);
    t

  let placeholder ~seekable ~close_unix =
    { seekable; close_unix; fd = `Closed; release_hook = Eio.Switch.null_hook }

  let uring_file_offset t =
    if t.seekable then Optint.Int63.minus_one else Optint.Int63.zero

  let pp f t =
    match t.fd with
    | `Open fd -> Fmt.pf f "%d" (Obj.magic fd : int)
    | `Closed -> Fmt.string f "(closed)"
end

type rw_req = {
  op : [`R|`W];
  file_offset : Optint.Int63.t;
  fd : FD.t;
  len : amount;
  buf : Uring.Region.chunk;
  mutable cur_off : int;
  action : int Suspended.t;
}

(* Type of user-data attached to jobs. *)
type io_job =
  | Read : rw_req -> io_job
  | Job_no_cancel : int Suspended.t -> io_job
  | Job : int Suspended.t -> io_job
  | Write : rw_req -> io_job
  | Job_fn : 'a Suspended.t * (int -> [`Exit_scheduler]) -> io_job
  (* When done, remove the cancel_fn from [Suspended.t] and call the callback (unless cancelled). *)

type runnable =
  | Thread : 'a Suspended.t * 'a -> runnable
  | Failed_thread : 'a Suspended.t * exn -> runnable

type t = {
  uring: io_job Uring.t;
  mem: Uring.Region.t option;
  io_q: (t -> unit) Queue.t;     (* waiting for room on [uring] *)
  mem_q : Uring.Region.chunk Suspended.t Queue.t;

  (* The queue of runnable fibers ready to be resumed. Note: other domains can also add work items here. *)
  run_q : runnable Lf_queue.t;

  (* When adding to [run_q] from another domain, this domain may be sleeping and so won't see the event.
     In that case, [need_wakeup = true] and you must signal using [eventfd]. You must hold [eventfd_mutex]
     when writing to or closing [eventfd]. *)
  eventfd : FD.t;
  eventfd_mutex : Mutex.t;

  (* If [false], the main thread will check [run_q] before sleeping again
     (possibly because an event has been or will be sent to [eventfd]).
     It can therefore be set to [false] in either of these cases:
     - By the receiving thread because it will check [run_q] before sleeping, or
     - By the sending thread because it will signal the main thread later *)
  need_wakeup : bool Atomic.t;

  sleep_q: Zzz.t;
  mutable io_jobs: int;
}

let wake_buffer =
  let b = Bytes.create 8 in
  Bytes.set_int64_ne b 0 1L;
  b

let wakeup t =
  Mutex.lock t.eventfd_mutex;
  match
    Log.debug (fun f -> f "Sending wakeup on eventfd %a" FD.pp t.eventfd);
    Atomic.set t.need_wakeup false; (* [t] will check [run_q] after getting the event below *)
    let sent = Unix.single_write (FD.get "wakeup" t.eventfd) wake_buffer 0 8 in
    assert (sent = 8)
  with
  | ()           -> Mutex.unlock t.eventfd_mutex
  | exception ex -> Mutex.unlock t.eventfd_mutex; raise ex

let enqueue_thread st k x =
  Lf_queue.push st.run_q (Thread (k, x));
  if Atomic.get st.need_wakeup then wakeup st

let enqueue_failed_thread st k ex =
  Lf_queue.push st.run_q (Failed_thread (k, ex));
  if Atomic.get st.need_wakeup then wakeup st

(* Can only be called from our own domain, so no need to check for wakeup. *)
let enqueue_at_head st k x =
  Lf_queue.push_head st.run_q (Thread (k, x))

type _ Effect.t += Enter_unchecked : (t -> 'a Suspended.t -> unit) -> 'a Effect.t
type _ Effect.t += Enter : (t -> 'a Suspended.t -> unit) -> 'a Effect.t
let enter fn = Effect.perform (Enter fn)

(* Cancellations always come from the same domain, so no need to send wake events here. *)
let rec enqueue_cancel job st action =
  Log.debug (fun l -> l "cancel: submitting call");
  Ctf.label "cancel";
  match Uring.cancel st.uring job (Job_no_cancel action) with
  | None -> Queue.push (fun st -> enqueue_cancel job st action) st.io_q
  | Some _ -> ()

let cancel job =
  let res = Effect.perform (Enter_unchecked (enqueue_cancel job)) in
  Log.debug (fun l -> l "cancel returned");
  if res = -2 then (
    Log.debug (fun f -> f "Cancel returned ENOENT - operation completed before cancel took effect")
  ) else if res = -114 then (
    Log.debug (fun f -> f "Cancel returned EALREADY - operation cancelled while already in progress")
  ) else if res <> 0 then (
    raise (Unix.Unix_error (Uring.error_of_errno res, "cancel", ""))
  )

(* Cancellation

   For operations that can be cancelled we need to set the fiber's cancellation function.
   The typical sequence is:

   1. We submit an operation, getting back a uring job (needed for cancellation).
   2. We set the cancellation function. The function uses the uring job to cancel.

   The cancellation function owns the uring job.

   When the job completes, we remove the cancellation function from the fiber.
   The function must have been set by this point because we don't poll for
   completions until the above steps have finished.

   If the context is cancelled while the operation is running, the function will get removed and called,
   which will submit a cancellation request to uring.
   If the operation completes before Linux processes the cancellation, we get [ENOENT], which we ignore.

   If the context is cancelled before starting then we discontinue the fiber. *)

(* [with_cancel_hook ~action st fn] calls [fn] to create a job,
   then sets the fiber's cancel function to cancel it.
   If [action] is already cancelled, it schedules [action] to be discontinued.
   @return Whether to retry the operation later, once there is space. *)
let with_cancel_hook ~action st fn =
  match Fiber_context.get_error action.Suspended.fiber with
  | Some ex -> enqueue_failed_thread st action ex; false
  | None ->
    match fn () with
    | None -> true
    | Some job ->
      Fiber_context.set_cancel_fn action.fiber (fun _ -> cancel job);
      false

let clear_cancel (action : _ Suspended.t) =
  (* It doesn't matter whether the operation was cancelled or not;
     there's nothing we need to do with the job now. *)
  ignore (Fiber_context.clear_cancel_fn action.fiber : bool)

let rec submit_rw_req st ({op; file_offset; fd; buf; len; cur_off; action} as req) =
  let fd = FD.get "submit_rw_req" fd in
  let {uring;io_q;_} = st in
  let off = Uring.Region.to_offset buf + cur_off in
  let len = match len with Exactly l | Upto l -> l in
  let len = len - cur_off in
  let retry = with_cancel_hook ~action st (fun () ->
      match op with
      |`R -> Uring.read_fixed uring ~file_offset fd ~off ~len (Read req)
      |`W -> Uring.write_fixed uring ~file_offset fd ~off ~len (Write req)
    )
  in
  if retry then (
    Ctf.label "await-sqe";
    (* wait until an sqe is available *)
    Queue.push (fun st -> submit_rw_req st req) io_q
  )

(* TODO bind from unixsupport *)
let errno_is_retry = function -62 | -11 | -4 -> true |_ -> false

let enqueue_read st action (file_offset,fd,buf,len) =
  let file_offset =
    match file_offset with
    | Some x -> x
    | None -> FD.uring_file_offset fd
  in
  let req = { op=`R; file_offset; len; fd; cur_off = 0; buf; action } in
  Log.debug (fun l -> l "read: submitting call");
  Ctf.label "read";
  submit_rw_req st req

let rec enqueue_readv args st action =
  let (file_offset,fd,bufs) = args in
  let file_offset =
    match file_offset with
    | Some x -> x
    | None -> FD.uring_file_offset fd
  in
  Ctf.label "readv";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.readv st.uring ~file_offset (FD.get "readv" fd) bufs (Job action))
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_readv args st action) st.io_q

let rec enqueue_writev args st action =
  let (file_offset,fd,bufs) = args in
  let file_offset =
    match file_offset with
    | Some x -> x
    | None -> FD.uring_file_offset fd
  in
  Ctf.label "writev";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.writev st.uring ~file_offset (FD.get "writev" fd) bufs (Job action)
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_writev args st action) st.io_q

let rec enqueue_poll_add fd poll_mask st action =
  Log.debug (fun l -> l "poll_add: submitting call");
  Ctf.label "poll_add";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.poll_add st.uring (FD.get "poll_add" fd) poll_mask (Job action)
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_poll_add fd poll_mask st action) st.io_q

let rec enqueue_poll_add_unix fd poll_mask st action cb =
  Log.debug (fun l -> l "poll_add: submitting call");
  Ctf.label "poll_add";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.poll_add st.uring fd poll_mask (Job_fn (action, cb))
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_poll_add_unix fd poll_mask st action cb) st.io_q

let rec enqueue_close st action fd =
  Log.debug (fun l -> l "close: submitting call");
  Ctf.label "close";
  let subm = Uring.close st.uring fd (Job_no_cancel action) in
  if subm = None then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_close st action fd) st.io_q

let enqueue_write st action (file_offset,fd,buf,len) =
  let file_offset =
    match file_offset with
    | Some x -> x
    | None -> FD.uring_file_offset fd
  in
  let req = { op=`W; file_offset; len; fd; cur_off = 0; buf; action } in
  Log.debug (fun l -> l "write: submitting call");
  Ctf.label "write";
  submit_rw_req st req

let rec enqueue_splice ~src ~dst ~len st action =
  Log.debug (fun l -> l "splice: submitting call");
  Ctf.label "splice";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.splice st.uring (Job action) ~src:(FD.get "splice" src) ~dst:(FD.get "splice" dst) ~len
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_splice ~src ~dst ~len st action) st.io_q

let rec enqueue_openat2 ((access, flags, perm, resolve, dir, path) as args) st action =
  Log.debug (fun l -> l "openat2: submitting call");
  Ctf.label "openat2";
  let fd = Option.map (FD.get "openat2") dir in
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.openat2 st.uring ~access ~flags ~perm ~resolve ?fd path (Job action)
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_openat2 args st action) st.io_q

let rec enqueue_connect fd addr st action =
  Log.debug (fun l -> l "connect: submitting call");
  Ctf.label "connect";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.connect st.uring (FD.get "connect" fd) addr (Job action)
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_connect fd addr st action) st.io_q

let rec enqueue_send_msg fd ~fds ~dst buf st action =
  Log.debug (fun l -> l "send_msg: submitting call");
  Ctf.label "send_msg";
  let retry = with_cancel_hook ~action st (fun () ->
      let fds = List.map (FD.get "send_msg") fds in
      Uring.send_msg st.uring (FD.get "send_msg" fd) ~fds ?dst buf (Job action)
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_send_msg fd ~fds ~dst buf st action) st.io_q

let rec enqueue_recv_msg fd msghdr st action =
  Log.debug (fun l -> l "recv_msg: submitting call");
  Ctf.label "recv_msg";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.recv_msg st.uring (FD.get "recv_msg" fd) msghdr (Job action);
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_recv_msg fd msghdr st action) st.io_q 

let rec enqueue_accept fd client_addr st action =
  Log.debug (fun l -> l "accept: submitting call");
  Ctf.label "accept";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.accept st.uring (FD.get "accept" fd) client_addr (Job action)
    ) in
  if retry then (
    (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_accept fd client_addr st action) st.io_q
  )

let rec enqueue_noop st action =
  Log.debug (fun l -> l "noop: submitting call");
  Ctf.label "noop";
  let retry = (Uring.noop st.uring (Job_no_cancel action) = None) in
  if retry then (
    (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_noop st action) st.io_q
  )

let submit_pending_io st =
  match Queue.take_opt st.io_q with
  | None -> ()
  | Some fn ->
    Ctf.label "submit_pending_io";
    fn st

(* Switch control to the next ready continuation.
   If none is ready, wait until we get an event to wake one and then switch.
   Returns only if there is nothing to do and no queued operations. *)
let rec schedule ({run_q; sleep_q; mem_q; uring; _} as st) : [`Exit_scheduler] =
  (* This is not a fair scheduler *)
  (* Wakeup any paused fibers *)
  match Lf_queue.pop run_q with
  | Some Thread (k, v) -> Suspended.continue k v               (* We already have a runnable task *)
  | Some Failed_thread (k, ex) -> Suspended.discontinue k ex
  | None ->
    let now = Unix.gettimeofday () in
    match Zzz.pop ~now sleep_q with
    | `Due k -> Suspended.continue k ()                   (* A sleeping task is now due *)
    | `Wait_until _ | `Nothing as next_due ->
      (* Handle any pending events before submitting. This is faster. *)
      match Uring.peek uring with
      | Some { data = runnable; result } -> handle_complete st ~runnable result
      | None ->
        let num_jobs = Uring.submit uring in
        st.io_jobs <- st.io_jobs + num_jobs;
        let timeout =
          match next_due with
          | `Wait_until time -> Some (time -. now)
          | `Nothing -> None
        in
        Log.debug (fun l -> l "scheduler: %d sub / %d total, timeout %s" num_jobs st.io_jobs
                      (match timeout with None -> "inf" | Some v -> string_of_float v));
        if timeout = None && st.io_jobs = 0 then (
          (* Nothing further can happen at this point.
             If there are no events in progress but also still no memory available, something has gone wrong! *)
          assert (Queue.length mem_q = 0);
          Log.debug (fun l -> l "schedule: exiting");    (* Nothing left to do *)
          Lf_queue.close st.run_q;      (* Just to catch bugs if something tries to enqueue later *)
          `Exit_scheduler
        ) else (
          Atomic.set st.need_wakeup true;
          if Lf_queue.is_empty st.run_q then (
            (* At this point we're not going to check [run_q] again before sleeping.
               If [need_wakeup] is still [true], this is fine because we don't promise to do that.
               If [need_wakeup = false], a wake-up event will arrive and wake us up soon. *)
            Ctf.(note_hiatus Wait_for_work);
            let result = Uring.wait ?timeout uring in
            Ctf.note_resume system_thread;
            Atomic.set st.need_wakeup false;
            match result with
            | None ->
              (* Woken by a timeout, which is now due, or by a signal. *)
              schedule st
            | Some { data = runnable; result } ->
              handle_complete st ~runnable result
          ) else (
            (* Someone added a new job while we were setting [need_wakeup] to [true].
               They might or might not have seen that, so we can't be sure they'll send an event. *)
            Atomic.set st.need_wakeup false;
            schedule st
          )
        )
and handle_complete st ~runnable result =
  st.io_jobs <- st.io_jobs - 1;
  submit_pending_io st;                       (* If something was waiting for a slot, submit it now. *)
  match runnable with
  | Read req ->
    Log.debug (fun l -> l "read returned");
    clear_cancel req.action;
    complete_rw_req st req result
  | Write req ->
    Log.debug (fun l -> l "write returned");
    clear_cancel req.action;
    complete_rw_req st req result
  | Job k ->
    clear_cancel k;
    begin match Fiber_context.get_error k.fiber with
      | None -> Suspended.continue k result
      | Some e ->
        (* If cancelled, report that instead.
           Should we only do this on error, to avoid losing the return value?
           We already do that with rw jobs. *)
        Suspended.discontinue k e
    end
  | Job_no_cancel k ->
    Suspended.continue k result
  | Job_fn (k, f) ->
    clear_cancel k;
    begin match Fiber_context.get_error k.fiber with
      | None -> f result
      | Some e -> Suspended.discontinue k e
    end
and complete_rw_req st ({len; cur_off; action; _} as req) res =
  match res, len with
  | 0, _ -> Suspended.discontinue action End_of_file
  | e, _ when e < 0 ->
    begin match Fiber_context.get_error action.fiber with
      | Some e -> Suspended.discontinue action e        (* If cancelled, report that instead. *)
      | None ->
        if errno_is_retry e then (
          submit_rw_req st req;
          schedule st
        ) else (
          Suspended.continue action e
        )
    end
  | n, Exactly len when n < len - cur_off ->
    req.cur_off <- req.cur_off + n;
    submit_rw_req st req;
    schedule st
  | _, Exactly len -> Suspended.continue action len
  | n, Upto _ -> Suspended.continue action n

module Low_level = struct
  let alloc_buf_or_wait st k =
    match st.mem with
    | None -> Suspended.discontinue k (Failure "No fixed buffer available")
    | Some mem ->
      Log.debug (fun l -> l "alloc: %d" (Uring.Region.avail mem));
      match Uring.Region.alloc mem with
      | buf -> Suspended.continue k buf
      | exception Uring.Region.No_space ->
        Queue.push k st.mem_q;
        schedule st

  let free_buf st buf =
    match Queue.take_opt st.mem_q with
    | None -> Uring.Region.free buf
    | Some k -> enqueue_thread st k buf

  let noop () =
    let result = enter enqueue_noop in
    Log.debug (fun l -> l "noop returned");
    if result <> 0 then raise (Unix.Unix_error (Uring.error_of_errno result, "noop", ""))

  type _ Effect.t += Sleep_until : float -> unit Effect.t
  let sleep_until d =
    Effect.perform (Sleep_until d)

  type _ Effect.t += ERead : (Optint.Int63.t option * FD.t * Uring.Region.chunk * amount) -> int Effect.t

  let read_exactly ?file_offset fd buf len =
    let res = Effect.perform (ERead (file_offset, fd, buf, Exactly len)) in
    Log.debug (fun l -> l "read_exactly: woken up after read");
    if res < 0 then (
      raise (Unix.Unix_error (Uring.error_of_errno res, "read_exactly", ""))
    )

  let read_upto ?file_offset fd buf len =
    let res = Effect.perform (ERead (file_offset, fd, buf, Upto len)) in
    Log.debug (fun l -> l "read_upto: woken up after read");
    if res < 0 then (
      let err = Uring.error_of_errno res in
      let ex = Unix.Unix_error (err, "read_upto", "") in
      if err = Unix.ECONNRESET then raise (Eio.Net.Connection_reset ex)
      else raise ex
    ) else (
      res
    )

  let readv ?file_offset fd bufs =
    let res = enter (enqueue_readv (file_offset, fd, bufs)) in
    Log.debug (fun l -> l "readv: woken up after read");
    if res < 0 then (
      raise (Unix.Unix_error (Uring.error_of_errno res, "readv", ""))
    ) else if res = 0 then (
      raise End_of_file
    ) else (
      res
    )

  let rec writev ?file_offset fd bufs =
    let res = enter (enqueue_writev (file_offset, fd, bufs)) in
    Log.debug (fun l -> l "writev: woken up after write");
    if res < 0 then (
      raise (Unix.Unix_error (Uring.error_of_errno res, "writev", ""))
    ) else (
      match Cstruct.shiftv bufs res with
      | [] -> ()
      | bufs ->
        let file_offset =
          let module I63 = Optint.Int63 in
          match file_offset with
          | None -> None
          | Some ofs when ofs = I63.minus_one -> Some I63.minus_one
          | Some ofs -> Some (I63.add ofs (I63.of_int res))
        in
        writev ?file_offset fd bufs
    )

  let await_readable fd =
    let res = enter (enqueue_poll_add fd (Uring.Poll_mask.(pollin + pollerr))) in
    Log.debug (fun l -> l "await_readable: woken up");
    if res < 0 then (
      raise (Unix.Unix_error (Uring.error_of_errno res, "await_readable", ""))
    )

  let await_writable fd =
    let res = enter (enqueue_poll_add fd (Uring.Poll_mask.(pollout + pollerr))) in
    Log.debug (fun l -> l "await_writable: woken up");
    if res < 0 then (
      raise (Unix.Unix_error (Uring.error_of_errno res, "await_writable", ""))
    )

  type _ Effect.t += EWrite : (Optint.Int63.t option * FD.t * Uring.Region.chunk * amount) -> int Effect.t

  let write ?file_offset fd buf len =
    let res = Effect.perform (EWrite (file_offset, fd, buf, Exactly len)) in
    Log.debug (fun l -> l "write: woken up after write");
    if res < 0 then (
      raise (Unix.Unix_error (Uring.error_of_errno res, "write", ""))
    )

  type _ Effect.t += Alloc : Uring.Region.chunk option Effect.t
  let alloc_fixed () = Effect.perform Alloc

  type _ Effect.t += Alloc_or_wait : Uring.Region.chunk Effect.t
  let alloc_fixed_or_wait () = Effect.perform Alloc_or_wait

  type _ Effect.t += Free : Uring.Region.chunk -> unit Effect.t
  let free_fixed buf = Effect.perform (Free buf)

  let splice src ~dst ~len =
    let res = enter (enqueue_splice ~src ~dst ~len) in
    Log.debug (fun l -> l "splice returned");
    if res > 0 then res
    else if res = 0 then raise End_of_file
    else raise (Unix.Unix_error (Uring.error_of_errno res, "splice", ""))

  let connect fd addr =
    let res = enter (enqueue_connect fd addr) in
    Log.debug (fun l -> l "connect returned");
    if res < 0 then (
      raise (Unix.Unix_error (Uring.error_of_errno res, "connect", ""))
    )

  let send_msg fd ?(fds=[]) ?dst buf =
    let res = enter (enqueue_send_msg fd ~fds ~dst buf) in
    Log.debug (fun l -> l "send_msg returned");
    if res < 0 then (
      raise (Unix.Unix_error (Uring.error_of_errno res, "send_msg", ""))
    )

  let recv_msg fd buf =
    let addr = Uring.Sockaddr.create () in
    let msghdr = Uring.Msghdr.create ~addr buf in
    let res = enter (enqueue_recv_msg fd msghdr) in
    Log.debug (fun l -> l "recv_msg returned");
    if res < 0 then (
      raise (Unix.Unix_error (Uring.error_of_errno res, "recv_msg", ""))
    );
    addr, res

  let recv_msg_with_fds ~sw ~max_fds fd buf =
    let addr = Uring.Sockaddr.create () in
    let msghdr = Uring.Msghdr.create ~n_fds:max_fds ~addr buf in
    let res = enter (enqueue_recv_msg fd msghdr) in
    Log.debug (fun l -> l "recv_msg returned");
    if res < 0 then (
      raise (Unix.Unix_error (Uring.error_of_errno res, "recv_msg", ""))
    );
    let fds =
      Uring.Msghdr.get_fds msghdr
      |> List.map (fun fd -> FD.of_unix ~sw ~seekable:(FD.is_seekable fd) ~close_unix:true fd)
    in
    addr, res, fds

  let with_chunk ~fallback fn =
    match alloc_fixed () with
    | Some chunk ->
      Fun.protect ~finally:(fun () -> free_fixed chunk) @@ fun () ->
      fn chunk
    | None ->
      fallback ()

  let openfile ~sw path flags mode =
    let fd = Unix.openfile path flags mode in
    FD.of_unix ~sw ~seekable:(FD.is_seekable fd) ~close_unix:true fd

  let openat2 ~sw ?seekable ~access ~flags ~perm ~resolve ?dir path =
    wrap_errors path @@ fun () ->
    let res = enter (enqueue_openat2 (access, flags, perm, resolve, dir, path)) in
    Log.debug (fun l -> l "openat2 returned");
    if res < 0 then (
      Switch.check sw;    (* If cancelled, report that instead. *)
      raise @@ Unix.Unix_error (Uring.error_of_errno res, "openat2", "")
    );
    let fd : Unix.file_descr = Obj.magic res in
    let seekable =
      match seekable with
      | None -> FD.is_seekable fd
      | Some x -> x
    in
    FD.of_unix ~sw ~seekable ~close_unix:true fd

  let fstat fd =
    Unix.fstat (FD.get "fstat" fd)

  external eio_mkdirat : Unix.file_descr -> string -> Unix.file_perm -> unit = "caml_eio_mkdirat"

  external eio_getrandom : Cstruct.buffer -> int -> int -> int = "caml_eio_getrandom"

  let getrandom { Cstruct.buffer; off; len } =
    eio_getrandom buffer off len

  (* We ignore [sw] because this isn't a uring operation yet. *)
  let mkdirat ~perm dir path =
    wrap_errors path @@ fun () ->
    match dir with
    | None -> Unix.mkdir path perm
    | Some dir -> eio_mkdirat (FD.get "mkdirat" dir) path perm

  let mkdir_beneath ~perm ?dir path =
    let dir_path = Filename.dirname path in
    let leaf = Filename.basename path in
    (* [mkdir] is really an operation on [path]'s parent. Get a reference to that first: *)
    Switch.run (fun sw ->
        let parent =
          wrap_errors path @@ fun () ->
          openat2 ~sw ~seekable:false ?dir dir_path
            ~access:`R
            ~flags:Uring.Open_flags.(cloexec + path + directory)
            ~perm:0
            ~resolve:Uring.Resolve.beneath
        in
        mkdirat ~perm (Some parent) leaf
      )

  let shutdown socket command =
    Unix.shutdown (FD.get "shutdown" socket) command

  let accept ~sw fd =
    Ctf.label "accept";
    let client_addr = Uring.Sockaddr.create () in
    let res = enter (enqueue_accept fd client_addr) in
    Log.debug (fun l -> l "accept returned");
    if res < 0 then (
      raise (Unix.Unix_error (Uring.error_of_errno res, "accept", ""))
    ) else (
      let unix : Unix.file_descr = Obj.magic res in
      let client = FD.of_unix ~sw ~seekable:false ~close_unix:true unix in
      let client_addr = Uring.Sockaddr.get client_addr in
      client, client_addr
    )
end

external eio_eventfd : int -> Unix.file_descr = "caml_eio_eventfd"

type _ Eio.Generic.ty += FD : FD.t Eio.Generic.ty

type has_fd = < fd : FD.t >
type source = < Eio.Flow.source; Eio.Flow.close; has_fd >
type sink   = < Eio.Flow.sink  ; Eio.Flow.close; has_fd >

let get_fd (t : <has_fd; ..>) = t#fd

let get_fd_opt t = Eio.Generic.probe t FD

(* When copying between a source with an FD and a sink with an FD, we can share the chunk
   and avoid copying. *)
let fast_copy src dst =
  let fallback () =
    (* No chunks available. Use regular memory instead. *)
    let buf = Cstruct.create 4096 in
    try
      while true do
        let got = Low_level.readv src [buf] in
        Low_level.writev dst [Cstruct.sub buf 0 got]
      done
    with End_of_file -> ()
  in
  Low_level.with_chunk ~fallback @@ fun chunk ->
  let chunk_size = Uring.Region.length chunk in
  try
    while true do
      let got = Low_level.read_upto src chunk chunk_size in
      Low_level.write dst chunk got
    done
  with End_of_file -> ()

(* Try a fast copy using splice. If the FDs don't support that, switch to copying. *)
let fast_copy_try_splice src dst =
  try
    while true do
      let _ : int = Low_level.splice src ~dst ~len:max_int in
      ()
    done
  with
  | End_of_file -> ()
  | Unix.Unix_error (Unix.EINVAL, "splice", _) -> fast_copy src dst

(* Copy using the [Read_source_buffer] optimisation.
   Avoids a copy if the source already has the data. *)
let copy_with_rsb rsb dst =
  try
    while true do
      rsb (Low_level.writev dst)
    done
  with End_of_file -> ()

(* Copy by allocating a chunk from the pre-shared buffer and asking
   the source to write into it. This used when the other methods
   aren't available. *)
let fallback_copy src dst =
  let fallback () =
    (* No chunks available. Use regular memory instead. *)
    let buf = Cstruct.create 4096 in
    try
      while true do
        let got = Eio.Flow.read src buf in
        Low_level.writev dst [Cstruct.sub buf 0 got]
      done
    with End_of_file -> ()
  in
  Low_level.with_chunk ~fallback @@ fun chunk ->
  let chunk_cs = Uring.Region.to_cstruct chunk in
  try
    while true do
      let got = Eio.Flow.read src chunk_cs in
      Low_level.write dst chunk got
    done
  with End_of_file -> ()

let udp_socket sock = object
  inherit Eio.Net.datagram_socket

  method send sockaddr buf = 
    let addr = match sockaddr with 
      | `Udp (host, port) -> 
        let host = Eio_unix.Ipaddr.to_unix host in
        Unix.ADDR_INET (host, port)
    in
    Low_level.send_msg sock ~dst:addr [buf] 
  
  method recv buf =
    let addr, recv = Low_level.recv_msg sock [buf] in
    match Uring.Sockaddr.get addr with
      | Unix.ADDR_INET (inet, port) ->
        `Udp (Eio_unix.Ipaddr.of_unix inet, port), recv
      | Unix.ADDR_UNIX _ -> 
        raise (Failure "Expected INET UDP socket address but got Unix domain socket address.")
end

let flow fd =
  let is_tty = lazy (Unix.isatty (FD.get "isatty" fd)) in
  object (_ : <source; sink; ..>)
    method fd = fd
    method close = FD.close fd

    method probe : type a. a Eio.Generic.ty -> a option = function
      | FD -> Some fd
      | Eio_unix.Private.Unix_file_descr op -> Some (FD.to_unix op fd)
      | _ -> None

    method read_into buf =
      if Lazy.force is_tty then (
        (* Work-around for https://github.com/axboe/liburing/issues/354
           (should be fixed in Linux 5.14) *)
        Low_level.await_readable fd
      );
      Low_level.readv fd [buf]

    method read_methods = []

    method copy src =
      match get_fd_opt src with
      | Some src -> fast_copy_try_splice src fd
      | None ->
        let rec aux = function
          | Eio.Flow.Read_source_buffer rsb :: _ -> copy_with_rsb rsb fd
          | _ :: xs -> aux xs
          | [] -> fallback_copy src fd
        in
        aux (Eio.Flow.read_methods src)

    method shutdown cmd =
      Unix.shutdown (FD.get "shutdown" fd) @@ match cmd with
      | `Receive -> Unix.SHUTDOWN_RECEIVE
      | `Send -> Unix.SHUTDOWN_SEND
      | `All -> Unix.SHUTDOWN_ALL
  end

let source fd = (flow fd :> source)
let sink   fd = (flow fd :> sink)

let listening_socket fd = object
  inherit Eio.Net.listening_socket

  method! probe : type a. a Eio.Generic.ty -> a option = function
    | Eio_unix.Private.Unix_file_descr op -> Some (FD.to_unix op fd)
    | _ -> None

  method close = FD.close fd

  method accept ~sw =
    Switch.check sw;
    let client, client_addr = Low_level.accept ~sw fd in
    let client_addr = match client_addr with
      | Unix.ADDR_UNIX path         -> `Unix path
      | Unix.ADDR_INET (host, port) -> `Tcp (Eio_unix.Ipaddr.of_unix host, port)
    in
    let flow = (flow client :> <Eio.Flow.two_way; Eio.Flow.close>) in
    flow, client_addr
end

let net = object
  inherit Eio.Net.t

  method listen ~reuse_addr ~reuse_port  ~backlog ~sw listen_addr =
    let socket_domain, socket_type, addr =
      match listen_addr with
      | `Unix path         ->
        if reuse_addr then (
          match Unix.lstat path with
          | Unix.{ st_kind = S_SOCK; _ } -> Unix.unlink path
          | _ -> ()
          | exception Unix.Unix_error (Unix.ENOENT, _, _) -> ()
        );
        Unix.PF_UNIX, Unix.SOCK_STREAM, Unix.ADDR_UNIX path
      | `Tcp (host, port)  ->
        let host = Eio_unix.Ipaddr.to_unix host in
        Unix.PF_INET, Unix.SOCK_STREAM, Unix.ADDR_INET (host, port)
    in
    let sock_unix = Unix.socket socket_domain socket_type 0 in
    (* For Unix domain sockets, remove the path when done (except for abstract sockets). *)
    begin match listen_addr with
      | `Unix path ->
        if String.length path > 0 && path.[0] <> Char.chr 0 then
          Switch.on_release sw (fun () -> Unix.unlink path)
      | `Tcp _ -> 
        Unix.setsockopt sock_unix Unix.TCP_NODELAY true;
        ()
    end;
    if reuse_addr then
      Unix.setsockopt sock_unix Unix.SO_REUSEADDR true;
    if reuse_port then
      Unix.setsockopt sock_unix Unix.SO_REUSEPORT true;

    let sock = FD.of_unix ~sw ~seekable:false ~close_unix:true sock_unix in
    Unix.bind sock_unix addr;
    Unix.listen sock_unix backlog;
    listening_socket sock

  method connect ~sw addr =
    let socket_domain, socket_type, addr =
      match addr with
      | `Unix path         -> Unix.PF_UNIX, Unix.SOCK_STREAM, Unix.ADDR_UNIX path
      | `Tcp (host, port)  ->
        let host = Eio_unix.Ipaddr.to_unix host in
        Unix.PF_INET, Unix.SOCK_STREAM, Unix.ADDR_INET (host, port)
    in
    let sock_unix = Unix.socket socket_domain socket_type 0 in
    let sock = FD.of_unix ~sw ~seekable:false ~close_unix:true sock_unix in
    Low_level.connect sock addr;
    (flow sock :> <Eio.Flow.two_way; Eio.Flow.close>)

  method datagram_socket ~sw = function
    | `Udp (host, port) ->
      let host = Eio_unix.Ipaddr.to_unix host in
      let addr = Unix.ADDR_INET (host, port) in
      let sock_unix = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
      Unix.setsockopt sock_unix Unix.SO_REUSEADDR true;
      Unix.setsockopt sock_unix Unix.SO_REUSEPORT true;
      let sock = FD.of_unix ~sw ~seekable:false ~close_unix:true sock_unix in
      Unix.bind sock_unix addr;
      udp_socket sock
end

type stdenv = <
  stdin  : source;
  stdout : sink;
  stderr : sink;
  net : Eio.Net.t;
  domain_mgr : Eio.Domain_manager.t;
  clock : Eio.Time.clock;
  fs : Eio.Dir.t;
  cwd : Eio.Dir.t;
  secure_random : Eio.Flow.source;
>

let domain_mgr ~run_event_loop = object (self)
  inherit Eio.Domain_manager.t

  method run_raw fn =
    let domain = ref None in
    enter (fun t k ->
        domain := Some (Domain.spawn (fun () -> Fun.protect fn ~finally:(fun () -> enqueue_thread t k ())))
      );
    Domain.join (Option.get !domain)

  method run fn =
    self#run_raw (fun () ->
        let result = ref None in
        run_event_loop (fun _ -> result := Some (fn ()));
        Option.get !result
      )
end

let clock = object
  inherit Eio.Time.clock

  method now = Unix.gettimeofday ()
  method sleep_until = Low_level.sleep_until
end

class dir fd = object
  inherit Eio.Dir.t

  val resolve_flags = Uring.Resolve.beneath

  method open_in ~sw path =
    let fd = Low_level.openat2 ~sw ?dir:fd path
        ~access:`R
        ~flags:Uring.Open_flags.cloexec
        ~perm:0
        ~resolve:resolve_flags
    in
    (flow fd :> <Eio.Flow.source; Eio.Flow.close>)

  method open_out ~sw ~append ~create path =
    let perm, flags =
      match create with
      | `Never            -> 0,    Uring.Open_flags.empty
      | `If_missing  perm -> perm, Uring.Open_flags.creat
      | `Or_truncate perm -> perm, Uring.Open_flags.(creat + trunc)
      | `Exclusive   perm -> perm, Uring.Open_flags.(creat + excl)
    in
    let flags = if append then Uring.Open_flags.(flags + append) else flags in
    let fd = Low_level.openat2 ~sw ?dir:fd path
        ~access:`RW
        ~flags:Uring.Open_flags.(cloexec + flags)
        ~perm
        ~resolve:resolve_flags
    in
    (flow fd :> <Eio.Dir.rw; Eio.Flow.close>)

  method open_dir ~sw path =
    let fd = Low_level.openat2 ~sw ~seekable:false ?dir:fd path
        ~access:`R
        ~flags:Uring.Open_flags.(cloexec + path + directory)
        ~perm:0
        ~resolve:resolve_flags
    in
    (new dir (Some fd) :> <Eio.Dir.t; Eio.Flow.close>)

  method mkdir ~perm path =
    Low_level.mkdir_beneath ~perm ?dir:fd path

  method close =
    FD.close (Option.get fd)
end

(* Full access to the filesystem. *)
let fs = object
  inherit dir None

  val! resolve_flags = Uring.Resolve.empty

  method! mkdir ~perm path =
    Low_level.mkdirat ~perm None path
end

let secure_random = object
  inherit Eio.Flow.source
  method read_into buf = Low_level.getrandom buf
end

let stdenv ~run_event_loop =
  let of_unix fd = FD.of_unix_no_hook ~seekable:(FD.is_seekable fd) ~close_unix:true fd in
  let stdin = lazy (source (of_unix Unix.stdin)) in
  let stdout = lazy (sink (of_unix Unix.stdout)) in
  let stderr = lazy (sink (of_unix Unix.stderr)) in
  let cwd = new dir None in
  object (_ : stdenv)
    method stdin  = Lazy.force stdin
    method stdout = Lazy.force stdout
    method stderr = Lazy.force stderr
    method net = net
    method domain_mgr = domain_mgr ~run_event_loop
    method clock = clock
    method fs = (fs :> Eio.Dir.t)
    method cwd = (cwd :> Eio.Dir.t)
    method secure_random = secure_random
  end

let pipe sw =
  let r, w = Unix.pipe () in
  let r = source (FD.of_unix ~sw ~seekable:false ~close_unix:true r) in
  let w = sink (FD.of_unix ~sw ~seekable:false ~close_unix:true w) in
  r, w

let monitor_event_fd t =
  let buf = Cstruct.create 8 in
  while true do
    let got = Low_level.readv t.eventfd [buf] in
    Log.debug (fun f -> f "Received wakeup on eventfd %a" FD.pp t.eventfd);
    assert (got = 8);
    (* We just go back to sleep now, but this will cause the scheduler to look
       at the run queue again and notice any new items. *)
  done

let no_fallback (`Msg msg) = failwith msg

(* Don't use [Fun.protect] - it throws away the original exception! *)
let with_uring ~queue_depth ?polling_timeout ?(fallback=no_fallback) fn =
  match Uring.create ~queue_depth ?polling_timeout () with
  | exception Unix.Unix_error(Unix.ENOSYS, _, _) -> fallback (`Msg "io_uring is not available on this system")
  | uring ->
    match fn uring with
    | x -> Uring.exit uring; x
    | exception ex ->
      let bt = Printexc.get_raw_backtrace () in
      begin
        try Uring.exit uring
        with ex2 -> Log.warn (fun f -> f "Uring.exit failed (%a) while handling another error" Fmt.exn ex2)
      end;
      Printexc.raise_with_backtrace ex bt

let rec run ?(queue_depth=64) ?n_blocks ?(block_size=4096) ?polling_timeout ?fallback main =
  Log.debug (fun l -> l "starting run");
  let n_blocks = Option.value n_blocks ~default:queue_depth in
  let stdenv = stdenv ~run_event_loop:(run ~queue_depth ~n_blocks ~block_size ?polling_timeout ?fallback:None) in
  (* TODO unify this allocation API around baregion/uring *)
  with_uring ~queue_depth ?polling_timeout ?fallback @@ fun uring ->
  let mem =
    let fixed_buf_len = block_size * n_blocks in
    let buf = Bigarray.(Array1.create char c_layout fixed_buf_len) in
    match Uring.set_fixed_buffer uring buf with
    | Ok () ->
      Some (Uring.Region.init ~block_size buf n_blocks)
    | Error `ENOMEM ->
      Log.warn (fun f -> f "Failed to allocate %d byte fixed buffer" fixed_buf_len);
      None
  in
  let run_q = Lf_queue.create () in
  let eventfd_mutex = Mutex.create () in
  let sleep_q = Zzz.create () in
  let io_q = Queue.create () in
  let mem_q = Queue.create () in
  let eventfd = FD.placeholder ~seekable:false ~close_unix:false in
  let st = { mem; uring; run_q; eventfd_mutex; io_q; mem_q; eventfd; need_wakeup = Atomic.make false; sleep_q; io_jobs = 0 } in
  Log.debug (fun l -> l "starting main thread");
  let rec fork ~new_fiber:fiber fn =
    let open Effect.Deep in
    Ctf.note_switch (Fiber_context.tid fiber);
    match_with fn ()
      { retc = (fun () -> Fiber_context.destroy fiber; schedule st);
        exnc = (fun ex ->
            Fiber_context.destroy fiber;
            Printexc.raise_with_backtrace ex (Printexc.get_raw_backtrace ())
          );
        effc = fun (type a) (e : a Effect.t) ->
          match e with
          | Enter fn -> Some (fun k ->
              match Fiber_context.get_error fiber with
              | Some e -> discontinue k e
              | None ->
                let k = { Suspended.k; fiber } in
                fn st k;
                schedule st
            )
          | Enter_unchecked fn -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              fn st k;
              schedule st
            )
          | Low_level.ERead args -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              enqueue_read st k args;
              schedule st)
          | Close fd -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              enqueue_close st k fd;
              schedule st
            )
          | Low_level.EWrite args -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              enqueue_write st k args;
              schedule st
            )
          | Low_level.Sleep_until time -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              match Fiber_context.get_error fiber with
              | Some ex -> Suspended.discontinue k ex
              | None ->
                let job = Zzz.add sleep_q time k in
                Fiber_context.set_cancel_fn fiber (fun ex ->
                    Zzz.remove sleep_q job;
                    enqueue_failed_thread st k ex
                  );
                schedule st
            )
          | Eio.Private.Effects.Get_context -> Some (fun k -> continue k fiber)
          | Eio.Private.Effects.Suspend f -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              f fiber (function
                  | Ok v -> enqueue_thread st k v
                  | Error ex -> enqueue_failed_thread st k ex
                );
              schedule st
            )
          | Eio.Private.Effects.Fork (new_fiber, f) -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              enqueue_at_head st k ();
              fork ~new_fiber f
            )
          | Eio.Private.Effects.Trace -> Some (fun k -> continue k Eio_utils.Trace.default_traceln)
          | Eio_unix.Private.Await_readable fd -> Some (fun k ->
              match Fiber_context.get_error fiber with
              | Some e -> discontinue k e
              | None ->
                let k = { Suspended.k; fiber } in
                enqueue_poll_add_unix fd Uring.Poll_mask.(pollin + pollerr) st k (fun res ->
                    if res >= 0 then Suspended.continue k ()
                    else Suspended.discontinue k (Unix.Unix_error (Uring.error_of_errno res, "await_readable", ""))
                  );
                schedule st
            )
          | Eio_unix.Private.Await_writable fd -> Some (fun k ->
              match Fiber_context.get_error fiber with
              | Some e -> discontinue k e
              | None ->
                let k = { Suspended.k; fiber } in
                enqueue_poll_add_unix fd Uring.Poll_mask.(pollout + pollerr) st k (fun res ->
                    if res >= 0 then Suspended.continue k ()
                    else Suspended.discontinue k (Unix.Unix_error (Uring.error_of_errno res, "await_writable", ""))
                  );
                schedule st
            )
          | Eio_unix.Private.Get_system_clock -> Some (fun k -> continue k clock)
          | Eio_unix.Private.Socket_of_fd (sw, close_unix, fd) -> Some (fun k ->
              let fd = FD.of_unix ~sw ~seekable:false ~close_unix fd in
              continue k (flow fd :> < Eio.Flow.two_way; Eio.Flow.close >)
            )
          | Low_level.Alloc -> Some (fun k ->
              match st.mem with
              | None -> continue k None
              | Some mem ->
                match Uring.Region.alloc mem with
                | buf -> continue k (Some buf)
                | exception Uring.Region.No_space -> continue k None
            )
          | Low_level.Alloc_or_wait -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              Low_level.alloc_buf_or_wait st k
            )
          | Low_level.Free buf -> Some (fun k ->
              Low_level.free_buf st buf;
              continue k ()
            )
          | _ -> None
      }
  in
  let `Exit_scheduler =
    let new_fiber = Fiber_context.make_root () in
    fork ~new_fiber (fun () ->
        Switch.run_protected (fun sw ->
            let fd = eio_eventfd 0 in
            st.eventfd.fd <- `Open fd;
            Switch.on_release sw (fun () ->
                Mutex.lock st.eventfd_mutex;
                FD.close st.eventfd;
                Mutex.unlock st.eventfd_mutex;
                Unix.close fd
              );
            Log.debug (fun f -> f "Monitoring eventfd %a" FD.pp st.eventfd);
            Fiber.first
              (fun () -> main stdenv)
              (fun () -> monitor_event_fd st)
          )
      )
  in
  Log.debug (fun l -> l "exit")
