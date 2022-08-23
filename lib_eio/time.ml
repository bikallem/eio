exception Timeout

class virtual ['a] clock = object
  method virtual now : 'a
  method virtual sleep_until : 'a -> unit
  method virtual add_seconds : 'a -> float -> 'a
end

let now (t: (_ #clock)) = t#now

let sleep_until (t : (_ #clock)) time = t#sleep_until time

let sleep t d = sleep_until t (t#add_seconds t#now d)

let with_timeout t d = Fiber.first (fun () -> sleep t d; Error `Timeout)
let with_timeout_exn t d = Fiber.first (fun () -> sleep t d; raise Timeout)
