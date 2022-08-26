exception Timeout

class virtual ['a] clock = object
  method virtual now : 'a
  method virtual sleep : float -> unit
  method virtual compare : 'a -> 'a -> int
  method virtual diff : 'a -> 'a -> float
end

let now (t: (_ #clock)) = t#now

let sleep_until (t : (_ #clock)) time =
  let now = t#now in
  if t#compare now time = -1 then
    t#sleep (t#diff time now)

let sleep t d = t#sleep d

let with_timeout t d = Fiber.first (fun () -> sleep t d; Error `Timeout)
let with_timeout_exn t d = Fiber.first (fun () -> sleep t d; raise Timeout)
