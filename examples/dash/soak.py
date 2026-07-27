#!/usr/bin/env python3
"""
Fan-out soak for the dashboard: N SSE readers, 1 writer.

Every previous concurrency number in this project measured IDLE connections — they
connected, mounted once, and then nothing was pushed. That is the easy half. This
measures the other half: hundreds of connections all receiving patches from one
simulation, which is what "massive concurrent connections" actually means for this
kind of app.

Methodology follows dev/soak.clj, including the mistakes it records:

  * connections are HELD, never ramped-and-dropped;
  * failures are counted BY REASON, because "out of ports" and "server refused" imply
    different conclusions;
  * source addresses are bound explicitly, so loopback aliases widen the tuple past
    the ~16k single-address ceiling;
  * **bytes received per connection are counted**, so a connection that opened but is
    receiving nothing cannot be mistaken for a working one. That is the specific lie
    the Phoenix driver told three times.

Usage: drive.py <n> [seconds] [src-ip,src-ip,...]
"""
import socket
import sys
import threading
import time
from collections import Counter

N = int(sys.argv[1]) if len(sys.argv) > 1 else 200
HOLD = int(sys.argv[2]) if len(sys.argv) > 2 else 30
SRCS = sys.argv[3].split(",") if len(sys.argv) > 3 else [None]
PORT = 3004

errors = Counter()
conns = []          # (socket, stats-dict)
lock = threading.Lock()
stop = threading.Event()


def reader(sock, st):
    """Drain one SSE stream, counting bytes and datastar events."""
    buf = b""
    try:
        while not stop.is_set():
            chunk = sock.recv(65536)
            if not chunk:
                st["closed"] = True
                return
            st["bytes"] += len(chunk)
            buf += chunk
            # Count events rather than parse them: the question is whether patches are
            # arriving, not what they say.
            st["events"] += chunk.count(b"event: datastar-patch-elements")
            if len(buf) > 1 << 16:
                buf = buf[-1024:]
    except Exception as e:
        st["error"] = type(e).__name__


def open_one(src):
    try:
        s = socket.socket()
        if src:
            s.bind((src, 0))
        s.settimeout(60)
        s.connect(("127.0.0.1", PORT))
        s.sendall(
            b"GET /d/live HTTP/1.1\r\nHost: 127.0.0.1:3004\r\n"
            b"Accept: text/event-stream\r\n\r\n"
        )
        head = s.recv(4096)
        if b"200" not in head.split(b"\r\n")[0]:
            errors[f"status: {head.split(chr(13).encode())[0][:40]!r}"] += 1
            s.close()
            return
        st = {"bytes": len(head), "events": head.count(b"event: datastar-patch-elements"),
              "closed": False, "error": None}
        t = threading.Thread(target=reader, args=(s, st), daemon=True)
        t.start()
        with lock:
            conns.append((s, st))
    except Exception as e:
        errors[f"{type(e).__name__}: {e}"] += 1


def main():
    per = N // len(SRCS)
    print(f"opening {N} SSE readers across {len(SRCS)} source address(es)", flush=True)
    t0 = time.time()
    threads = [threading.Thread(target=open_one, args=(src,), daemon=True)
               for src in SRCS for _ in range(per)]
    i = 0
    while i < len(threads):
        batch = threads[i:i + 32]
        for t in batch:
            t.start()
        for t in batch:
            t.join()
        i += 32
    print(f"opened={len(conns)} errors={sum(errors.values())} "
          f"in {time.time()-t0:.1f}s", flush=True)
    for kind, n in errors.most_common(5):
        print(f"    {n} x {kind}", flush=True)

    # Sample every 5s so a plateau is visible rather than a single reading.
    for tick in range(HOLD // 5):
        time.sleep(5)
        with lock:
            snap = [st for _, st in conns]
        alive = sum(1 for st in snap if not st["closed"])
        total_ev = sum(st["events"] for st in snap)
        total_mb = sum(st["bytes"] for st in snap) / 1048576
        silent = sum(1 for st in snap if st["events"] == 0)
        print(f"  t+{(tick+1)*5:3d}s  alive={alive:5d}  events={total_ev:8d}  "
              f"recv={total_mb:7.1f}MB  silent={silent}", flush=True)

    stop.set()
    print("done", flush=True)


if __name__ == "__main__":
    main()
