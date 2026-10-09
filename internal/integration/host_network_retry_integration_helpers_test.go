//go:build all || (integration && hostnetwork)

package integration_test

const hostNetworkPortBlockerScript = `import errno, signal, socket, sys, time
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
address, port = sys.argv[1], int(sys.argv[2])
s = socket.socket()
deadline = time.monotonic() + 60
while True:
    try:
        s.bind((address, port))
        break
    except OSError as e:
        if e.errno != errno.EADDRINUSE or time.monotonic() >= deadline:
            raise
        time.sleep(0.01)
s.listen(8)
print("TC_MON_PORT_BLOCKED", flush=True)
while True:
    connection, _ = s.accept()
    with connection:
        try:
            connection.sendall(b"TC_MON_PORT_BLOCKED\n")
        except OSError:
            pass
`

const hostNetworkBlockedMonitorScript = `import os, socket, sys, time
address, port = sys.argv[1], int(sys.argv[2])
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    try:
        with socket.create_connection((address, port), timeout=0.5) as connection:
            if connection.makefile("rb").readline(64) == b"TC_MON_PORT_BLOCKED\n":
                os.execv("/bin/sh", ["/bin/sh", "/tc/mon.sh"])
    except OSError:
        pass
    time.sleep(0.01)
raise RuntimeError("MON port blocker did not acquire the candidate")
`
