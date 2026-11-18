//go:build all || integration

package integration_test

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// telegrafReceiverScript receives the telegraf module's line protocol, run
// after moduleReceiverPrelude with a UDP port as argv[1] and a TCP port as
// argv[2]. It publishes one record per datagram or closed TCP connection,
// with complete newline-terminated lines or a framing or transport error.
// Publishing a whole unit at once prevents partial TCP evidence from being
// visible while a connection is still active.
const telegrafReceiverScript = `UDP_PORT, TCP_PORT = int(sys.argv[1]), int(sys.argv[2])
sockets = {}
counter = [0]
def next_id():
    with _lock:
        counter[0] += 1
        return counter[0]
def record_unit(proto, peer, unit, data, error=''):
    if not error and not data.endswith(b'\n'):
        error = 'unterminated line'
    lines = []
    if not error:
        try:
            lines = data[:-1].decode('utf-8').split('\n')
        except UnicodeDecodeError as exc:
            error = str(exc)
    record({'proto': proto, 'peer': peer, 'unit': unit, 'size': len(data),
            'lines': lines, 'error': error})
def receive_udp(sock):
    while True:
        try:
            data, peer = sock.recvfrom(65535)
        except OSError:
            return
        record_unit('udp', peer[0], next_id(), data)
def read_tcp(conn, peer):
    unit, data, error = next_id(), bytearray(), ''
    with conn:
        while True:
            try:
                chunk = conn.recv(65536)
            except OSError as exc:
                error = 'socket read: ' + str(exc)
                break
            if not chunk:
                break
            data.extend(chunk)
    if data or error:
        record_unit('tcp', peer, unit, data, error)
def accept_tcp(sock):
    while True:
        try:
            conn, peer = sock.accept()
        except OSError:
            return
        threading.Thread(target=read_tcp, args=(conn, peer[0]), daemon=True).start()
def open_listener():
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind(('', UDP_PORT))
    tcp = socket.create_server(('', TCP_PORT))
    sockets.update(udp=udp, tcp=tcp)
    threading.Thread(target=receive_udp, args=(udp,), daemon=True).start()
    threading.Thread(target=accept_tcp, args=(tcp,), daemon=True).start()
def close_listener():
    for sock in sockets.values():
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()
    sockets.clear()
serve_receiver()
`

// telegrafRecord is one record of telegrafReceiverScript.
type telegrafRecord struct {
	Time  float64  `json:"time"`
	Proto string   `json:"proto"`
	Peer  string   `json:"peer"`
	Unit  int      `json:"unit"`
	Size  int      `json:"size"`
	Lines []string `json:"lines"`
	Error string   `json:"error"`
}

// influxPoint is one parsed line of the InfluxDB line protocol.
type influxPoint struct {
	Measurement string
	Tags        map[string]string
	// Fields holds int64, float64, bool or string values.
	Fields    map[string]any
	Timestamp int64
}

// splitUnescaped splits s at every sep that no backslash escapes and that is
// outside a double-quoted string when quotes is set.
func splitUnescaped(s string, sep byte, quotes bool) []string {
	var parts []string
	start, quoted := 0, false
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '\\' && i+1 < len(s):
			i++
		case quotes && s[i] == '"':
			quoted = !quoted
		case s[i] == sep && !quoted:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}

// unescapeInflux removes the backslash of an escaped comma, space or equals
// sign in a measurement, tag key or tag value.
func unescapeInflux(s string) string {
	return strings.NewReplacer(`\,`, ",", `\ `, " ", `\=`, "=").Replace(s)
}

// parseInfluxLine parses one line of the line protocol the telegraf module
// writes (Line.to_line_protocol in src/pybind/mgr/telegraf/protocol.py):
// measurement and sorted tags, fields, and a nanosecond timestamp.
func parseInfluxLine(line string) (influxPoint, error) {
	parts := splitUnescaped(line, ' ', true)
	if len(parts) != 3 {
		return influxPoint{}, fmt.Errorf("%d space-separated parts in %q, want 3", len(parts), line)
	}
	point := influxPoint{Tags: map[string]string{}, Fields: map[string]any{}}
	series := splitUnescaped(parts[0], ',', false)
	point.Measurement = unescapeInflux(series[0])
	if point.Measurement == "" {
		return influxPoint{}, fmt.Errorf("no measurement in %q", line)
	}
	for _, tag := range series[1:] {
		pair := splitUnescaped(tag, '=', false)
		if len(pair) != 2 || pair[0] == "" || pair[1] == "" {
			return influxPoint{}, fmt.Errorf("tag %q in %q", tag, line)
		}
		point.Tags[unescapeInflux(pair[0])] = unescapeInflux(pair[1])
	}
	for _, field := range splitUnescaped(parts[1], ',', true) {
		key, raw, ok := strings.Cut(field, "=")
		if !ok || key == "" || raw == "" {
			return influxPoint{}, fmt.Errorf("field %q in %q", field, line)
		}
		var value any
		var err error
		switch {
		case strings.HasPrefix(raw, `"`):
			if len(raw) < 2 || !strings.HasSuffix(raw, `"`) {
				return influxPoint{}, fmt.Errorf("string field %q in %q", field, line)
			}
			value = strings.ReplaceAll(raw[1:len(raw)-1], `\"`, `"`)
		case strings.HasSuffix(raw, "i"):
			value, err = strconv.ParseInt(strings.TrimSuffix(raw, "i"), 10, 64)
		case raw == "True" || raw == "true" || raw == "False" || raw == "false":
			value = raw == "True" || raw == "true"
		default:
			value, err = strconv.ParseFloat(raw, 64)
		}
		if err != nil {
			return influxPoint{}, fmt.Errorf("field %q in %q: %v", field, line, err)
		}
		point.Fields[unescapeInflux(key)] = value
	}
	timestamp, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || timestamp <= 0 {
		return influxPoint{}, fmt.Errorf("timestamp %q in %q", parts[2], line)
	}
	point.Timestamp = timestamp
	return point, nil
}

// telegrafBatch groups the observed lines with one module timestamp.
// UDP has no end-of-pass marker or count of emitted lines.
type telegrafBatch struct {
	Timestamp int64
	// Peers and Units are the senders and the datagrams or connections the
	// lines arrived in.
	Peers, Units map[string]int
	Proto        string
	// Receive times are in Unix seconds, one sample per transport unit.
	Received     float64
	LastReceived float64
	Points       []influxPoint
}

// telegrafBatches parses every record and groups the lines by timestamp, in
// timestamp order. A line that does not parse, a batch that mixes
// transports and a duplicate line are errors: the module writes each line
// once per pass.
func telegrafBatches(records []telegrafRecord) ([]telegrafBatch, error) {
	byTimestamp := map[int64]*telegrafBatch{}
	seen := map[string]bool{}
	for _, record := range records {
		if record.Error != "" {
			return nil, fmt.Errorf("%s unit %d from %s (%d bytes): %s", record.Proto, record.Unit, record.Peer, record.Size, record.Error)
		}
		if len(record.Lines) == 0 {
			return nil, fmt.Errorf("%s unit %d has no framed lines", record.Proto, record.Unit)
		}
		var unitTimestamp int64
		for _, line := range record.Lines {
			point, err := parseInfluxLine(line)
			if err != nil {
				return nil, err
			}
			if seen[line] {
				return nil, fmt.Errorf("duplicate line %q", line)
			}
			seen[line] = true
			if unitTimestamp != 0 && point.Timestamp != unitTimestamp {
				return nil, fmt.Errorf("%s unit %d mixes timestamps %d and %d", record.Proto, record.Unit, unitTimestamp, point.Timestamp)
			}
			unitTimestamp = point.Timestamp
			batch := byTimestamp[point.Timestamp]
			if batch == nil {
				batch = &telegrafBatch{Timestamp: point.Timestamp, Peers: map[string]int{}, Units: map[string]int{}, Proto: record.Proto,
					Received: record.Time, LastReceived: record.Time}
				byTimestamp[point.Timestamp] = batch
			}
			if batch.Proto != record.Proto {
				return nil, fmt.Errorf("batch %d arrived over %s and %s", point.Timestamp, batch.Proto, record.Proto)
			}
			batch.Peers[record.Peer]++
			batch.Units[strconv.Itoa(record.Unit)]++
			batch.Received = min(batch.Received, record.Time)
			batch.LastReceived = max(batch.LastReceived, record.Time)
			batch.Points = append(batch.Points, point)
		}
	}
	batches := make([]telegrafBatch, 0, len(byTimestamp))
	for _, batch := range byTimestamp {
		batches = append(batches, *batch)
	}
	sort.Slice(batches, func(i, j int) bool { return batches[i].Timestamp < batches[j].Timestamp })
	return batches, nil
}

// ready waits for a quiet interval after the latest UDP arrival. A clean
// TCP connection is published only after EOF and needs no quiet interval.
func (b telegrafBatch) ready(now time.Time) bool {
	return b.Proto == "tcp" || now.Sub(telegrafReceiveTime(b.LastReceived)) >= 2*time.Second
}

func telegrafReceiveTime(seconds float64) time.Time {
	return time.Unix(0, int64(seconds*1e9))
}

// Peer returns the batch's only sender, or an error when several sent it.
func (b telegrafBatch) Peer() (string, error) {
	if len(b.Peers) != 1 {
		return "", fmt.Errorf("batch %d has senders %v", b.Timestamp, b.Peers)
	}
	for peer := range b.Peers {
		return peer, nil
	}
	return "", nil
}

// Select returns the points of measurement whose tags include tags.
func (b telegrafBatch) Select(measurement string, tags map[string]string) []influxPoint {
	var points []influxPoint
next:
	for _, point := range b.Points {
		if point.Measurement != measurement {
			continue
		}
		for key, value := range tags {
			if point.Tags[key] != value {
				continue next
			}
		}
		points = append(points, point)
	}
	return points
}

// Value returns the integer value field of the one point of measurement
// with tags.
func (b telegrafBatch) Value(measurement string, tags map[string]string) (int64, error) {
	points := b.Select(measurement, tags)
	if len(points) != 1 {
		return 0, fmt.Errorf("%d %s points with %v in batch %d", len(points), measurement, tags, b.Timestamp)
	}
	value, ok := points[0].Fields["value"].(int64)
	if !ok {
		return 0, fmt.Errorf("%s with %v: value %v is not an integer", measurement, tags, points[0].Fields["value"])
	}
	return value, nil
}

// checkTelegrafBatch checks the observed framing, cluster identity and
// measurement kinds. These checks do not prove how many lines a pass sent.
func checkTelegrafBatch(b telegrafBatch, fsid string) error {
	switch b.Proto {
	case "udp":
		if len(b.Units) != len(b.Points) {
			return fmt.Errorf("UDP batch %d: %d lines in %d datagrams, want one line per datagram", b.Timestamp, len(b.Points), len(b.Units))
		}
	case "tcp":
		if len(b.Units) != 1 {
			return fmt.Errorf("TCP batch %d arrived over %d connections, want one", b.Timestamp, len(b.Units))
		}
	default:
		return fmt.Errorf("batch %d has unknown transport %q", b.Timestamp, b.Proto)
	}
	for _, point := range b.Points {
		if point.Tags["fsid"] != fsid {
			return fmt.Errorf("batch %d: %s with fsid %q, want %q", b.Timestamp, point.Measurement, point.Tags["fsid"], fsid)
		}
	}
	for _, measurement := range []string{"ceph_cluster_stats", "ceph_pool_stats", "ceph_daemon_stats"} {
		if len(b.Select(measurement, nil)) == 0 {
			return fmt.Errorf("batch %d has no %s", b.Timestamp, measurement)
		}
	}
	return nil
}
