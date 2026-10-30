//go:build all || integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// featureSession is one MON session with the features its connection negotiated.
type featureSession struct {
	Entity   string `json:"entity"`
	Type     string `json:"type"`
	GlobalID uint64 `json:"global_id"`
	Features uint64 `json:"features"`
	Release  string `json:"release"`
}

// parseFeatureSessions reads the MON's "sessions" output. Squid prints the
// session's name as "osd.0" or "client.4123"; the entity name, which can
// differ for clients, is reported separately.
func parseFeatureSessions(data []byte) ([]featureSession, error) {
	var raw []struct {
		Name           string          `json:"name"`
		ConType        string          `json:"con_type"`
		ConFeatures    json.RawMessage `json:"con_features"`
		FeaturesHex    string          `json:"con_features_hex"`
		FeaturesRelase string          `json:"con_features_release"`
		GlobalID       uint64          `json:"global_id"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	var result []featureSession
	for _, s := range raw {
		features, err := strconv.ParseUint(strings.TrimPrefix(s.FeaturesHex, "0x"), 16, 64)
		if err != nil {
			if features, err = strconv.ParseUint(string(s.ConFeatures), 10, 64); err != nil {
				return nil, fmt.Errorf("session %s features %q/%s: %w", s.Name, s.FeaturesHex, s.ConFeatures, err)
			}
		}
		result = append(result, featureSession{Entity: s.Name, Type: s.ConType, GlobalID: s.GlobalID, Features: features, Release: s.FeaturesRelase})
	}
	return result, nil
}

// sessionFeature is a feature that every Squid or Tentacle daemon and client
// negotiates, from ceph_features.h at the pinned commit, by name, bit and incarnation.
// HAVE_FEATURE tests a bit together with its incarnation's marker bits,
// because a reused bit has older meanings. SERVER_LUMINOUS and RADOS_BACKOFF
// are two of the five names of (21, 2): the bits cannot tell them apart, so
// each name is listed on its own.
type sessionFeature struct {
	name        string
	bit         uint
	incarnation int
}

func (f sessionFeature) mask() uint64 {
	marker := map[int]uint64{1: 0, 2: 1 << 57, 3: 1<<57 | 1<<28}[f.incarnation]
	return 1<<f.bit | marker
}

var requiredSessionFeatures = []sessionFeature{
	{"SERVER_LUMINOUS", 21, 2}, {"RADOS_BACKOFF", 21, 2}, {"SERVER_NAUTILUS", 2, 3}, {"SERVER_OCTOPUS", 16, 3},
	{"SERVER_PACIFIC", 20, 3}, {"SERVER_QUINCY", 33, 3}, {"SERVER_REEF", 31, 3}, {"SERVER_SQUID", 49, 2},
	{"MSG_ADDR2", 59, 1}, {"CEPHX_V2", 61, 1}, {"OSDHASHPSPOOL", 30, 1},
}

// missingSessionFeatures names the required features a session lacks.
func missingSessionFeatures(features uint64) []string {
	var missing []string
	for _, feature := range requiredSessionFeatures {
		if features&feature.mask() != feature.mask() {
			missing = append(missing, feature.name)
		}
	}
	return missing
}

// messengerCompressionProbe connects a librados client that asks OSDs for
// compression, writes and reads back a compressible object in pool through
// the object's primary, and, while still connected, reads the MON's sessions
// and, when the release has it, each OSD's messenger dump. It prints one JSON
// receipt and shuts its client down.
const messengerCompressionProbe = `import json,subprocess,sys,rados
pool,name,dump=sys.argv[1],sys.argv[2],sys.argv[3]=='1'
def ceph(*args):
    r=subprocess.run(['ceph','--connect-timeout','10',*args,'--format','json'],capture_output=True,text=True,timeout=60)
    assert r.returncode==0,(args,r.returncode,r.stderr[-400:])
    return json.loads(r.stdout)
payload=b'compressible messenger payload '*4096
client=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'30','rados_mon_op_timeout':'30','ms_osd_compress_mode':'force','ms_osd_compression_algorithm':'snappy'})
client.connect()
try:
    io=client.open_ioctx(pool)
    try:
        io.write_full(name,payload)
        assert io.read(name,len(payload)+1,0)==payload
        version=io.get_last_version()
    finally:
        io.close()
    me=client.get_instance_id()
    mapping=ceph('osd','map',pool,name)
    receipt={'client_global_id':me,'object':name,'bytes':len(payload),'version':version,'up':mapping['up'],'acting':mapping['acting'],'primary':mapping['acting_primary'],'sessions':ceph('tell','mon.a','sessions'),'dumps':{}}
    if dump:
        for osd in mapping['acting']:
            daemon='osd.%d'%osd
            catalog=ceph('tell',daemon,'messenger','dump'); catalog=catalog.get('status',catalog)
            receipt['dumps'][daemon]={m:ceph('tell',daemon,'messenger','dump',m) for m in catalog['messengers']}
    print(json.dumps(receipt))
finally:
    client.shutdown()
`

// compressionPhase is one OSD Messenger policy and the sessions it must
// produce between the two OSDs.
type compressionPhase struct {
	name     string
	settings []ceph.ConfigSetting
	// mode and compressed are what every READY OSD-to-OSD session must use.
	mode       string
	compressed bool
}

// checkDumpedSession compares one READY session from messenger dump with
// the phase. Reused sessions may report con_mode "unknown"; their cipher
// still tells secure from CRC.
func checkDumpedSession(daemon string, c harness.MessengerConnection, phase compressionPhase) []string {
	where := fmt.Sprintf("%s %s session with %s.%d", daemon, c.Messenger, c.PeerType, c.PeerID)
	var problems []string
	if !c.Revision1 {
		problems = append(problems, where+" is not msgr2.1")
	}
	if c.PeerType != "osd" {
		if c.CompressionRx != "UNCOMPRESSED" || c.CompressionTx != "UNCOMPRESSED" {
			problems = append(problems, fmt.Sprintf("%s is compressed (%s/%s)", where, c.CompressionRx, c.CompressionTx))
		}
		return problems
	}
	secure := c.CryptoRx != "PLAIN" && c.CryptoTx != "PLAIN"
	if !slices.Contains([]string{phase.mode, "unknown"}, c.Mode) || secure != (phase.mode == "secure") {
		problems = append(problems, fmt.Sprintf("%s mode %s crypto %s/%s, want %s", where, c.Mode, c.CryptoRx, c.CryptoTx, phase.mode))
	}
	want := "UNCOMPRESSED"
	if phase.compressed {
		want = "snappy"
	}
	if c.CompressionRx != want || c.CompressionTx != want {
		problems = append(problems, fmt.Sprintf("%s compression %s/%s, want %s", where, c.CompressionRx, c.CompressionTx, want))
	}
	return problems
}

// checkLoggedSession compares one READY log line with the phase.
func checkLoggedSession(daemon string, c harness.ReadyLogConnection, phase compressionPhase) []string {
	where := fmt.Sprintf("%s session with %s.%s", daemon, c.PeerType, c.PeerID)
	if c.PeerType != "osd" {
		if c.Compression {
			return []string{where + " has a compression handler"}
		}
		return nil
	}
	var problems []string
	// The accepting side of a lossless session logs mode "unknown", as
	// messenger dump does; its crypto handlers still tell secure from CRC.
	if !slices.Contains([]string{phase.mode, "unknown"}, c.Mode) || c.Crypto != (phase.mode == "secure") {
		problems = append(problems, fmt.Sprintf("%s mode %s crypto=%v, want %s", where, c.Mode, c.Crypto, phase.mode))
	}
	if c.Compression != phase.compressed {
		problems = append(problems, fmt.Sprintf("%s compression=%v, want %v", where, c.Compression, phase.compressed))
	}
	return problems
}
