package expect_test

import (
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd/expect"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
)

// After the leader stops, the survivor must lead while the stopped member
// stays in the cohort. The evaluation names what the observation lacks.
func ExampleReceivers_Evaluate() {
	observed := mirror.ReceiverStatus{
		Pool: "volumes", ExpectedDaemons: []string{"a", "b"},
		Daemons: map[string]mirror.ReceiverDaemonStatus{
			"a": {ContainerID: "cid-a", InstanceID: "4101", PoolState: "running", Running: true, NamespaceDiscovered: true, Leader: true},
			"b": {ContainerID: "cid-b"},
		},
		LeaderDaemonName: "a", LeaderInstanceID: "4101", Instances: []string{"4101", "4102"},
		DaemonProblems: map[string]string{"a": "receiver-membership-converging", "b": "receiver-not-running"},
	}
	result, err := expect.Receivers{Select: []expect.ReceiverDaemon{{Name: "a"}, {Name: "b", Running: true}}, Leader: "b"}.Evaluate(observed)
	if err != nil {
		panic(err) // invalid expectation or malformed observation: no verdict
	}
	fmt.Println(result.Matched)
	fmt.Println(result)
	// Output:
	// false
	// receivers [a(stopped) b] leader=b in cohort [a b]: a running: want stopped, observed running; b running: want running, observed stopped; cohort leader: want b, observed a
}

// A receiver restart must leave the image pair in place and replay it
// through a new receiving instance.
func ExampleMirrorImage_Evaluate() {
	baseline := mirror.ImageStatus{
		Pool: "volumes", Name: "volume", Mode: mirror.ModeJournal,
		GlobalID: "6c1b8f9e-0000-4000-8000-000000000001", SourceImageID: "1a2b", DestinationImageID: "3c4d",
		SourcePrimary: true, SourceMirrorState: "enabled", DestinationMirrorState: "enabled",
		State: "up+replaying", DaemonName: "a", InstanceID: "4101", ReplayReady: true,
	}
	restarted := baseline
	restarted.State, restarted.ReplayReady = "up+starting_replay", false
	result, err := expect.MirrorImage{Select: baseline, Replaying: true, NewInstance: true}.Evaluate(restarted)
	if err != nil {
		panic(err)
	}
	fmt.Println(result)
	// Output:
	// image volumes/:/volume want replaying=true new instance: replay: want true, observed false; state: want up+replaying, observed up+starting_replay; instance: want other than 4101, observed 4101
}
