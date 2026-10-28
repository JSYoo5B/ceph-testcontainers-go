//go:build all || integration

package integration_test

import (
	"fmt"
	"strings"
)

// admitPendingMkdir admits the one request that the path cut is meant to
// hold back: the actor's mkdir. A request of any other operation is not that
// mkdir. In particular a lookup is a predecessor that the client sends when a
// dentry lease on the way to the parent ran out, and the mkdir itself is not
// sent until it returns, so a cut that holds a lookup has not exposed the
// mkdir to the MDS at all. A missing operation or tid is no evidence either.
func admitPendingMkdir(requests []clientMDSRequest) (clientMDSRequest, error) {
	if len(requests) != 1 {
		return clientMDSRequest{}, fmt.Errorf("the client lists %d pending requests, want the one mkdir: %+v", len(requests), requests)
	}
	request := requests[0]
	switch {
	case request.Op == "" || request.TID == 0:
		return clientMDSRequest{}, fmt.Errorf("the client's pending request has no operation or tid: %+v", request)
	case request.Op != "mkdir":
		return clientMDSRequest{}, fmt.Errorf("the client's pending request is a %s, not the mkdir: the mkdir has not reached the cut (tid %d): %+v", request.Op, request.TID, request)
	}
	return request, nil
}

// requireMDSMkdirCompleted checks the MDS's historic record of the client's
// request tid: it must be this client's mkdir of name, and the MDS must have
// journaled it and sent the final reply after the early one. A completed
// request of another operation under the same tid does not count.
func requireMDSMkdirCompleted(historic mdsRequestOp, found bool, client, tid uint64, name string) error {
	if !found {
		return fmt.Errorf("no historic record of tid %d", tid)
	}
	if prefix := fmt.Sprintf("client_request(client.%d:%d mkdir ", client, tid); !strings.HasPrefix(historic.Description, prefix) {
		return fmt.Errorf("the record of tid %d is %q, not a %q request", tid, historic.Description, prefix)
	}
	if !strings.Contains(historic.Description, "/"+name+" ") {
		return fmt.Errorf("the mkdir of tid %d does not name %q: %q", tid, name, historic.Description)
	}
	if safety := requestSafety(historic); safety != "safe" {
		return fmt.Errorf("the mkdir of tid %d is %s at the MDS: %v", tid, safety, historic.events())
	}
	return nil
}
