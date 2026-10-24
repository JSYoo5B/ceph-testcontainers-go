//go:build all || integration

package integration_test

import (
	"errors"
	"fmt"
)

// snapRemovalGate reports whether the derived inherited context of the
// inode's realm shows the removal of snapID. The removal is known to the
// observer only as a change: the context observed while the snapshot existed
// had sequence priorSeq, and Server::handle_client_rmsnap raises the realm
// sequence to the snapshot table's new sequence, which the client applies only
// when it is newer than what it holds (Client::update_snap_trace). So the gate
// releases only for a complete observation that no longer lists snapID AND
// whose derived sequence is newer than priorSeq. An absent ID alone does not
// release it (a cache that never listed the ID, or a partial view, would
// satisfy that), and a newer sequence alone does not either. An incomplete or
// invalid observation never releases the gate; err explains why.
func snapRemovalGate(data []byte, inode, snapID, priorSeq uint64) (released bool, ctx clientSnapContext, err error) {
	if snapID == 0 || priorSeq == 0 {
		return false, clientSnapContext{}, errors.New("snapshot removal gate needs the snapshot ID and the sequence observed while it existed")
	}
	view, err := parseClientSnapRealms(data, inode)
	if err != nil {
		return false, clientSnapContext{}, err
	}
	ctx, err = view.derive()
	if err != nil {
		return false, clientSnapContext{}, err
	}
	return !ctx.hasSnap(snapID) && ctx.Seq > priorSeq, ctx, nil
}

// snapRemovalPriorOK checks that the context observed before the removal
// really listed the snapshot, so that its disappearance is a change.
func snapRemovalPriorOK(ctx clientSnapContext, snapID uint64) error {
	if ctx.Seq == 0 || !ctx.hasSnap(snapID) {
		return fmt.Errorf("the context before the removal does not list snapshot %x: %+v", snapID, ctx)
	}
	return nil
}
