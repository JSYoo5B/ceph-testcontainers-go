//go:build all || integration

package rbd_test

import (
	"encoding/json"
	"fmt"
	"slices"
)

func rbdNamespaceExcludesImage(code int, listing []byte, err error, excluded string) error {
	if err != nil || code != 0 {
		return fmt.Errorf("list receiving namespace: exit=%d error=%v output=%s", code, err, listing)
	}
	var images []string
	if err := json.Unmarshal(listing, &images); err != nil || images == nil {
		return fmt.Errorf("invalid receiving namespace image list: %s (%v)", listing, err)
	}
	if slices.Contains(images, excluded) {
		return fmt.Errorf("image %s unexpectedly appeared in receiving namespace", excluded)
	}
	return nil
}
