package monsession

import "testing"

func TestIncompleteIdentityCannotProveAbsence(t *testing.T) {
	for _, data := range []string{`null`, `[null]`, `[{}]`, `[{"entity_name":"mgr.a","global_id":7}]`, `[{"entity_name":"mgr.a","con_type":"mgr"}]`, `[{"entity_name":"mgr.a","con_type":"client","global_id":0}]`} {
		if _, err := Decode([]byte(data)); err == nil {
			t.Errorf("accepted incomplete session %s", data)
		}
	}
	for _, data := range []string{`[]`, `[{"entity_name":"","con_type":"mon","global_id":0}]`, `[{"entity_name":"mgr.a","con_type":"client","global_id":7}]`} {
		if _, err := Decode([]byte(data)); err != nil {
			t.Errorf("valid session: %v", err)
		}
	}
}

func TestSettlementRequiresTypedNonces(t *testing.T) {
	for _, addr := range []string{`null`, `{}`, `{"addrvec":null}`, `{"addrvec":[]}`, `{"addrvec":[{}]}`, `{"addrvec":[{"nonce":"7"}]}`, `{"addrvec":[{"nonce":-7}]}`, `{"addrvec":[{"nonce":4294967296}]}`} {
		data := `[{"entity_name":"client.probe","con_type":"client","global_id":7,"addrs":` + addr + `}]`
		if _, err := Nonces([]byte(data)); err == nil {
			t.Errorf("accepted incomplete nonces %s", addr)
		}
	}
	data := `[{"entity_name":"","con_type":"mon","global_id":0},{"entity_name":"client.probe","con_type":"client","global_id":7,"addrs":{"addrvec":[{"nonce":7},{"nonce":8}]}}]`
	got, err := Nonces([]byte(data))
	if err != nil || len(got) != 2 || got[0] != 7 || got[1] != 8 {
		t.Fatalf("nonces %v: %v", got, err)
	}
}
