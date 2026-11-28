package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// crushRule is one rule of osd crush rule dump.
type crushRule struct {
	id    int
	name  string
	steps []crushRuleStep
}

type crushRuleStep struct {
	op, itemName, kind string
	num                *int
}

// crushRules is the one reader of osd crush rule dump. Every rule must carry
// a nonnegative rule_id and a nonempty rule_name, and neither may repeat:
// a rule without an ID would otherwise read as rule 0.
func (pc poolCatalog) crushRules(ctx context.Context) ([]crushRule, error) {
	data, err := pc.conn.ceph(ctx, "osd", "crush", "rule", "dump", "--format", "json")
	if err != nil {
		return nil, err
	}
	return decodeCRUSHRules(data)
}

func decodeCRUSHRules(data []byte) ([]crushRule, error) {
	var native []struct {
		ID    *int    `json:"rule_id"`
		Name  *string `json:"rule_name"`
		Steps []struct {
			Op       string `json:"op"`
			ItemName string `json:"item_name"`
			Type     string `json:"type"`
			Num      *int   `json:"num"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native == nil {
		return nil, errors.New("decode native CRUSH rules")
	}
	rules := make([]crushRule, 0, len(native))
	ids, names := map[int]bool{}, map[string]bool{}
	for _, rule := range native {
		if rule.ID == nil || *rule.ID < 0 || rule.Name == nil || *rule.Name == "" {
			return nil, errors.New("incomplete native CRUSH rule")
		}
		if ids[*rule.ID] || names[*rule.Name] {
			return nil, errors.New("duplicate native CRUSH rule")
		}
		ids[*rule.ID], names[*rule.Name] = true, true
		steps := make([]crushRuleStep, len(rule.Steps))
		for i, step := range rule.Steps {
			steps[i] = crushRuleStep{op: step.Op, itemName: step.ItemName, kind: step.Type, num: step.Num}
		}
		rules = append(rules, crushRule{id: *rule.ID, name: *rule.Name, steps: steps})
	}
	return rules, nil
}

// crushRuleByID and crushRuleByName find one rule of a dump.
func crushRuleByID(rules []crushRule, id int) (crushRule, bool) {
	for _, rule := range rules {
		if rule.id == id {
			return rule, true
		}
	}
	return crushRule{}, false
}

func crushRuleByName(rules []crushRule, name string) (crushRule, bool) {
	for _, rule := range rules {
		if rule.name == name {
			return rule, true
		}
	}
	return crushRule{}, false
}

// placement is the root, device class and failure domain of a rule. Only
// the simple take/choose/emit rule that this fixture generates is understood;
// a custom algorithm is refused rather than miscounted.
func (r crushRule) placement() (PoolConfig, error) {
	unsupported := fmt.Errorf("native CRUSH rule %d is not a supported simple osd/host/rack/zone rule", r.id)
	if len(r.steps) != 3 || r.steps[0].op != "take" || r.steps[2].op != "emit" {
		return PoolConfig{}, unsupported
	}
	choose := r.steps[1]
	if choose.num == nil || *choose.num != 0 {
		return PoolConfig{}, unsupported
	}
	if choose.op != "chooseleaf_firstn" && choose.op != "choose_firstn" {
		return PoolConfig{}, unsupported
	}
	if choose.kind != "osd" && choose.kind != "host" && choose.kind != "rack" && choose.kind != "zone" {
		return PoolConfig{}, unsupported
	}
	if choose.op == "choose_firstn" && choose.kind != "osd" {
		return PoolConfig{}, unsupported
	}
	root, class, _ := strings.Cut(r.steps[0].itemName, "~")
	if root == "" {
		return PoolConfig{}, unsupported
	}
	return PoolConfig{CRUSHRoot: root, DeviceClass: class, FailureDomain: choose.kind}, nil
}

// poolKind maps the native pool type of an OSDMap pool to PoolState.Type:
// 1 is replicated and 3 erasure; Ceph has no other pool type in use.
func poolKind(native int) (string, error) {
	switch native {
	case 1:
		return "replicated", nil
	case 3:
		return "erasure", nil
	}
	return "", fmt.Errorf("unsupported native pool type %d", native)
}
