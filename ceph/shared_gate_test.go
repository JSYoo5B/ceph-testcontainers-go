package ceph

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// TestSharedGateClassifiesEveryMethod fails when a method of *Container,
// promoted ones included, has no policy for a shared cluster, and calls every
// method that is not sharedOpen on a shared cluster with zero arguments: each
// must refuse with ErrSharedCluster before it touches the cluster, which a
// zero Container would make it panic on.
func TestSharedGateClassifiesEveryMethod(t *testing.T) {
	typ := reflect.TypeFor[*Container]()
	var methods []string
	for method := range typ.Methods() {
		methods = append(methods, method.Name)
		if _, ok := sharedMethodPolicies[method.Name]; !ok {
			t.Errorf("(*Container).%s has no shared-cluster policy in sharedMethodPolicies", method.Name)
		}
	}
	for name := range sharedMethodPolicies {
		if !slices.Contains(methods, name) {
			t.Errorf("sharedMethodPolicies classifies %s, which *Container does not have", name)
		}
	}

	shared := &Container{}
	shared.sharing.Store(int32(sharedWithTenants))
	value := reflect.ValueOf(shared)
	errorType := reflect.TypeFor[error]()
	customizerType := reflect.TypeFor[testcontainers.CustomizeRequestOption]()
	for _, name := range methods {
		policy := sharedMethodPolicies[name]
		if policy == sharedOpen || policy == 0 {
			continue
		}
		t.Run(name, func(t *testing.T) {
			method := value.MethodByName(name)
			args := zeroArguments(method.Type())
			var results []reflect.Value
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						t.Fatalf("%s ran past the gate on a shared cluster: %v", name, recovered)
					}
				}()
				results = method.Call(args)
			}()
			refused := false
			for _, result := range results {
				switch {
				case result.Type() == errorType:
					err, _ := result.Interface().(error)
					if !errors.Is(err, ErrSharedCluster) {
						t.Fatalf("%s returned %v, want ErrSharedCluster", name, err)
					}
					var operation *OperationError
					if !errors.As(err, &operation) || operation.Op != name {
						t.Fatalf("%s returned %v, which does not name the method", name, err)
					}
					refused = true
				case result.Type() == customizerType:
					err := result.Interface().(testcontainers.CustomizeRequestOption)(&testcontainers.GenericContainerRequest{})
					if !errors.Is(err, ErrSharedCluster) {
						t.Fatalf("the customizer of %s returned %v, want ErrSharedCluster", name, err)
					}
					refused = true
				case !result.IsZero():
					t.Fatalf("%s returned %v on a shared cluster", name, result)
				}
			}
			if !refused && slices.ContainsFunc(results, func(result reflect.Value) bool { return result.Type() == errorType }) {
				t.Fatalf("%s was not refused", name)
			}
		})
	}
}

func zeroArguments(method reflect.Type) []reflect.Value {
	count := method.NumIn()
	if method.IsVariadic() {
		count--
	}
	args := make([]reflect.Value, count)
	contextType := reflect.TypeFor[context.Context]()
	for i := range count {
		if method.In(i) == contextType {
			args[i] = reflect.ValueOf(context.Background())
		} else {
			args[i] = reflect.Zero(method.In(i))
		}
	}
	return args
}

func TestSharedGateOpensForTheModuleAndUnsharedClusters(t *testing.T) {
	var unshared Container
	if err := unshared.sharedGate("OverrideConfig"); err != nil {
		t.Fatalf("an unshared cluster refused: %v", err)
	}
	var nilCluster *Container
	if err := nilCluster.sharedGate("Ceph"); err != nil {
		t.Fatalf("a nil cluster refused: %v", err)
	}
	shared := &Container{}
	shared.sharing.Store(int32(sharedWithTenants))
	if err := shared.sharedGate("Status"); err != nil {
		t.Fatalf("an open method was refused: %v", err)
	}
	err := shared.sharedGate("Terminate")
	if !errors.Is(err, ErrSharedCluster) || errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Terminate returned %v", err)
	}
}

// TestSharedGateOpensForAnExclusiveLease checks that an exclusive lease may
// use every method except those that end the cluster, which it refuses
// before it touches the cluster.
func TestSharedGateOpensForAnExclusiveLease(t *testing.T) {
	exclusive := &Container{}
	exclusive.sharing.Store(int32(sharedExclusively))
	for name, policy := range sharedMethodPolicies {
		err := exclusive.sharedGate(name)
		if refused := err != nil; refused != (policy == sharedRefused) {
			t.Errorf("%s (policy %d) under an exclusive lease: %v", name, policy, err)
		}
	}
	method := reflect.ValueOf(exclusive).MethodByName("Terminate")
	results := method.Call(zeroArguments(method.Type()))
	if err, _ := results[0].Interface().(error); !errors.Is(err, ErrSharedCluster) {
		t.Fatalf("Terminate under an exclusive lease returned %v", err)
	}
}
