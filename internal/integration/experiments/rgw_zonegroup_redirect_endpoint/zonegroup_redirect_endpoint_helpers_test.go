//go:build all || integration

package rgw_zonegroup_redirect_endpoint_test

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
)

// zonegroupView is one zonegroup of a committed period: its name, the name
// the S3 API calls it by, and the endpoints it advertises.
type zonegroupView struct {
	Name, APIName string
	Endpoints     []string
}

// periodView is the part of a committed period the case reads: its ID, its
// epoch and its zonegroups.
type periodView struct {
	ID         string
	Epoch      int64
	Zonegroups []zonegroupView
}

// parsePeriod reads a "period get" report. The period must carry its ID,
// its epoch and its zonegroup list, and every zonegroup its name, its API
// name and its endpoint list, which may be empty but not absent or null.
func parsePeriod(report []byte) (periodView, error) {
	var raw struct {
		ID    *string `json:"id"`
		Epoch *int64  `json:"epoch"`
		Map   *struct {
			Zonegroups *[]struct {
				Name      *string   `json:"name"`
				APIName   *string   `json:"api_name"`
				Endpoints *[]string `json:"endpoints"`
			} `json:"zonegroups"`
		} `json:"period_map"`
	}
	if err := json.Unmarshal(report, &raw); err != nil {
		return periodView{}, fmt.Errorf("period: %w", err)
	}
	if raw.ID == nil || *raw.ID == "" || raw.Epoch == nil || raw.Map == nil || raw.Map.Zonegroups == nil {
		return periodView{}, fmt.Errorf("period: the ID, the epoch or the zonegroup list is missing")
	}
	period := periodView{ID: *raw.ID, Epoch: *raw.Epoch}
	for i, group := range *raw.Map.Zonegroups {
		if group.Name == nil || *group.Name == "" || group.APIName == nil || group.Endpoints == nil {
			return periodView{}, fmt.Errorf("period: zonegroup %d has no name, no API name or no endpoint list", i)
		}
		period.Zonegroups = append(period.Zonegroups, zonegroupView{Name: *group.Name, APIName: *group.APIName, Endpoints: *group.Endpoints})
	}
	return period, nil
}

// site is one zonegroup as the case uses it: its name, the name the S3 API
// calls it by and the one endpoint it advertises.
type site struct {
	Name, APIName, Endpoint string
}

// advertisedSites returns the two zonegroups first and second of a
// committed period, as every zone of the realm holds it. periods are the
// reports of the zones. They must all be the same period at the same epoch
// and agree on the zonegroups, each of the two zonegroups must be in the
// period exactly once with exactly one endpoint, which is what a redirect to
// the zonegroup is built from, and the two endpoints and API names must
// differ, so a redirect names one zonegroup and not the other.
func advertisedSites(periods []periodView, first, second string) (a, b site, err error) {
	if len(periods) < 2 {
		return site{}, site{}, fmt.Errorf("condition not established: %d periods, want the period of every zone", len(periods))
	}
	sites := make([]site, 2)
	for i, name := range []string{first, second} {
		for j, period := range periods {
			if period.ID != periods[0].ID || period.Epoch != periods[0].Epoch {
				return site{}, site{}, fmt.Errorf("condition not established: the zones hold different periods: %s at epoch %d and %s at epoch %d", periods[0].ID, periods[0].Epoch, period.ID, period.Epoch)
			}
			var found []zonegroupView
			for _, group := range period.Zonegroups {
				if group.Name == name {
					found = append(found, group)
				}
			}
			if len(found) != 1 || len(found[0].Endpoints) != 1 || found[0].Endpoints[0] == "" || found[0].APIName == "" {
				return site{}, site{}, fmt.Errorf("condition not established: period %s does not hold the zonegroup %s once with one endpoint and an API name: %+v", period.ID, name, found)
			}
			got := site{Name: name, APIName: found[0].APIName, Endpoint: found[0].Endpoints[0]}
			if j > 0 && got != sites[i] {
				return site{}, site{}, fmt.Errorf("condition not established: the zones disagree on the zonegroup %s: %+v and %+v", name, sites[i], got)
			}
			sites[i] = got
		}
	}
	if sites[0].Endpoint == sites[1].Endpoint || sites[0].APIName == sites[1].APIName {
		return site{}, site{}, fmt.Errorf("condition not established: the zonegroups %s and %s do not advertise distinct endpoints and API names: %+v", first, second, sites)
	}
	return sites[0], sites[1], nil
}

// parseLocationConstraint reads the body of a GET ?location answer: the API
// name of the zonegroup that holds the bucket.
func parseLocationConstraint(body []byte) (string, error) {
	var raw struct {
		XMLName xml.Name `xml:"LocationConstraint"`
		Name    string   `xml:",chardata"`
	}
	if err := xml.Unmarshal(body, &raw); err != nil {
		return "", fmt.Errorf("bucket location: %w", err)
	}
	return strings.TrimSpace(raw.Name), nil
}

// The targets a redirect can name.
const (
	targetOwner    = "owner"
	targetReceiver = "receiver"
)

// redirectAnswer is the answer of a gateway to a request for a bucket of
// another zonegroup: the status and the Location headers. The gateway sends
// no body with it.
type redirectAnswer struct {
	Status    int
	Locations []string
}

// classifyRedirect says which zonegroup the answer of the receiving gateway
// redirects to, for a request whose path and query were uri.
//
// The answer must be a 301 with exactly one Location. The
// gateway builds the Location from one zonegroup's endpoint and the
// request's path and query, so the whole Location is compared: it names the
// owner when it is the owning zonegroup's endpoint followed by uri, and the
// receiver when it is the receiving zonegroup's endpoint followed by uri.
// Any other answer is an error: no redirect, a redirect without a Location,
// or a Location that is neither.
func classifyRedirect(answer redirectAnswer, owner, receiver site, uri string) (string, error) {
	if owner.Endpoint == "" || receiver.Endpoint == "" || owner.Endpoint == receiver.Endpoint || !strings.HasPrefix(uri, "/") {
		return "", fmt.Errorf("outcome not observed: the endpoints %q and %q or the request %q cannot tell a redirect's target", owner.Endpoint, receiver.Endpoint, uri)
	}
	if answer.Status != http.StatusMovedPermanently {
		return "", fmt.Errorf("condition not established: the gateway of %s answered %d to %s, not 301", receiver.Name, answer.Status, uri)
	}
	if len(answer.Locations) != 1 {
		return "", fmt.Errorf("another state: the redirect of %s has %d Location headers, want one: %q", uri, len(answer.Locations), answer.Locations)
	}
	switch answer.Locations[0] {
	case strings.TrimSuffix(owner.Endpoint, "/") + uri:
		return targetOwner, nil
	case strings.TrimSuffix(receiver.Endpoint, "/") + uri:
		return targetReceiver, nil
	}
	return "", fmt.Errorf("another state: the Location %q of %s is neither the endpoint of the owner %s nor of the receiver %s followed by the request", answer.Locations[0], uri, owner.Endpoint, receiver.Endpoint)
}

// redirectObservation is one classified redirect: the request, the
// zonegroup that received it, the one that holds the bucket, the Location
// and the target it names.
type redirectObservation struct {
	Request, Receiver, Owner, Location, Target string
}

// judgeRedirects decides what the redirects show. There must be a redirect
// received by each of the two zonegroups, so the run has seen each endpoint
// in a Location or has seen that it never appears there. The symptoms are
// returned when every redirect names its receiver, and neither symptoms nor
// an error when every redirect names its owner. Redirects that disagree are
// an error.
func judgeRedirects(observations []redirectObservation) ([]string, error) {
	receivers, targets := map[string]bool{}, map[string]int{}
	var symptoms []string
	for _, o := range observations {
		if o.Receiver == "" || o.Owner == "" || o.Receiver == o.Owner {
			return nil, fmt.Errorf("outcome not observed: %s was received by %q for a bucket of %q", o.Request, o.Receiver, o.Owner)
		}
		receivers[o.Receiver] = true
		switch o.Target {
		case targetOwner:
		case targetReceiver:
			symptoms = append(symptoms, fmt.Sprintf("the zonegroup %s answered %s for a bucket of the zonegroup %s with a redirect to itself: %s", o.Receiver, o.Request, o.Owner, o.Location))
		default:
			return nil, fmt.Errorf("outcome not observed: the redirect of %s names %q", o.Request, o.Target)
		}
		targets[o.Target]++
	}
	if len(receivers) != 2 {
		return nil, fmt.Errorf("outcome not observed: redirects of %d zonegroups, want both: %+v", len(receivers), observations)
	}
	if targets[targetOwner] > 0 && targets[targetReceiver] > 0 {
		return nil, fmt.Errorf("another state: %d redirects name the owning zonegroup and %d the receiving one: %+v", targets[targetOwner], targets[targetReceiver], observations)
	}
	return symptoms, nil
}
