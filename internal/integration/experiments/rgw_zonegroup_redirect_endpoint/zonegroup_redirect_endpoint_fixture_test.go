//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_zonegroup_redirect_endpoint_test

import (
	"net/http"
	"reflect"
	"strings"
	"testing"
)

// The reports below have the form "period get" prints. They are synthetic
// inputs of the reader, not native evidence of a realm.
func periodGroup(name, endpoints string) string {
	return `{"id":"id-` + name + `","name":"` + name + `","api_name":"` + name + `","is_master":true,"endpoints":` + endpoints + `,"zones":[]}`
}

func periodReport(id, epoch string, groups ...string) []byte {
	return []byte(`{"id":"` + id + `","epoch":` + epoch + `,"period_map":{"id":"` + id + `","zonegroups":[` + strings.Join(groups, ",") + `]},"master_zonegroup":"id-us"}`)
}

var (
	siteUS = site{Name: "us", APIName: "us", Endpoint: "http://rgw-a:7480"}
	siteEU = site{Name: "eu", APIName: "eu", Endpoint: "http://rgw-b:7480"}
)

func TestParsePeriodReadsTheZonegroups(t *testing.T) {
	got, err := parsePeriod(periodReport("p1", "3", periodGroup("us", `["http://rgw-a:7480"]`), periodGroup("eu", `[]`)))
	if err != nil {
		t.Fatal(err)
	}
	want := periodView{ID: "p1", Epoch: 3, Zonegroups: []zonegroupView{
		{Name: "us", APIName: "us", Endpoints: []string{"http://rgw-a:7480"}},
		{Name: "eu", APIName: "eu", Endpoints: []string{}},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("period = %+v, want %+v", got, want)
	}
}

func TestParsePeriodRefusesWhatItCannotRead(t *testing.T) {
	for name, report := range map[string]string{
		"not JSON":               `failed to read the period`,
		"no ID":                  `{"epoch":3,"period_map":{"zonegroups":[]}}`,
		"an empty ID":            string(periodReport("", "3")),
		"no epoch":               `{"id":"p1","period_map":{"zonegroups":[]}}`,
		"a null epoch":           string(periodReport("p1", "null")),
		"no period map":          `{"id":"p1","epoch":3}`,
		"no zonegroup list":      `{"id":"p1","epoch":3,"period_map":{}}`,
		"a null zonegroup list":  `{"id":"p1","epoch":3,"period_map":{"zonegroups":null}}`,
		"zonegroup without name": string(periodReport("p1", "3", `{"api_name":"us","endpoints":[]}`)),
		"zonegroup without API":  string(periodReport("p1", "3", `{"name":"us","endpoints":[]}`)),
		"zonegroup without list": string(periodReport("p1", "3", `{"name":"us","api_name":"us"}`)),
		"zonegroup, null list":   string(periodReport("p1", "3", periodGroup("us", "null"))),
	} {
		if got, err := parsePeriod([]byte(report)); err == nil {
			t.Errorf("%s: read as %+v", name, got)
		}
	}
}

func TestAdvertisedSites(t *testing.T) {
	period := func(id string, epoch int64, groups ...zonegroupView) periodView {
		return periodView{ID: id, Epoch: epoch, Zonegroups: groups}
	}
	group := func(name string, endpoints ...string) zonegroupView {
		return zonegroupView{Name: name, APIName: name, Endpoints: endpoints}
	}
	us, eu := group("us", siteUS.Endpoint), group("eu", siteEU.Endpoint)
	a, b, err := advertisedSites([]periodView{period("p1", 3, us, eu), period("p1", 3, eu, us)}, "us", "eu")
	if err != nil || a != siteUS || b != siteEU {
		t.Fatalf("sites = %+v, %+v, %v", a, b, err)
	}
	// Endpoints two slashes apart start different Locations and stay distinct.
	if _, _, err := advertisedSites([]periodView{period("p1", 3, us, group("eu", siteUS.Endpoint+"//")), period("p1", 3, us, group("eu", siteUS.Endpoint+"//"))}, "us", "eu"); err != nil {
		t.Errorf("endpoints two slashes apart: %v", err)
	}
	for name, periods := range map[string][]periodView{
		"one zone's period":          {period("p1", 3, us, eu)},
		"different periods":          {period("p1", 3, us, eu), period("p2", 3, us, eu)},
		"different epochs":           {period("p1", 3, us, eu), period("p1", 4, us, eu)},
		"a zonegroup missing":        {period("p1", 3, us), period("p1", 3, us)},
		"a zonegroup twice":          {period("p1", 3, us, eu, eu), period("p1", 3, us, eu, eu)},
		"a zonegroup, no endpoint":   {period("p1", 3, us, group("eu")), period("p1", 3, us, group("eu"))},
		"a zonegroup, two endpoints": {period("p1", 3, us, group("eu", siteEU.Endpoint, "http://rgw-c:7480")), period("p1", 3, us, group("eu", siteEU.Endpoint, "http://rgw-c:7480"))},
		"an empty endpoint":          {period("p1", 3, us, group("eu", "")), period("p1", 3, us, group("eu", ""))},
		"no API name":                {period("p1", 3, us, zonegroupView{Name: "eu", Endpoints: []string{siteEU.Endpoint}}), period("p1", 3, us, zonegroupView{Name: "eu", Endpoints: []string{siteEU.Endpoint}})},
		"zones that disagree":        {period("p1", 3, us, eu), period("p1", 3, us, group("eu", "http://rgw-c:7480"))},
		"the same endpoint":          {period("p1", 3, us, group("eu", siteUS.Endpoint)), period("p1", 3, us, group("eu", siteUS.Endpoint))},
		"endpoints one slash apart":  {period("p1", 3, us, group("eu", siteUS.Endpoint+"/")), period("p1", 3, us, group("eu", siteUS.Endpoint+"/"))},
		"both endpoints slashed":     {period("p1", 3, group("us", siteUS.Endpoint+"/"), group("eu", siteUS.Endpoint+"/")), period("p1", 3, group("us", siteUS.Endpoint+"/"), group("eu", siteUS.Endpoint+"/"))},
		"an endpoint of one slash":   {period("p1", 3, us, group("eu", "/")), period("p1", 3, us, group("eu", "/"))},
		"an API name with space":     {period("p1", 3, us, zonegroupView{Name: "eu", APIName: "us ", Endpoints: []string{siteEU.Endpoint}}), period("p1", 3, us, zonegroupView{Name: "eu", APIName: "us ", Endpoints: []string{siteEU.Endpoint}})},
		"the same API name":          {period("p1", 3, us, zonegroupView{Name: "eu", APIName: "us", Endpoints: []string{siteEU.Endpoint}}), period("p1", 3, us, zonegroupView{Name: "eu", APIName: "us", Endpoints: []string{siteEU.Endpoint}})},
	} {
		if a, b, err := advertisedSites(periods, "us", "eu"); err == nil || !strings.HasPrefix(err.Error(), "condition not established") {
			t.Errorf("%s: %+v, %+v, %v", name, a, b, err)
		}
	}
}

func TestParseLocationConstraint(t *testing.T) {
	for body, want := range map[string]string{
		`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">eu</LocationConstraint>`: "eu",
		`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`:                                         "",
	} {
		if got, err := parseLocationConstraint([]byte(body)); err != nil || got != want {
			t.Errorf("%s = %q, %v, want %q", body, got, err, want)
		}
	}
	for _, body := range []string{``, `not XML`, `<Error><Code>NoSuchBucket</Code></Error>`} {
		if got, err := parseLocationConstraint([]byte(body)); err == nil {
			t.Errorf("%q: read as %q", body, got)
		}
	}
}

func TestClassifyRedirect(t *testing.T) {
	const uri = "/tc-redirect-us/object"
	redirect := func(locations ...string) redirectAnswer {
		return redirectAnswer{Status: http.StatusMovedPermanently, Locations: locations}
	}
	// The bucket is in us and the request went to eu.
	for location, want := range map[string]string{
		siteUS.Endpoint + uri: targetOwner,
		siteEU.Endpoint + uri: targetReceiver,
	} {
		if got, err := classifyRedirect(redirect(location), siteUS, siteEU, uri); err != nil || got != want {
			t.Errorf("%s = %q, %v, want %q", location, got, err, want)
		}
	}
	// The query of the request is part of the Location, and an endpoint's
	// trailing slash is not.
	const listing = "/tc-redirect-us?list-type=2&prefix=obj"
	slashed := site{Name: "us", APIName: "us", Endpoint: siteUS.Endpoint + "/"}
	if got, err := classifyRedirect(redirect(siteUS.Endpoint+listing), slashed, siteEU, listing); err != nil || got != targetOwner {
		t.Errorf("listing = %q, %v", got, err)
	}
	for name, test := range map[string]struct {
		answer          redirectAnswer
		owner, receiver site
		uri, reason     string
	}{
		"the object itself":            {redirectAnswer{Status: http.StatusOK}, siteUS, siteEU, uri, "condition not established"},
		"no such bucket":               {redirectAnswer{Status: http.StatusNotFound}, siteUS, siteEU, uri, "condition not established"},
		"a temporary redirect":         {redirectAnswer{Status: http.StatusFound, Locations: []string{siteEU.Endpoint + uri}}, siteUS, siteEU, uri, "condition not established"},
		"a 308":                        {redirectAnswer{Status: http.StatusPermanentRedirect, Locations: []string{siteEU.Endpoint + uri}}, siteUS, siteEU, uri, "condition not established"},
		"no Location":                  {redirect(), siteUS, siteEU, uri, "another state"},
		"two Locations":                {redirect(siteUS.Endpoint+uri, siteEU.Endpoint+uri), siteUS, siteEU, uri, "another state"},
		"an empty Location":            {redirect(""), siteUS, siteEU, uri, "another state"},
		"the endpoint alone":           {redirect(siteEU.Endpoint), siteUS, siteEU, uri, "another state"},
		"another request":              {redirect(siteEU.Endpoint + "/tc-redirect-us/other"), siteUS, siteEU, uri, "another state"},
		"the request without a query":  {redirect(siteEU.Endpoint + "/tc-redirect-us"), siteUS, siteEU, listing, "another state"},
		"a longer host":                {redirect("http://rgw-b:74801" + uri), siteUS, siteEU, uri, "another state"},
		"a third endpoint":             {redirect("http://rgw-c:7480" + uri), siteUS, siteEU, uri, "another state"},
		"another scheme":               {redirect("https://rgw-b:7480" + uri), siteUS, siteEU, uri, "another state"},
		"the same endpoints":           {redirect(siteUS.Endpoint + uri), siteUS, siteUS, uri, "outcome not observed"},
		"endpoints one slash apart":    {redirect(siteUS.Endpoint + uri), siteUS, site{Name: "eu", APIName: "eu", Endpoint: siteUS.Endpoint + "/"}, uri, "outcome not observed"},
		"the receiver, a slash apart":  {redirect(siteUS.Endpoint + uri), site{Name: "us", APIName: "us", Endpoint: siteUS.Endpoint + "/"}, siteUS, uri, "outcome not observed"},
		"an owner endpoint of a slash": {redirect(uri), site{Name: "us", Endpoint: "/"}, siteEU, uri, "outcome not observed"},
		"no owner endpoint":            {redirect(siteEU.Endpoint + uri), site{Name: "us"}, siteEU, uri, "outcome not observed"},
		"no receiver endpoint":         {redirect(siteUS.Endpoint + uri), siteUS, site{Name: "eu"}, uri, "outcome not observed"},
		"a request without a path":     {redirect(siteEU.Endpoint), siteUS, siteEU, "", "outcome not observed"},
	} {
		got, err := classifyRedirect(test.answer, test.owner, test.receiver, test.uri)
		if err == nil || !strings.HasPrefix(err.Error(), test.reason) {
			t.Errorf("%s: %q, %v, want an error that starts with %q", name, got, err, test.reason)
		}
	}
}

func TestJudgeRedirects(t *testing.T) {
	observation := func(receiver, owner, target string) redirectObservation {
		return redirectObservation{Request: "GET /tc-redirect-" + owner + "/object", Receiver: receiver, Owner: owner, Location: "http://rgw:7480/tc-redirect-" + owner + "/object", Target: target}
	}
	// Every redirect names its receiver: the symptom, once per redirect.
	looped := []redirectObservation{observation("eu", "us", targetReceiver), observation("eu", "us", targetReceiver), observation("us", "eu", targetReceiver)}
	if symptoms, err := judgeRedirects(looped); err != nil || len(symptoms) != 3 {
		t.Fatalf("redirects to the receiver = %q, %v", symptoms, err)
	}
	// Every redirect names its owner.
	if symptoms, err := judgeRedirects([]redirectObservation{observation("eu", "us", targetOwner), observation("us", "eu", targetOwner)}); err != nil || len(symptoms) != 0 {
		t.Fatalf("redirects to the owner = %q, %v", symptoms, err)
	}
	for name, test := range map[string]struct {
		observations []redirectObservation
		reason       string
	}{
		"no redirect":               {nil, "outcome not observed"},
		"one receiving zonegroup":   {[]redirectObservation{observation("eu", "us", targetReceiver), observation("eu", "us", targetReceiver)}, "outcome not observed"},
		"a target without a name":   {[]redirectObservation{observation("eu", "us", ""), observation("us", "eu", targetOwner)}, "outcome not observed"},
		"an unknown target":         {[]redirectObservation{observation("eu", "us", "elsewhere"), observation("us", "eu", targetOwner)}, "outcome not observed"},
		"a request to the owner":    {[]redirectObservation{observation("us", "us", targetOwner), observation("us", "eu", targetOwner)}, "outcome not observed"},
		"no receiver":               {[]redirectObservation{observation("", "us", targetOwner), observation("us", "eu", targetOwner)}, "outcome not observed"},
		"redirects that disagree":   {[]redirectObservation{observation("eu", "us", targetReceiver), observation("us", "eu", targetOwner)}, "another state"},
		"one zonegroup, both forms": {[]redirectObservation{observation("eu", "us", targetReceiver), observation("eu", "us", targetOwner), observation("us", "eu", targetOwner)}, "another state"},
	} {
		symptoms, err := judgeRedirects(test.observations)
		if err == nil || !strings.HasPrefix(err.Error(), test.reason) {
			t.Errorf("%s: %q, %v, want an error that starts with %q", name, symptoms, err, test.reason)
		}
	}
}
