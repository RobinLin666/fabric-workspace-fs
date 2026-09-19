package resources

import (
	"errors"
	"io/fs"
	"testing"
)

func TestEnvironmentBindingIDsAndFallback(t *testing.T) {
	const workspace = "11111111-1111-1111-1111-111111111111"
	const environment = "22222222-2222-2222-2222-222222222222"
	for _, test := range []struct {
		body, ws string
		bound    bool
	}{
		{`{"metadata":{}}`, "", false},
		{`{"metadata":{"dependencies":{"environment":null}}}`, "", false},
		{`{"metadata":{"dependencies":{"environment":{}}}}`, "", false},
		{`{"metadata":{"dependencies":{"environment":{"environmentId":"` + environment + `"}}}}`, workspace, true},
		{`{"metadata":{"trident":{"environment":{"environmentId":"` + environment + `"}}}}`, workspace, true},
		{`{"metadata":{"dependencies":{"environment":{"environmentId":"` + environment + `","workspaceId":"33333333-3333-3333-3333-333333333333"}}}}`, "33333333-3333-3333-3333-333333333333", true},
	} {
		got, err := EnvironmentBinding([]byte(test.body), workspace)
		if err != nil {
			t.Fatal(test.body, err)
		}
		if test.bound && (got == nil || got.ItemID != environment || got.WorkspaceID != test.ws || got.Kind != "Environment") {
			t.Fatal("binding identity changed", got)
		}
		if !test.bound && got != nil {
			t.Fatal("binding was invented", got)
		}
	}
	for _, body := range []string{
		`{"metadata":{"dependencies":{"environment":{"logicalId":"` + environment + `"}}}}`,
		`{"metadata":{"dependencies":{"environment":{"environmentId":"name-not-id"}}}}`,
		`{"metadata":{"dependencies":{"environment":{"environmentId":"` + environment + `","workspaceId":"../escape"}}}}`,
		`{"metadata":{"dependencies":{"environment":"invalid"}}}`,
	} {
		if _, err := EnvironmentBinding([]byte(body), workspace); !errors.Is(err, fs.ErrInvalid) {
			t.Fatal("unsafe binding accepted", body, err)
		}
	}
}
