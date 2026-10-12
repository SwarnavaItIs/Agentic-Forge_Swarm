package execution

import (
	"bytes"
	"github.com/google/uuid"
	"strings"
	"testing"
)

func TestNotificationAndOutcomeBounds(t *testing.T) {
	valid := `{"schema_version":1,"task_id":"` + uuid.NewString() + `","dispatch_generation":1}`
	if _, err := DecodeNotification([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{valid + ` {}`, `{"schema_version":2}`, strings.Repeat("x", 4097), valid[:len(valid)-1] + `,"token":"forbidden"`, valid[:len(valid)-1] + `,"schema_version":1}`} {
		if _, err := DecodeNotification([]byte(body)); err == nil {
			t.Fatal("invalid notification accepted")
		}
	}
	one, h1, err := Outcome([]byte(`{"b":2,"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	two, h2, err := Outcome([]byte(`{ "a":1, "b":2 }`))
	if err != nil || h1 != h2 || !bytes.Equal(one, two) {
		t.Fatal("logical object retry mismatch")
	}
	for _, body := range []string{`[]`, `{"a":1,"a":2}`, `{} {}`, strings.Repeat("x", 4097), strings.Repeat(`{"a":`, 18) + `0` + strings.Repeat(`}`, 18)} {
		if _, _, err := Outcome([]byte(body)); err == nil {
			t.Fatal("unsafe result accepted")
		}
	}
	if !DefaultLimits().Valid() {
		t.Fatal("invalid defaults")
	}
}
