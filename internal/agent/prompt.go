package agent

import (
	"encoding/json"
	"unicode/utf8"
)

// Count serialized provider-input bytes, not raw source bytes: quoting/control
// characters expand in the JSON text supplied to the model. A byte ceiling is
// a conservative token ceiling. Go's additional HTML escaping only overcounts
// Python's compact UTF-8 JSON. Schema/system overhead is verified in Python.
func BoundPrompt(n Next, role *RoleSpec, repository, revision string) (Next, int) {
	overhead := 8192
	if role != nil {
		overhead = 10240
	}
	for {
		input := map[string]any{"task": n.Task, "untrusted_context": n.Context, "workspace_sha256": n.Workspace}
		if role != nil {
			input["role"] = role
			input["repository_id"] = repository
			input["base_revision"] = revision
		}
		body, _ := json.Marshal(input)
		upper := len(body) + overhead
		if upper <= 30000 || len(n.Context) == 0 {
			return n, upper
		}
		n.Context = n.Context[:len(n.Context)/2]
		for !utf8.ValidString(n.Context) {
			n.Context = n.Context[:len(n.Context)-1]
		}
	}
}
