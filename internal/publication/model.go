// Package publication defines trusted approval bindings, never model tools.
package publication

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/sandbox"
	"github.com/google/uuid"
	"regexp"
	"slices"
	"strings"
	"time"
)

const PolicyVersion = "controlled-publish-v1"
const Action = "draft_pull_request"

var SHA = regexp.MustCompile("^[0-9a-f]{40}$")
var name = regexp.MustCompile("^[A-Za-z0-9_-]{1,100}$")
var branch = regexp.MustCompile("^[A-Za-z0-9][A-Za-z0-9/_-]{0,99}$")

type Repository struct {
	ID     int64  `json:"id"`
	Owner  string `json:"owner"`
	Name   string `json:"name"`
	Target string `json:"target_branch"`
}

func (r Repository) Valid() bool {
	return r.ID > 0 && name.MatchString(r.Owner) && name.MatchString(r.Name) && branch.MatchString(r.Target) && !strings.Contains(r.Target, "//") && !strings.HasSuffix(r.Target, "/")
}

type Prepared struct {
	Repository  Repository       `json:"repository"`
	Issue       int              `json:"issue_number"`
	Description string           `json:"description"`
	BaseTree    string           `json:"base_tree"`
	Snapshot    sandbox.Snapshot `json:"snapshot"`
	Policy      string           `json:"policy_version"`
	TestDemo    bool             `json:"test_demo"`
}

func (p Prepared) Valid() bool {
	if !p.Repository.Valid() || p.Issue < 1 || p.Issue > 1000000000 || len(p.Description) < 1 || len(p.Description) > 2000 || !SHA.MatchString(p.BaseTree) || !SHA.MatchString(p.Snapshot.BaseRevision) || !agent.ControlledMath(p.Snapshot) || p.Policy != PolicyVersion {
		return false
	}
	for n := range p.Snapshot.Files {
		if !slices.Contains([]string{"go.mod", "add.go", "double.go", "score.go", "README.txt"}, n) {
			return false
		}
	}
	return p.BaseTree == TreeSHA(p.Snapshot)
}

type Binding struct {
	Root          uuid.UUID  `json:"root_id"`
	Workflow      uuid.UUID  `json:"workflow_id"`
	PlanVersion   int        `json:"plan_version"`
	PlanDigest    string     `json:"plan_sha256"`
	Repository    Repository `json:"repository"`
	BaseCommit    string     `json:"base_commit"`
	BaseTree      string     `json:"base_tree"`
	Candidate     string     `json:"candidate_sha256"`
	CandidateTree string     `json:"candidate_tree"`
	Patch         string     `json:"patch_sha256"`
	Test          string     `json:"test_evidence_sha256"`
	Review        string     `json:"review_sha256"`
	Action        string     `json:"requested_action"`
	Policy        string     `json:"policy_version"`
	Flags         []string   `json:"policy_flags"`
	TestDemo      bool       `json:"test_demo"`
}

func (b Binding) Digest() string { return agent.JSONSHA(b) }
func Flags(paths []string) []string {
	flags := []string{}
	for _, p := range paths {
		l := strings.ToLower(p)
		if strings.HasPrefix(l, ".github/") || strings.Contains(l, "auth") || strings.HasPrefix(l, "infra") || strings.HasPrefix(l, "migrations/") {
			flags = append(flags, "sensitive_change_requires_additional_human_review")
		}
	}
	slices.Sort(flags)
	return slices.Compact(flags)
}

type Decision struct {
	ExpectedVersion      int    `json:"expected_version"`
	Binding              string `json:"binding_sha256"`
	Candidate            string `json:"candidate_sha256"`
	Decision             string `json:"decision"`
	AcknowledgeSensitive bool   `json:"acknowledge_sensitive"`
}
type Operation struct {
	ID          uuid.UUID        `json:"id"`
	Approval    uuid.UUID        `json:"approval_id"`
	Binding     Binding          `json:"binding"`
	Snapshot    sandbox.Snapshot `json:"-"`
	CreatedAt   time.Time        `json:"created_at"`
	Status      string           `json:"status"`
	Attempts    int              `json:"attempts"`
	Branch      string           `json:"branch"`
	Head        string           `json:"head_commit"`
	PRNumber    int              `json:"pr_number"`
	URL         string           `json:"url"`
	Reason      string           `json:"reason"`
	NextAttempt time.Time        `json:"next_attempt_at"`
}

func (o Operation) Marker() string { return "<!-- forgeswarm-operation:" + o.ID.String() + " -->" }
func (o Operation) Message() string {
	return "ForgeSwarm operation " + o.ID.String() + "\n\nCandidate: " + o.Binding.Candidate + "\n"
}
func (o Operation) CommitSHA() string {
	who := fmt.Sprintf("ForgeSwarm <forgeswarm@localhost> %d +0000", o.CreatedAt.Unix())
	return ObjectSHA("commit", []byte("tree "+o.Binding.CandidateTree+"\nparent "+o.Binding.BaseCommit+"\nauthor "+who+"\ncommitter "+who+"\n\n"+o.Message()))
}

// SHA-1 here represents Git object identity. Approval/evidence uses SHA-256.
func ObjectSHA(kind string, body []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "%s %d%c", kind, len(body), byte(0))
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}
func TreeSHA(s sandbox.Snapshot) string {
	names := []string{}
	for n := range s.Files {
		names = append(names, n)
	}
	slices.Sort(names)
	var body bytes.Buffer
	for _, n := range names {
		body.WriteString("100644 " + n)
		body.WriteByte(0)
		id, _ := hex.DecodeString(ObjectSHA("blob", s.Files[n]))
		body.Write(id)
	}
	return ObjectSHA("tree", body.Bytes())
}
