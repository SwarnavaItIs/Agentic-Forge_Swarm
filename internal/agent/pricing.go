package agent

import (
	"encoding/json"
	"forgeswarm/internal/sandbox"
	"io"
	"os"
	"time"
)

// Pricing is operator policy shared by the gateway and Python deployment, not a
// Go provider implementation. Values are dated estimates, never an invoice.
type Pricing struct {
	SchemaVersion int    `json:"schema_version"`
	Provider      string `json:"provider"`
	Model         string `json:"model"`
	VerifiedAt    string `json:"verified_at"`
	Source        string `json:"source"`
	Input         int64  `json:"input_microusd_per_million"`
	Output        int64  `json:"output_microusd_per_million"`
	Maximum       int64  `json:"maximum_estimate_microusd"`
	Note          string `json:"note"`
}

func (p Pricing) Valid() bool {
	_, err := time.Parse("2006-01-02", p.VerifiedAt)
	return p.SchemaVersion == 1 && p.Provider == "openai" && len(p.Model) > 0 && len(p.Model) <= 100 && err == nil && p.Input > 0 && p.Output > 0 && p.Input <= 100000000 && p.Output <= 100000000 && p.Maximum > 0 && p.Maximum <= 1000000 && len(p.Note) <= 800 && len(p.Source) <= 200
}
func LoadPricing(file string) (Pricing, error) {
	var p Pricing
	f, e := os.Open(file)
	if e != nil {
		return p, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, 2049))
	if e != nil || sandbox.Decode(b, &p, 2048) != nil || !p.Valid() {
		return p, sandbox.ErrPolicy
	}
	return p, nil
}
func (p Pricing) Estimate(input, output int) int64 {
	return (int64(input)*p.Input + int64(output)*p.Output + 999999) / 1000000
}
func (p Pricing) Bytes() []byte { b, _ := json.Marshal(p); return b }
