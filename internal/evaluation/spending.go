package evaluation

import (
	"encoding/json"
	"errors"
	"fmt"
	"forgeswarm/internal/agent"
	"github.com/google/uuid"
	"os"
	"path/filepath"
)

// Authorization is trusted local operator configuration, never model/task data.
// The CLI additionally requires an explicit spending-confirmation flag. A file
// does not itself establish that this Codex session may execute paid calls.
type Authorization struct {
	ID             string `json:"authorization_id"`
	AuthorizedBy   string `json:"authorized_by"`
	TotalMicrousd  int64  `json:"total_cap_microusd"`
	PerRunMicrousd int64  `json:"per_run_cap_microusd"`
	PricingHash    string `json:"pricing_sha256"`
	CatalogHash    string `json:"catalog_sha256"`
}
type Ledger struct {
	file, lock string
	auth       Authorization
	spent      int64
	calls      int
}
type ledgerState struct {
	AuthorizationHash string `json:"authorization_sha256"`
	Spent             int64  `json:"reserved_microusd"`
	Calls             int    `json:"calls"`
}

func OpenLedger(auth Authorization, pricing agent.Pricing, root string) (*Ledger, error) {
	id, err := uuid.Parse(auth.ID)
	if err != nil || id == uuid.Nil || auth.AuthorizedBy == "" || len(auth.AuthorizedBy) > 100 || auth.TotalMicrousd <= 0 || auth.TotalMicrousd > 100000000 || auth.PerRunMicrousd <= 0 || auth.PerRunMicrousd > auth.TotalMicrousd || auth.PricingHash != agent.SHA(pricing.Bytes()) {
		return nil, errors.New("valid existing operator authorization and exact pricing required")
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	ledger := &Ledger{file: filepath.Join(root, id.String()+".json"), lock: filepath.Join(root, id.String()+".lock"), auth: auth}
	lock, err := os.OpenFile(ledger.lock, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, errors.New("spending ledger locked; inspect interrupted execution before manual reconciliation")
	}
	_ = lock.Close()
	data, err := os.ReadFile(ledger.file)
	if err == nil {
		var state ledgerState
		if json.Unmarshal(data, &state) != nil || state.AuthorizationHash != agent.JSONSHA(auth) || state.Spent < 0 || state.Spent > auth.TotalMicrousd {
			ledger.Close()
			return nil, errors.New("spending ledger authorization mismatch")
		}
		ledger.spent, ledger.calls = state.Spent, state.Calls
	} else if !os.IsNotExist(err) {
		ledger.Close()
		return nil, err
	}
	return ledger, nil
}
func (l *Ledger) Close() { _ = os.Remove(l.lock) }
func (l *Ledger) Reservation(pricing agent.Pricing) Reserve {
	runCost := int64(0)
	return func(input, output int) error {
		amount := pricing.Estimate(input, output)
		if amount <= 0 || l.spent+amount > l.auth.TotalMicrousd || runCost+amount > l.auth.PerRunMicrousd {
			return ErrSpendingCap
		}
		next := ledgerState{agent.JSONSHA(l.auth), l.spent + amount, l.calls + 1}
		data, _ := json.Marshal(next)
		temp := l.file + ".tmp"
		file, err := os.OpenFile(temp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, err = file.Write(data)
		if err == nil {
			err = file.Sync()
		}
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		if err = os.Rename(temp, l.file); err != nil {
			return err
		}
		l.spent += amount
		l.calls++
		runCost += amount
		return nil
	}
}
func (l *Ledger) Summary() string {
	return fmt.Sprintf("Reserved %d microUSD across %d calls; uncertain calls retain their full reservation.", l.spent, l.calls)
}
