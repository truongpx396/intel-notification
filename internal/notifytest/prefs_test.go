package notifytest

import (
	"testing"

	"github.com/truongpx396/intel-notification/ports"
)

// The fake runs the same table as the PostgreSQL store (T018), so what it decides
// where the contract leaves room is decided for both.
func TestPreferencesFakeRunsTheStoreContract(t *testing.T) {
	t.Parallel()
	PreferenceStoreContract(t, func(*testing.T) ports.PreferenceStore { return NewPreferences() })
}
