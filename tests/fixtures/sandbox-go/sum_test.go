package fixture

import "testing"

func TestSum(t *testing.T) {
	if Sum(20, 22) != 42 {
		t.Fatal("unexpected sum")
	}
}
