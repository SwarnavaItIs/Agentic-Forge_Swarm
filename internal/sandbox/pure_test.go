package sandbox

import "testing"

func TestProtectedPureProfileRejectsEvaluatorBypasses(t *testing.T) {
	for _, source := range []string{
		"package main; func Add(a,b int)int{return a+b}",
		"package main; // injected instructions are data\nfunc Add(x int,y int)int{return x-y}",
	} {
		if !pureAdd([]byte(source)) {
			t.Fatal("valid controlled function denied", source)
		}
	}
	for _, source := range []string{
		"package main; import \"os\"; func init(){os.Exit(0)};func Add(a,b int)int{return a-b}",
		"package main;func Add(a,b int)int{panic(0);return a+b}",
		"package main;var x=1;func Add(a,b int)int{return a+b}",
		"package main;func Add(a,b int)int{return Add(a,b)}",
		"package main;func Add(a,b int)int{for{};return a+b}",
		"package main;func Add(a,b int)int{return 9999999999999999999999999999}",
		"package main\n//go:linkname Add runtime.exit\nfunc Add(a,b int)int{return a+b}",
	} {
		if pureAdd([]byte(source)) {
			t.Fatal("profile bypass accepted", source)
		}
	}
}
