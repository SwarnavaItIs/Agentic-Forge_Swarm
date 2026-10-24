package sandbox

import "testing"

func TestProtectedEvaluationRequiresEveryRealTestAndPackagePass(t *testing.T) {
	expected := []string{"TestProtected", "TestEdge"}
	valid := []byte("{\"Action\":\"run\",\"Test\":\"TestProtected\"}\n{\"Action\":\"pass\",\"Test\":\"TestProtected\"}\n{\"Action\":\"run\",\"Test\":\"TestEdge\"}\n{\"Action\":\"pass\",\"Test\":\"TestEdge\"}\n{\"Action\":\"pass\"}\n")
	if len(passedEvaluationTests(valid, expected)) != 2 {
		t.Fatal("valid protected transcript rejected")
	}
	for _, body := range [][]byte{nil, []byte("success\n"), []byte("{\"Action\":\"pass\"}\n"), []byte("{\"Action\":\"pass\",\"Test\":\"TestProtected\"}\n{\"Action\":\"pass\"}\n"), []byte("{\"Action\":\"run\",\"Test\":\"TestProtected\"}\n{\"Action\":\"skip\",\"Test\":\"TestProtected\"}\n{\"Action\":\"pass\"}\n")} {
		if len(passedEvaluationTests(body, expected)) == len(expected) {
			t.Fatal("fake, omitted or skipped tests accepted")
		}
	}
}
