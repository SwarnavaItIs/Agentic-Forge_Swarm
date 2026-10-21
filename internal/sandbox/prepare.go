package sandbox

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// PrepareImage is an explicit trusted setup step with no repository input. Its
// layers contain a fixed offline Go cache/harness and immutable helper over the
// digest-pinned toolchain. No daemon
// socket or writable harness mount is exposed to repository execution.
func PrepareImage(ctx context.Context, helper string) (string, error) {
	f, err := os.Open(helper)
	if err != nil {
		return "", err
	}
	body, err := io.ReadAll(io.LimitReader(f, 16<<20+1))
	_ = f.Close()
	if err != nil || len(body) > 16<<20 || len(body) < 4 || !bytes.Equal(body[:4], []byte{127, 'E', 'L', 'F'}) {
		return "", ErrPolicy
	}
	hash := sha256.Sum256(body)
	id := hex.EncodeToString(hash[:])
	if _, err = dockerCall(ctx, nil, 128<<10, "image", "inspect", Image); err != nil {
		return "", fmt.Errorf("prepare the pinned base image explicitly first: %w", err)
	}
	dockerfile := []byte("FROM " + Image + "\nENV CGO_ENABLED=0 GOMAXPROCS=2 GOFLAGS=-p=1 GOPROXY=off GOSUMDB=off GOTELEMETRY=off GOTOOLCHAIN=local GOCACHE=/opt/forgeswarm-cache\nCOPY harness_test.go /trusted/harness_test.go\nRUN [\"/usr/local/go/bin/go\",\"test\",\"/trusted/harness_test.go\"]\nCOPY --chmod=0555 forgeswarm-sandbox /usr/local/bin/forgeswarm-sandbox\nLABEL forgeswarm.helper_sha256=" + id + "\nLABEL forgeswarm.base_image=" + Image + "\n")
	harness := []byte("package harness\nimport(\"testing\";_ \"bytes\";_ \"encoding/json\";_ \"fmt\";_ \"net\";_ \"os/exec\";_ \"strings\";_ \"syscall\";_ \"time\";_ \"crypto/sha256\")\nfunc TestHarness(t *testing.T){}\n")
	var input bytes.Buffer
	tw := tar.NewWriter(&input)
	for _, file := range []struct {
		name string
		body []byte
	}{{"Dockerfile", dockerfile}, {"harness_test.go", harness}, {"forgeswarm-sandbox", body}} {
		if err = tw.WriteHeader(&tar.Header{Name: file.name, Mode: 0555, Size: int64(len(file.body)), Typeflag: tar.TypeReg}); err != nil {
			return "", err
		}
		if _, err = tw.Write(file.body); err != nil {
			return "", err
		}
	}
	if err = tw.Close(); err != nil {
		return "", err
	}
	tag := "forgeswarm-execution:" + id
	if _, err = dockerCall(ctx, &input, 32<<10, "build", "--platform", "linux/amd64", "--network", "none", "--pull=false", "--tag", tag, "-"); err != nil {
		return "", err
	}
	return tag, nil
}
