package pgtest

import (
	"fmt"
	"strconv"
)

// DefaultImage is the PostgreSQL the suite runs against unless PGTEST_IMAGE says otherwise.
const DefaultImage = "postgres:16-alpine"

// imageFrom is the image a run uses: the value of PGTEST_IMAGE, or DefaultImage when it is unset.
func imageFrom(env string) string {
	if env == "" {
		return DefaultImage
	}
	return env
}

// checkMajor is the guard behind PGTEST_EXPECT_MAJOR. A CI leg that says it runs PostgreSQL 17
// states the major twice, once as the image to start and once as the major it expects, and the
// two come from different places, so a leg whose image never reached the suite fails here and
// does not pass as a run it never made. An empty expectation checks nothing.
func checkMajor(expect string, serverVersionNum int) error {
	if expect == "" {
		return nil
	}
	want, err := strconv.Atoi(expect)
	if err != nil {
		return fmt.Errorf("PGTEST_EXPECT_MAJOR=%q is not a major version number such as 17", expect)
	}
	if got := serverVersionNum / 10000; got != want {
		return fmt.Errorf("PGTEST_EXPECT_MAJOR=%d but the server is PostgreSQL %d: this run is not the one it was asked to be",
			want, got)
	}
	return nil
}
