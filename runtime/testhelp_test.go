package runtime

import (
	"fmt"
	"os"
	"testing"
)

// bizLog prints a business-tagged test line so that `go test -v` output
// shows which business area and which case is covered.
func bizLog(t testing.TB, biz, caseName, format string, args ...any) {
	t.Helper()
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stdout, "    [biz:%s] case=%s %s\n", biz, caseName, msg)
}
