package media

import (
	"encoding/json"
	"net/http"
)

// GitSHA and GitBranch are stamped at build time via -ldflags "-X main.gitSHA=...".
// For the media package they are package-level vars so the /version handler can
// read them without importing main. When unset (e.g. `go run` / `go test`), the
// values are empty and /version reports "dev".
var (
	GitSHA    string
	GitBranch string
)

// HandleVersion writes a JSON build-info document. Mounted at /version so deploys
// can be verified against the expected commit without inspecting the binary.
func HandleVersion(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	sha := GitSHA
	branch := GitBranch
	if sha == "" {
		sha = "dev"
	}
	if branch == "" {
		branch = "dev"
	}
	_ = json.NewEncoder(w).Encode(map[string]string{
		"git_sha":    sha,
		"git_branch": branch,
	})
}
