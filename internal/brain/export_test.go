package brain

import "websocket/internal/media"

// ResolveBrainTenantForTest exposes resolveBrainTenant to the brain_test package.
func ResolveBrainTenantForTest(session *media.Session, fallback string) string {
	return resolveBrainTenant(session, fallback)
}
