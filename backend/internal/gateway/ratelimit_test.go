package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractIPIgnoresHeadersFromLoopbackPeer(t *testing.T) {
	req := httptest.NewRequest("POST", "/api/auth/login", nil)
	req.RemoteAddr = "127.0.0.1:54321"
	req.Header.Set("X-Real-IP", "203.0.113.10")
	require.Equal(t, "127.0.0.1", extractIP(req))
}

func TestExtractIPIgnoresForwardedHeadersFromUntrustedPeer(t *testing.T) {
	req := httptest.NewRequest("POST", "/api/auth/login", nil)
	req.RemoteAddr = "198.51.100.20:54321"
	req.Header.Set("X-Real-IP", "203.0.113.10")
	req.Header.Set("X-Forwarded-For", "203.0.113.11")
	require.Equal(t, "198.51.100.20", extractIP(req))
}

func TestExtractIPReturnsUnsplitPeerWhenPortIsMissing(t *testing.T) {
	req := httptest.NewRequest("POST", "/api/auth/login", nil)
	req.RemoteAddr = "127.0.0.1"
	require.Equal(t, "127.0.0.1", extractIP(req))
}

func TestLoginRateLimiterDelegatesLoopbackProxyLimitingToEdge(t *testing.T) {
	s := &Server{}
	calls := 0
	h := s.loginRateLimiter(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	for range 10 {
		req := httptest.NewRequest(http.MethodPost, "/api/auth/login", nil)
		req.RemoteAddr = "127.0.0.1:54321"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.NotEqual(t, http.StatusTooManyRequests, rec.Code)
	}
	require.Equal(t, 10, calls)
}

func TestLoginRateLimiterStillProtectsDirectRemoteClients(t *testing.T) {
	s := &Server{}
	h := s.loginRateLimiter(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	for i := 1; i <= 6; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/auth/login", nil)
		req.RemoteAddr = "198.51.100.20:54321"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if i <= 5 {
			require.Equal(t, http.StatusUnauthorized, rec.Code)
		} else {
			require.Equal(t, http.StatusTooManyRequests, rec.Code)
		}
	}
}
