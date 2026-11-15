package jetstreamd

import "net/http"

// userAgentTransport overrides the User-Agent of every request it carries.
type userAgentTransport struct {
	base      http.RoundTripper
	userAgent string
}

func (t userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	req = req.Clone(req.Context())
	req.Header.Set("User-Agent", t.userAgent)
	return base.RoundTrip(req)
}
