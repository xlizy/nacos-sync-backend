package nacos

import "testing"

func TestIsConnectionError(t *testing.T) {
	for _, msg := range []string{
		"client not connected, current status:UNHEALTHY",
		"client not connected, current status:STARTING",
		"client is closed",
		"no available server",
		"Connection refused",
	} {
		if !IsConnectionError(errString(msg)) {
			t.Errorf("expected connection error for %q", msg)
		}
	}
	for _, msg := range []string{
		"permission denied",
		"invalid parameter",
		"service not found",
	} {
		if IsConnectionError(errString(msg)) {
			t.Errorf("unexpected connection error classification for %q", msg)
		}
	}
	if IsConnectionError(nil) {
		t.Fatal("nil must not be classified as a connection error")
	}
}

type errString string

func (e errString) Error() string { return string(e) }
