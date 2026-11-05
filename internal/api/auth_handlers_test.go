package api

import "testing"

func TestUserInWhitelist(t *testing.T) {
	cases := []struct {
		name     string
		users    []string
		username string
		want     bool
	}{
		{"nil whitelist => allow all", nil, "alice", true},
		{"empty whitelist => deny all", []string{}, "alice", false},
		{"match exact", []string{"alice", "bob"}, "alice", true},
		{"match case-insensitive", []string{"Alice"}, "alice", true},
		{"match with surrounding spaces", []string{" alice "}, "alice", true},
		{"no match", []string{"alice", "bob"}, "carol", false},
		{"match target with spaces", []string{"alice"}, " Alice ", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := userInWhitelist(c.users, c.username); got != c.want {
				t.Fatalf("userInWhitelist(%v, %q) = %v, want %v", c.users, c.username, got, c.want)
			}
		})
	}
}
