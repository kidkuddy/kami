package engine

import (
	"net"
	"os"
	"strings"
	"time"
)

// Offline: with no network every step that would start fails (workers, Multica, GitLab, Slack) and
// a grace contract hands itself to AFK while the owner simply cannot see it. So the daemon checks
// the network, and while it is gone it holds every kekkai the way a pause does (runs park, probes,
// heartbeats and cron fires stop) and grace deadlines do not run out. Back online, parked runs
// start, and wait deadlines and grace deadlines move on by the time spent offline.
//
// KAMI_NET_PROBE: comma-separated host:port, online when any one accepts a TCP connection (DNS
// included). "off" disables the watch. KAMI_NET_EVERY: the check interval (Go duration).
const (
	defaultNetProbe = "dns.google:443,one.one.one.one:443"
	netFailsOffline = 3 // ponytail: 3 misses in a row (90s by default), so one dropped dial does not park everything
)

// WatchNetwork checks the network until Close. The daemon starts it; tests set KAMI_NET_PROBE.
func (e *Engine) WatchNetwork() {
	probe := os.Getenv("KAMI_NET_PROBE")
	if probe == "" {
		probe = defaultNetProbe
	}
	if probe == "off" {
		return
	}
	every, err := time.ParseDuration(os.Getenv("KAMI_NET_EVERY"))
	if err != nil || every <= 0 {
		every = 30 * time.Second
	}
	addrs := strings.Split(probe, ",")
	go func() {
		misses := 0
		for {
			ok := online(addrs, every)
			e.mu.Lock()
			if e.closed {
				e.mu.Unlock()
				return
			}
			if ok {
				misses = 0
				if e.offline != nil {
					e.goOnline()
				}
			} else if misses++; misses >= netFailsOffline && e.offline == nil {
				e.goOffline()
			}
			e.mu.Unlock()
			time.Sleep(every)
		}
	}()
}

func online(addrs []string, every time.Duration) bool {
	timeout := min(5*time.Second, every)
	for _, a := range addrs {
		if c, err := net.DialTimeout("tcp", strings.TrimSpace(a), timeout); err == nil {
			c.Close()
			return true
		}
	}
	return false
}

func (e *Engine) goOffline() {
	t := now()
	e.offline = &t
	e.event(nil, "kami.offline", map[string]any{})
}

func (e *Engine) goOnline() {
	away := now().Sub(*e.offline)
	e.offline = nil
	var resumed []string
	for _, id := range e.activeIDs() {
		r := e.runs[id]
		if e.held(r) { // still under a kekkai pause: its resume releases it
			continue
		}
		if e.release(r, away) {
			resumed = append(resumed, id)
		}
	}
	for _, c := range e.contracts { // the grace window did not count while offline
		if c.Status == "open" && c.Deadline != nil {
			d := c.Deadline.Add(away)
			c.Deadline = &d
			e.saveContract(c, "offline "+away.Round(time.Second).String())
			e.applyMode(c, false)
		}
	}
	e.event(nil, "kami.online", map[string]any{"offline_for": away.Round(time.Second).String(), "runs": resumed})
}
