package engine

import (
	"crypto/rand"
	"time"

	"kami/internal/kamui"
)

// Run statuses.
const (
	StRunning   = "running"  // a worker or script is executing
	StSleeping  = "sleeping" // an agent slept on a contract it raised
	StWaiting   = "waiting"  // a decision, wait or spawn is pending
	StDone      = "done"
	StFailed    = "failed"
	StCancelled = "cancelled"
)

// Terminal reports whether a run status is final.
func Terminal(s string) bool { return s == StDone || s == StFailed || s == StCancelled }

// QueuedSystem is a system instruction waiting for the run's current step to end.
type QueuedSystem struct {
	Text  string         `json:"text"`
	State string         `json:"state,omitempty"`
	Vars  map[string]any `json:"vars,omitempty"`
}

// Run is one workflow instance. It is saved whole to memento on every change.
type Run struct {
	ID          string         `json:"id"`
	Workflow    string         `json:"workflow"`
	DefHash     string         `json:"def_hash"`
	Kekkai      string         `json:"kekkai"`
	Parent      string         `json:"parent,omitempty"`
	ParentIndex int            `json:"parent_index,omitempty"`
	Entry       string         `json:"entry"`
	State       string         `json:"state"`
	Status      string         `json:"status"`
	Vars        map[string]any `json:"vars"`
	// SystemQueue holds instructions sent while the run was busy; they apply when its step ends.
	SystemQueue []QueuedSystem `json:"system_queue,omitempty"`
	Visits      map[string]int `json:"visits"`
	// Sessions remembers each agent state's last provider session
	// ("provider:id"), so a resume = "session" step continues it on re-entry.
	Sessions map[string]string `json:"sessions,omitempty"`
	AFK      *kamui.AFK        `json:"afk_override,omitempty"`
	Report   string            `json:"report,omitempty"`
	Outcome  string            `json:"outcome,omitempty"` // terminal state name
	Step     *StepState        `json:"step,omitempty"`
	Created  time.Time         `json:"created_at"`
	Updated  time.Time         `json:"updated_at"`
}

// StepState is what the current state is doing.
type StepState struct {
	Kind     string     `json:"kind"` // a kamui step kind, or "blocked"
	Phase    string     `json:"phase"`
	Attempt  int        `json:"attempt,omitempty"`
	Token    string     `json:"token,omitempty"`
	Dir      string     `json:"dir,omitempty"`
	Worker   string     `json:"worker,omitempty"`
	Provider string     `json:"provider,omitempty"`
	Session  string     `json:"session,omitempty"`
	Contract string     `json:"contract,omitempty"`
	Purpose  string     `json:"purpose,omitempty"` // blocked: budget | stuck
	Signal   string     `json:"signal,omitempty"`
	Key      string     `json:"key,omitempty"`
	WakeAt   *time.Time `json:"wake_at,omitempty"`
	Deadline *time.Time `json:"deadline,omitempty"`
	Children []string   `json:"children,omitempty"`
	Silent   int        `json:"silent_exits,omitempty"` // attempts that exited without an outcome

	Pending *pending   `json:"pending,omitempty"` // outcome reported by `kami step done`, applied when the process exits (or on recovery)
	probeAt *time.Time // next probe; in memory only, a restart probes at once
	probing bool
	errors  int    // probe errors in a row; in memory only
	lastSay string // the probe's last say; in memory only, so a restart may repeat it once
	beating bool   // a heartbeat script is running
}

type pending struct {
	Outcome string         `json:"outcome"`
	Vars    map[string]any `json:"vars,omitempty"`
}

// Contract is one open or resolved decision.
type Contract struct {
	ID         string            `json:"id"`
	RunID      string            `json:"run_id"`
	Step       string            `json:"step"`
	Kekkai     string            `json:"kekkai"`
	Origin     string            `json:"origin"` // state | worker | engine
	Kind       string            `json:"kind"`
	Title      string            `json:"title"`
	Body       string            `json:"body,omitempty"`
	Options    []string          `json:"options,omitempty"`
	Fields     map[string]string `json:"fields,omitempty"`
	Default    string            `json:"default,omitempty"`
	Lock       bool              `json:"lock"`
	Mode       *kamui.AFK        `json:"mode,omitempty"` // declared by the state or workflow
	Override   *kamui.AFK        `json:"override,omitempty"`
	Resolver   string            `json:"resolver,omitempty"`
	Status     string            `json:"status"` // open | resolved | cancelled
	Deadline   *time.Time        `json:"deadline,omitempty"`
	Attempt    int               `json:"attempt,omitempty"`
	Resolution *Resolution       `json:"resolution,omitempty"`
	Created    time.Time         `json:"created_at"`
	Updated    time.Time         `json:"updated_at"`

	resolving bool // a resolver process is running
}

// Resolution is how a contract was answered.
type Resolution struct {
	Answer   string         `json:"answer"`
	Data     map[string]any `json:"data,omitempty"`
	By       string         `json:"by"` // human | resolver
	Resolver string         `json:"resolver,omitempty"`
	Note     string         `json:"note,omitempty"`
	At       time.Time      `json:"resolved_at"`
}

// Aura is a compact snapshot a worker writes before sleeping.
type Aura struct {
	RunID     string         `json:"run_id"`
	Step      string         `json:"step"`
	Seq       int            `json:"seq"`
	Goal      string         `json:"goal"`
	Progress  []string       `json:"progress,omitempty"`
	Next      string         `json:"next"`
	Vars      map[string]any `json:"vars,omitempty"`
	WaitingOn string         `json:"waiting_on,omitempty"`
	Session   *AuraSession   `json:"session,omitempty"`
	Created   time.Time      `json:"created_at"`
}

// AuraSession records the provider session behind an Aura.
type AuraSession struct {
	Provider      string `json:"provider"`
	ID            string `json:"id"`
	ResumeAllowed bool   `json:"resume_allowed"`
}

const crockford = "0123456789abcdefghjkmnpqrstvwxyz"

// NewID returns a lowercase, time-sortable ULID-style id: 48 bits of
// milliseconds then random bits, 26 Crockford base32 characters.
func NewID() string {
	var b [17]byte // 130 bits of room for 26 chars; the last 2 bits stay zero
	ms := uint64(time.Now().UnixMilli())
	for i := 5; i >= 0; i-- {
		b[i] = byte(ms)
		ms >>= 8
	}
	rand.Read(b[6:16])
	out := make([]byte, 26)
	for i := range out {
		bit := i * 5
		v := (uint(b[bit/8])<<8 | uint(b[bit/8+1])) >> (11 - bit%8) & 31
		out[i] = crockford[v]
	}
	return string(out)
}

func token() string {
	var b [16]byte
	rand.Read(b[:])
	const hex = "0123456789abcdef"
	out := make([]byte, 32)
	for i, c := range b {
		out[i*2], out[i*2+1] = hex[c>>4], hex[c&15]
	}
	return string(out)
}
