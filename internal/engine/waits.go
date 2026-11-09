package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"kami/internal/kamui"
	"kami/internal/kekkai"
)

func (e *Engine) armWait(r *Run, s *kamui.Step) {
	t := now()
	r.Step.Phase, r.Status = "waiting", StWaiting
	if s.Signal != "" {
		r.Step.Signal = s.Signal
		r.Step.Key, _ = kamui.Render(s.Key, r.Vars)
	}
	if s.Timer > 0 {
		w := t.Add(s.Timer)
		r.Step.WakeAt = &w
	}
	if s.Timeout > 0 {
		d := t.Add(s.Timeout)
		r.Step.Deadline = &d
	}
	e.saveRun(r, "waiting")
	e.event(r, "wait.armed", map[string]any{"state": r.State, "signal": r.Step.Signal, "key": r.Step.Key,
		"wake_at": r.Step.WakeAt, "deadline": r.Step.Deadline, "probe": s.Probe})
	e.scheduleRun(r)
}

// scheduleRun arms the run's single timer for its next wake, timeout or probe.
func (e *Engine) scheduleRun(r *Run) {
	st := r.Step
	s := e.def(r).States[r.State].Step
	if s.Probe != "" && st.probeAt == nil && !st.probing {
		t := now()
		st.probeAt = &t // first probe right away, and right away after a restart
	}
	var next *time.Time
	for _, t := range []*time.Time{st.WakeAt, st.Deadline, st.probeAt} {
		if t != nil && (next == nil || t.Before(*next)) {
			next = t
		}
	}
	if next == nil {
		e.stopTimer("run:" + r.ID)
		return
	}
	id := r.ID
	e.setTimer("run:"+id, *next, func() { e.fireRun(id) })
}

func (e *Engine) fireRun(id string) {
	r := e.runs[id]
	if r == nil || r.Step == nil || r.Step.Kind != kamui.KWait || r.Step.Phase != "waiting" {
		return
	}
	t := now()
	st := r.Step
	if e.held(r) { // nothing fires while paused: resume moves the deadline by the pause
		e.probeHeld(r)
		e.scheduleRun(r)
		return
	}
	switch {
	case st.Deadline != nil && !t.Before(*st.Deadline):
		st.Phase = "done"
		e.outcome(r, "timeout", nil)
	case st.WakeAt != nil && !t.Before(*st.WakeAt):
		st.Phase = "done"
		e.outcome(r, "timer", nil)
	case st.probeAt != nil && !t.Before(*st.probeAt):
		st.probeAt = nil
		e.runProbe(r)
		e.scheduleRun(r)
	default:
		e.scheduleRun(r)
	}
}

func (e *Engine) runProbe(r *Run) {
	s := e.def(r).States[r.State].Step
	k, err := kekkai.Load(e.Home, r.Kekkai)
	if err != nil {
		e.block(r, "stuck", "probe in "+r.State+" could not start", err.Error(), []string{"retry", "stop"})
		return
	}
	dir := filepath.Join(e.runDir(r), r.State+".probe")
	os.MkdirAll(dir, 0o700)
	spec, err := e.scriptSpec(r, s, s.Probe, k, dir)
	if err != nil {
		e.block(r, "stuck", "probe in "+r.State+" could not start", err.Error(), []string{"retry", "stop"})
		return
	}
	spec.LogDir = "" // probes repeat; keep only the last result in the event
	tok := token()
	r.Step.Token, r.Step.probing = tok, true
	id := r.ID
	if _, err := startProc(spec, tok, func(res procResult) { e.probeExited(id, tok, res) }); err != nil {
		r.Step.probing = false
		e.block(r, "stuck", "probe in "+r.State+" could not start", err.Error(), []string{"retry", "stop"})
	}
}

func (e *Engine) probeExited(id, tok string, res procResult) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r := e.runs[id]
	if e.closed || r == nil || r.Step == nil || r.Step.Token != tok || r.Step.Phase != "waiting" {
		return
	}
	r.Step.probing, r.Step.Token = false, ""
	out, vars := scriptOutcome(res)
	s := e.def(r).States[r.State].Step
	every := s.Every
	if out == "pending" {
		r.Step.errors = 0
		if m, _ := lastJSON(res.Stdout); m != nil { // {"outcome": "pending", "say": "..."}: a status line, posted when it changes
			if say, _ := m["say"].(string); strings.TrimSpace(say) != "" && say != r.Step.lastSay {
				r.Step.lastSay = say
				e.event(r, "run.say", map[string]any{"state": r.State, "outcome": out, "text": strings.TrimSpace(say)})
			}
		}
		if len(r.SystemQueue) > 0 { // sent during this probe: the wait may be hours, so it applies now
			if next := e.applySystemQueue(r, r.State); next != r.State {
				e.stopTimer("run:" + r.ID)
				e.enter(r, next)
				return
			}
			e.saveRun(r, "system instruction applied")
		}
		t := now().Add(every)
		r.Step.probeAt = &t
		e.scheduleRun(r) // no memento write: waiting costs nothing
		return
	}
	e.event(r, "probe.result", map[string]any{"state": r.State, "outcome": out, "exit": res.Exit, "stderr": tail(res.Stderr, 400)})
	if _, ok := e.def(r).States[r.State].Outcomes[out]; !ok || out == "timeout" || out == "timer" || out == "signal" {
		limit := s.MaxErrors
		if limit == 0 {
			limit = 3
		}
		if out == "error" { // likely transient (a 5xx, a DNS blip): back off, and block only when it keeps failing
			r.Step.errors++
			every <<= min(r.Step.errors-1, 3)
		}
		e.event(r, "probe.unmapped", map[string]any{"state": r.State, "outcome": out, "errors": r.Step.errors})
		t := now().Add(every)
		r.Step.probeAt = &t
		if out == "error" && r.Step.errors >= limit {
			e.block(r, "stuck", fmt.Sprintf("%s: the probe failed (exit %d)", r.State, res.Exit),
				failureBody(res, filepath.Join(e.runDir(r), r.State+".probe")), []string{"retry", "stop"})
			return
		}
		e.scheduleRun(r)
		return
	}
	e.stopTimer("run:" + r.ID)
	r.Step.Phase = "done"
	e.outcome(r, out, vars)
}

// --- heartbeats --------------------------------------------------------------------

// armHeartbeat (re)starts the heartbeat of r's current step, or stops the last
// one. It beats every HeartbeatEvery while r stays in this visit of the state
// with that step in progress (a blocker or a pause ends it); a restart arms it
// again from recovery.
func (e *Engine) armHeartbeat(r *Run) {
	e.stopTimer("hb:" + r.ID)
	st := e.def(r).States[r.State]
	if st == nil {
		return
	}
	s := st.Step
	if s.Heartbeat == "" || r.Step == nil || r.Step.Kind != s.Kind {
		return
	}
	id, state, visit := r.ID, r.State, r.Visits[r.State]
	var beat func()
	beat = func() {
		r := e.runs[id]
		if r == nil || Terminal(r.Status) || r.State != state || r.Visits[state] != visit || r.Step == nil || r.Step.Kind != s.Kind {
			return
		}
		e.setTimer("hb:"+id, now().Add(s.HeartbeatEvery), beat)
		if !e.held(r) && !r.Step.beating {
			e.runHeartbeat(r, s)
		}
	}
	e.setTimer("hb:"+id, now().Add(s.HeartbeatEvery), beat)
}

// runHeartbeat runs the heartbeat script; its trimmed stdout, when not empty,
// becomes a run.say line. A failure is logged and never touches the step.
func (e *Engine) runHeartbeat(r *Run, s *kamui.Step) {
	state, visit := r.State, r.Visits[r.State]
	fail := func(data map[string]any) {
		data["state"] = state
		e.event(r, "heartbeat.failed", data)
	}
	k, err := kekkai.Load(e.Home, r.Kekkai)
	if err != nil {
		fail(map[string]any{"error": err.Error()})
		return
	}
	dir := filepath.Join(e.runDir(r), state+".heartbeat")
	os.MkdirAll(dir, 0o700)
	spec, err := e.scriptSpec(r, s, s.Heartbeat, k, dir)
	if err != nil {
		fail(map[string]any{"error": err.Error()})
		return
	}
	// ponytail: fixed 2m cap so a hung status script cannot pile up; make it a heartbeat() arg if one needs longer
	spec.LogDir, spec.Timeout = "", 2*time.Minute
	r.Step.beating = true
	id := r.ID
	_, err = startProc(spec, "", func(res procResult) {
		e.mu.Lock()
		defer e.mu.Unlock()
		r := e.runs[id]
		if e.closed || r == nil || Terminal(r.Status) || r.State != state || r.Visits[state] != visit || r.Step == nil {
			return // the step ended meanwhile: its status is stale
		}
		r.Step.beating = false
		if res.TimedOut || res.Exit != 0 || res.Err != nil {
			fail(map[string]any{"exit": res.Exit, "timed_out": res.TimedOut, "stderr": tail(res.Stderr, 400)})
			return
		}
		if text := strings.TrimSpace(string(res.Stdout)); text != "" {
			e.event(r, "run.say", map[string]any{"state": state, "text": text, "heartbeat": true})
		}
	})
	if err != nil {
		r.Step.beating = false
		fail(map[string]any{"error": err.Error()})
	}
}

// Publish delivers a signal to every run waiting on it in the kekkai.
func (e *Engine) Publish(k, typ, key string, data map[string]any) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Bus.Append(k, "", "signal.published", map[string]any{"type": typ, "key": key, "data": data})
	var ids []string
	for id, r := range e.runs {
		st := r.Step
		if r.Kekkai != k || Terminal(r.Status) || st == nil || st.Kind != kamui.KWait || st.Phase != "waiting" ||
			st.Signal != typ || (st.Key != "" && st.Key != key) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := e.runs[id]
		e.stopTimer("run:" + id)
		r.Step.Phase = "done"
		e.event(r, "signal.delivered", map[string]any{"type": typ, "key": key})
		e.outcome(r, "signal", data)
	}
	return ids
}

// --- cron triggers ----------------------------------------------------------------

// ReloadTriggers rescans every kekkai for cron triggers.
func (e *Engine) ReloadTriggers() map[string]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.scheduleTriggersLocked()
}

func (e *Engine) scheduleTriggersLocked() map[string]string {
	for key := range e.timers {
		if strings.HasPrefix(key, "trigger:") {
			e.stopTimer(key)
		}
	}
	scheduled := map[string]string{}
	names, _ := kekkai.List(e.Home)
	for _, kn := range names {
		k, err := kekkai.Load(e.Home, kn)
		if err != nil {
			continue
		}
		files, _ := filepath.Glob(filepath.Join(k.Dir, "workflows", "*.star"))
		for _, f := range files {
			wf := strings.TrimSuffix(filepath.Base(f), ".star")
			w, err := kamui.Load(kamui.WorkflowPath(wf), kamui.DirReader(k.Dir))
			if err != nil || len(w.Triggers) == 0 {
				continue
			}
			if p := kamui.Validate(w, k); len(p) > 0 {
				e.Bus.Append(kn, "", "trigger.skipped", map[string]any{"workflow": wf, "problems": p})
				continue
			}
			for i, tr := range w.Triggers {
				id := fmt.Sprintf("%s/%s#%d", kn, wf, i)
				next := tr.Sched.Next(time.Now())
				if next.IsZero() {
					continue
				}
				scheduled[id] = next.UTC().Format(time.RFC3339)
				e.armTrigger(id, kn, wf, tr, next)
			}
		}
	}
	return scheduled
}

// armTrigger fires at next, starts a run, and re-arms. Fires missed while the
// daemon was down, or while the kekkai is paused, are skipped, not caught up.
func (e *Engine) armTrigger(id, k, wf string, tr kamui.Trigger, next time.Time) {
	e.setTimer("trigger:"+id, next, func() {
		if p := e.pauses[k]; p != nil && !slices.Contains(p.Except, wf) {
			e.Bus.Append(k, "", "trigger.skipped", map[string]any{"trigger": id, "cron": tr.Cron, "why": "kekkai paused"})
			if n := tr.Sched.Next(time.Now()); !n.IsZero() {
				e.armTrigger(id, k, wf, tr, n)
			}
			return
		}
		in := map[string]any{}
		for key, v := range tr.Input {
			in[key] = v
		}
		r, err := e.createRun(k, wf, "", in, nil, "", 0)
		fired := now()
		if err != nil {
			e.Bus.Append(k, "", "trigger.failed", map[string]any{"trigger": id, "error": err.Error()})
		} else {
			e.event(r, "trigger.fired", map[string]any{"trigger": id, "cron": tr.Cron})
			e.begin(r)
		}
		e.triggers[id] = fired
		if err := e.Mem.SaveTrigger(id, fired.Format(time.RFC3339Nano)); err != nil {
			e.Bus.Append(k, "", "persist.failed", map[string]any{"what": "trigger", "error": err.Error()})
		}
		if n := tr.Sched.Next(time.Now()); !n.IsZero() {
			e.armTrigger(id, k, wf, tr, n)
		}
	})
}
