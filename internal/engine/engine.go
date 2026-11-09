// Package engine is the Kami daemon core: it runs workflow instances through
// their states, executes workers and scripts, parks decisions as contracts,
// and recovers everything from memento after a restart.
package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"kami/internal/eventbus"
	"kami/internal/kamui"
	"kami/internal/kekkai"
	"kami/internal/state"
)

// Engine owns every run and contract. All mutation happens under mu; slow
// work (processes) runs in goroutines and reports back through methods that
// take mu again and check the step token, so a stale report is dropped.
type Engine struct {
	Home    string
	Mem     *state.Memento
	Bus     *eventbus.Bus
	KamiBin string // put on workers' PATH so they can call `kami`
	Sock    string // socket path handed to workers

	mu        sync.Mutex
	runs      map[string]*Run
	contracts map[string]*Contract
	defs      map[string]*kamui.Workflow
	auras     map[string][]*Aura // run id -> snapshots, oldest first
	timers    map[string]*time.Timer
	procs     map[string]*proc // run id -> running worker/script
	resolvers map[string]*proc // contract id -> running resolver
	triggers  map[string]time.Time
	pauses    map[string]*Pause // kekkai -> its pause
	closed    bool
}

// ErrNotFound is returned for unknown ids.
var ErrNotFound = errors.New("not found")

// UserError is a request the caller can fix; the API maps it to 4xx.
type UserError struct {
	Code string
	Msg  string
}

func (e *UserError) Error() string { return e.Msg }

func uerr(code, f string, a ...any) error { return &UserError{Code: code, Msg: fmt.Sprintf(f, a...)} }

// New builds an engine; call Boot before serving.
func New(home string, mem *state.Memento, bus *eventbus.Bus, kamiBin, sock string) *Engine {
	return &Engine{Home: home, Mem: mem, Bus: bus, KamiBin: kamiBin, Sock: sock,
		runs: map[string]*Run{}, contracts: map[string]*Contract{}, defs: map[string]*kamui.Workflow{},
		auras: map[string][]*Aura{}, timers: map[string]*time.Timer{}, procs: map[string]*proc{},
		resolvers: map[string]*proc{}, triggers: map[string]time.Time{}}
}

func now() time.Time { return time.Now().UTC() }

// event appends to the log; a log failure never stops the engine.
func (e *Engine) event(r *Run, typ string, data map[string]any) {
	kek, id := "", ""
	if r != nil {
		kek, id = r.Kekkai, r.ID
	}
	if _, err := e.Bus.Append(kek, id, typ, data); err != nil {
		fmt.Fprintf(os.Stderr, "kami: event log: %v\n", err)
	}
}

// saveRun writes the whole run to memento. A failed write is logged; the next
// successful write carries the full document, so nothing is lost for good.
func (e *Engine) saveRun(r *Run, why string) {
	r.Updated = now()
	if err := e.Mem.SaveRun(r.ID, r.Kekkai, r.Workflow, r.Status, r.Parent, r.Updated.Format(time.RFC3339Nano), r,
		fmt.Sprintf("kami: run %s %s %s", r.ID, r.State, why)); err != nil {
		e.event(r, "persist.failed", map[string]any{"what": "run", "error": err.Error()})
	}
}

func (e *Engine) saveContract(c *Contract, why string) {
	c.Updated = now()
	if err := e.Mem.SaveContract(c.ID, c.RunID, c.Kekkai, c.Status, c.Updated.Format(time.RFC3339Nano), c,
		fmt.Sprintf("kami: contract %s %s", c.ID, why)); err != nil {
		e.event(e.runs[c.RunID], "persist.failed", map[string]any{"what": "contract", "id": c.ID, "error": err.Error()})
	}
}

func (e *Engine) def(r *Run) *kamui.Workflow { return e.defs[r.DefHash] }

// --- boot and recovery -------------------------------------------------------

// Boot loads durable state and resumes every run that was in flight.
func (e *Engine) Boot() error {
	if err := e.linkBin(); err != nil {
		return fmt.Errorf("link kami into %s/bin: %w", e.Home, err)
	}
	if err := e.Mem.Ensure(); err != nil {
		return fmt.Errorf("memento: %w", err)
	}
	defs, err := e.Mem.Defs()
	if err != nil {
		return err
	}
	for _, d := range defs {
		w, err := kamui.Load(d.Entry, kamui.BundleReader(d.Files))
		if err != nil {
			fmt.Fprintf(os.Stderr, "kami: pinned workflow %s/%s %s no longer loads: %v\n", d.Kekkai, d.Workflow, d.Hash[:12], err)
			continue
		}
		e.defs[d.Hash] = w
	}
	runDocs, err := e.Mem.Docs("runs", "")
	if err != nil {
		return err
	}
	for _, raw := range runDocs {
		r := &Run{}
		if err := json.Unmarshal(raw, r); err != nil {
			return fmt.Errorf("run doc: %w", err)
		}
		e.runs[r.ID] = r
	}
	cDocs, err := e.Mem.Docs("contracts", "")
	if err != nil {
		return err
	}
	for _, raw := range cDocs {
		c := &Contract{}
		if err := json.Unmarshal(raw, c); err != nil {
			return fmt.Errorf("contract doc: %w", err)
		}
		e.contracts[c.ID] = c
	}
	aDocs, err := e.Mem.Docs("auras", "")
	if err != nil {
		return err
	}
	for _, raw := range aDocs {
		a := &Aura{}
		if err := json.Unmarshal(raw, a); err == nil {
			e.auras[a.RunID] = append(e.auras[a.RunID], a)
		}
	}
	for _, l := range e.auras {
		sort.Slice(l, func(i, j int) bool { return l[i].Created.Before(l[j].Created) })
	}
	if tr, err := e.Mem.Triggers(); err == nil {
		for id, ts := range tr {
			if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
				e.triggers[id] = t
			}
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.loadPauses()
	ids := make([]string, 0, len(e.runs))
	for id, r := range e.runs {
		if !Terminal(r.Status) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		e.recover(e.runs[id])
	}
	e.scheduleTriggersLocked()
	e.event(nil, "daemon.started", map[string]any{"runs_resumed": len(ids)})
	return nil
}

func (e *Engine) recover(r *Run) {
	if e.def(r) == nil {
		e.finish(r, StFailed, "pinned workflow "+r.DefHash+" is missing or no longer loads")
		return
	}
	st := r.Step
	if st == nil {
		e.enter(r, r.State)
		return
	}
	e.event(r, "run.recovered", map[string]any{"state": r.State, "kind": st.Kind, "phase": st.Phase})
	e.armHeartbeat(r)
	switch st.Kind {
	case kamui.KAgent:
		if st.Phase == "sleeping" {
			if c := e.contracts[st.Contract]; c != nil && c.Status == "resolved" {
				e.startAgent(r, &wake{resolution: c})
				return
			}
			e.reapplyContract(st.Contract)
			return
		}
		if p := st.Pending; p != nil { // it had already reported: finish the step, do not redo it
			st.Pending, st.Phase, st.Token = nil, "done", ""
			e.outcome(r, p.Outcome, p.Vars)
			return
		}
		e.startAgent(r, &wake{note: "The daemon restarted while the previous attempt was running. Check what it already did before repeating any action."})
	case kamui.KScript:
		e.startScript(r)
	case kamui.KWait:
		e.scheduleRun(r)
	case kamui.KSpawn:
		e.checkChildren(r)
	case "blocked":
		e.reapplyContract(st.Contract)
	case "paused":
		e.enter(r, r.State) // parks again while the kekkai is still paused
	default:
		if e.def(r).States[r.State] != nil && e.def(r).States[r.State].Step.IsContract() {
			e.reapplyContract(st.Contract)
			return
		}
		e.enter(r, r.State)
	}
}

// reapplyContract re-arms an open contract, or finishes applying one that was
// resolved just before a crash.
func (e *Engine) reapplyContract(id string) {
	c := e.contracts[id]
	if c == nil {
		return
	}
	if c.Status == "resolved" {
		e.applyResolution(c)
		return
	}
	if c.Status == "open" {
		e.applyMode(c, false)
	}
}

// --- starting runs ------------------------------------------------------------

// StartRun starts workflow wf in kekkai k. entry may be empty for the start state.
func (e *Engine) StartRun(k, wf, entry string, inputs map[string]any, afk *kamui.AFK) (*Run, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, uerr("UNAVAILABLE", "daemon is shutting down")
	}
	r, err := e.createRun(k, wf, entry, inputs, afk, "", 0)
	if err != nil {
		return nil, err
	}
	e.begin(r)
	return r, nil
}

// LoadWorkflow loads and validates a workflow from a kekkai on disk.
func LoadWorkflow(home, k, wf string) (*kekkai.Kekkai, *kamui.Workflow, error) {
	kk, err := kekkai.Load(home, k)
	if err != nil {
		return nil, nil, uerr("NOT_FOUND", "%v", err)
	}
	w, err := kamui.Load(kamui.WorkflowPath(wf), kamui.DirReader(kk.Dir))
	if err != nil {
		if os.IsNotExist(errors.Unwrap(err)) || errors.Is(err, os.ErrNotExist) {
			return nil, nil, uerr("NOT_FOUND", "workflow %q not found in kekkai %s", wf, k)
		}
		return nil, nil, uerr("INVALID", "%v", err)
	}
	if p := kamui.Validate(w, kk); len(p) > 0 {
		return nil, nil, uerr("INVALID", "workflow %s/%s is invalid:\n  %s", k, wf, strings.Join(p, "\n  "))
	}
	return kk, w, nil
}

func (e *Engine) createRun(k, wf, entry string, inputs map[string]any, afk *kamui.AFK, parent string, idx int) (*Run, error) {
	_, w, err := LoadWorkflow(e.Home, k, wf)
	if err != nil {
		return nil, err
	}
	start := w.Start
	if entry != "" && entry != "start" {
		en, ok := w.Entries[entry]
		if !ok {
			names := make([]string, 0, len(w.Entries))
			for n := range w.Entries {
				names = append(names, n)
			}
			sort.Strings(names)
			return nil, uerr("NOT_FOUND", "workflow %s has no entry %q (entries: %s)", wf, entry, strings.Join(names, ", "))
		}
		for _, req := range en.Require {
			if _, ok := inputs[req]; !ok {
				return nil, uerr("INVALID", "entry %s:%s requires input %q", wf, entry, req)
			}
		}
		start = en.State
	} else {
		entry = "start"
	}
	if inputs == nil {
		inputs = map[string]any{}
	}
	if err := checkInputs(w, inputs, entry != "start"); err != nil {
		return nil, err
	}
	if _, ok := e.defs[w.Hash]; !ok {
		if err := e.Mem.SaveDef(w.Hash, k, wf, w.Entry, w.Files); err != nil {
			return nil, fmt.Errorf("pin workflow: %w", err)
		}
		e.defs[w.Hash] = w
	}
	r := &Run{ID: NewID(), Workflow: wf, DefHash: w.Hash, Kekkai: k, Parent: parent, ParentIndex: idx,
		Entry: entry, State: start, Status: StRunning, Vars: inputs, Visits: map[string]int{}, AFK: afk,
		Created: now()}
	e.runs[r.ID] = r
	e.saveRun(r, "created")
	e.event(r, "run.started", map[string]any{"workflow": wf, "entry": entry, "parent": parent, "def_hash": w.Hash})
	return r, nil
}

func checkInputs(w *kamui.Workflow, in map[string]any, viaEntry bool) error {
	for name, typ := range w.Inputs {
		v, ok := in[name]
		optional := strings.HasSuffix(typ, "?")
		if !ok || v == nil {
			if !optional && !viaEntry {
				return uerr("INVALID", "input %q (%s) is required", name, typ)
			}
			continue
		}
		if err := checkType(strings.TrimSuffix(typ, "?"), v); err != nil {
			return uerr("INVALID", "input %q: %v", name, err)
		}
	}
	return nil
}

func checkType(typ string, v any) error {
	ok := true
	switch {
	case typ == "string":
		_, ok = v.(string)
	case typ == "dict":
		_, ok = v.(map[string]any)
	case typ == "list":
		_, ok = v.([]any)
	case typ == "bool":
		_, ok = v.(bool)
	case typ == "int" || typ == "number":
		_, ok = v.(float64)
	case strings.HasPrefix(typ, "enum:"):
		s, _ := v.(string)
		ok = false
		for _, o := range strings.Split(strings.TrimPrefix(typ, "enum:"), ",") {
			if s == o {
				ok = true
			}
		}
	}
	if !ok {
		return fmt.Errorf("want %s, got %v", typ, v)
	}
	return nil
}

func (e *Engine) begin(r *Run) { e.enter(r, r.State) }

// --- transitions ----------------------------------------------------------------

// enter moves r into state and starts its step.
func (e *Engine) enter(r *Run, name string) {
	w := e.def(r)
	st := w.States[name]
	if st == nil {
		e.finish(r, StFailed, fmt.Sprintf("state %q does not exist", name))
		return
	}
	if e.held(r) {
		e.park(r, name)
		return
	}
	r.Visits[name]++
	r.State = name
	if limit := w.MaxVisitsFor(name); r.Visits[name] > limit {
		e.block(r, "budget", fmt.Sprintf("Loop budget: %s entered %d times (max %d)", name, r.Visits[name], limit),
			fmt.Sprintf("Run %s of %s keeps returning to state %q. Continue resets its visit count and runs it again; stop fails the run.", r.ID, r.Workflow, name),
			[]string{"continue", "stop"})
		return
	}
	s := st.Step
	r.Step = &StepState{Kind: s.Kind}
	e.event(r, "state.entered", map[string]any{"state": name, "kind": s.Kind, "visit": r.Visits[name]})
	e.armHeartbeat(r)
	switch {
	case s.Kind == kamui.KAgent:
		e.startAgent(r, nil)
	case s.Kind == kamui.KScript:
		e.startScript(r)
	case s.Kind == kamui.KWait:
		e.armWait(r, s)
	case s.Kind == kamui.KSpawn:
		e.startChildren(r, s)
	case s.Kind == kamui.KChoose:
		out, err := kamui.Call(s.Fn, e.guardVars(r))
		next, _ := out.(string)
		if err == nil && w.States[next] == nil {
			err = fmt.Errorf("returned %v, which is not a state", out)
		}
		if err != nil {
			e.block(r, "stuck", fmt.Sprintf("choose in state %s failed", name), err.Error(), []string{"retry", "stop"})
			return
		}
		e.event(r, "state.chosen", map[string]any{"state": name, "next": next})
		e.enter(r, next)
	case s.IsTerminal():
		status := map[string]string{kamui.KDone: StDone, kamui.KFail: StFailed, kamui.KCancel: StCancelled}[s.Kind]
		report, _ := kamui.Render(s.Report, r.Vars)
		e.finish(r, status, report)
	case s.IsContract():
		e.openStateContract(r, st)
	}
}

// guardVars is the read-only view functions get: vars plus visits.
func (e *Engine) guardVars(r *Run) map[string]any {
	v := make(map[string]any, len(r.Vars)+1)
	for k, x := range r.Vars {
		v[k] = x
	}
	vis := map[string]any{}
	for k, n := range r.Visits {
		vis[k] = float64(n)
	}
	v["visits"] = vis
	return v
}

// outcome applies a step's outcome: merge vars, follow the edge.
func (e *Engine) outcome(r *Run, out string, vars map[string]any) {
	for k, v := range vars {
		if k == "visits" {
			continue
		}
		// kami_* (the conversation a run speaks for) come from its inputs only: never from a
		// worker, a signal's data, a script or a contract's answer data
		if strings.HasPrefix(k, "kami_") {
			e.event(r, "vars.refused", map[string]any{"state": r.State, "var": k})
			continue
		}
		r.Vars[k] = v
	}
	next, err := e.def(r).Next(r.State, out)
	if err != nil {
		e.event(r, "step.outcome", map[string]any{"state": r.State, "outcome": out, "error": err.Error()})
		e.block(r, "stuck", fmt.Sprintf("%s returned outcome %q, which has no transition", r.State, out), err.Error(), []string{"retry", "stop"})
		return
	}
	e.event(r, "step.outcome", map[string]any{"state": r.State, "outcome": out, "next": next})
	if say := e.def(r).States[r.State].Step.Say; say != nil {
		tmpl, ok := say[out]
		if !ok {
			tmpl, ok = say[""]
		}
		if ok {
			text, _ := kamui.Render(tmpl, r.Vars)
			e.event(r, "run.say", map[string]any{"state": r.State, "outcome": out, "text": text})
		}
	}
	e.enter(r, e.applySystemQueue(r, next))
}

// finish ends a run, closes its contracts, and tells its parent.
func (e *Engine) finish(r *Run, status, report string) {
	if Terminal(r.Status) {
		return
	}
	e.stopTimer("run:" + r.ID)
	e.stopTimer("hb:" + r.ID)
	if p := e.procs[r.ID]; p != nil {
		p.kill()
	}
	e.cancelContracts(r, "run finished", "")
	for _, q := range r.SystemQueue { // the step never reached an outcome: say so, nothing is silently dropped
		e.event(r, "system.dropped", map[string]any{"text": q.Text, "state": q.State, "vars": q.Vars, "status": status})
	}
	r.SystemQueue = nil
	r.Status, r.Report, r.Outcome = status, report, r.State
	if r.Step != nil {
		r.Step.Phase, r.Step.Token = "done", ""
	}
	e.saveRun(r, status)
	e.event(r, "run.finished", map[string]any{"status": status, "outcome": r.Outcome, "report": report})
	if r.Parent != "" {
		if p := e.runs[r.Parent]; p != nil && !Terminal(p.Status) {
			e.checkChildren(p)
		}
	}
}

// cancelContracts withdraws r's open contracts, except keep.
func (e *Engine) cancelContracts(r *Run, why, keep string) {
	for _, c := range e.contracts {
		if c.RunID == r.ID && c.Status == "open" && c.ID != keep {
			c.Status = "cancelled"
			e.stopTimer("contract:" + c.ID)
			e.saveContract(c, "cancelled: "+why)
			e.event(r, "contract.cancelled", map[string]any{"id": c.ID})
		}
	}
}

// Upgrade moves a parked run onto its workflow's current definition and enters
// state (its current one when empty): runs stay pinned to the definition they
// started with, so a fix to a workflow otherwise only reaches new runs. Its open
// contracts are withdrawn, except one the state asks again unchanged (openStateContract
// keeps it). A run with a process or children still running is refused.
func (e *Engine) Upgrade(id, state string, vars map[string]any) (*Run, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, err := e.findRun(id)
	if err != nil {
		return nil, err
	}
	return r, e.upgrade(r, state, vars)
}

// busy reports a run with a process or children still running.
func (e *Engine) busy(r *Run) bool {
	return e.procs[r.ID] != nil || (r.Step != nil && (r.Step.probing || r.Step.Kind == kamui.KSpawn ||
		(r.Step.Kind == kamui.KAgent && r.Step.Phase != "done")))
}

func (e *Engine) upgrade(r *Run, state string, vars map[string]any) error {
	if Terminal(r.Status) {
		return uerr("CONFLICT", "run %s is already %s", r.ID, r.Status)
	}
	if e.busy(r) {
		return uerr("CONFLICT", "run %s is busy in %s: upgrade it while it waits", r.ID, r.State)
	}
	_, w, err := LoadWorkflow(e.Home, r.Kekkai, r.Workflow)
	if err != nil {
		return err
	}
	if state == "" {
		state = r.State
	}
	if w.States[state] == nil {
		return uerr("INVALID", "the current %s has no state %q", r.Workflow, state)
	}
	if _, ok := e.defs[w.Hash]; !ok {
		if err := e.Mem.SaveDef(w.Hash, r.Kekkai, r.Workflow, w.Entry, w.Files); err != nil {
			return fmt.Errorf("pin workflow: %w", err)
		}
		e.defs[w.Hash] = w
	}
	e.stopTimer("run:" + r.ID)
	e.event(r, "run.upgraded", map[string]any{"from": r.DefHash, "to": w.Hash, "state": state})
	for k, v := range vars { // kami_* stay what the run's inputs made them, as in outcome
		if k != "visits" && !strings.HasPrefix(k, "kami_") {
			r.Vars[k] = v
		}
	}
	r.DefHash, r.Step, r.Status = w.Hash, nil, StRunning
	e.enter(r, state)
	keep := ""
	if r.Step != nil {
		keep = r.Step.Contract
	}
	e.cancelContracts(r, "run upgraded", keep)
	return nil
}

// block parks the run on an engine contract (loop budget, a step that cannot
// continue). Nothing is ever silently dropped.
func (e *Engine) block(r *Run, purpose, title, body string, options []string) {
	c := &Contract{ID: NewID(), RunID: r.ID, Step: r.State, Kekkai: r.Kekkai, Origin: "engine", Kind: kamui.KBlock,
		Title: title, Body: body, Options: options, Status: "open", Created: now()}
	e.contracts[c.ID] = c
	e.saveContract(c, "opened")
	r.Step = &StepState{Kind: "blocked", Phase: "waiting", Contract: c.ID, Purpose: purpose}
	r.Status = StWaiting
	e.saveRun(r, "blocked")
	e.event(r, "contract.opened", contractEvent(c))
}

// --- spawn ------------------------------------------------------------------------

func (e *Engine) startChildren(r *Run, s *kamui.Step) {
	var items any
	var err error
	if s.ForEachFn != nil {
		items, err = kamui.Call(s.ForEachFn, e.guardVars(r))
	} else {
		var ok bool
		if items, ok = kamui.Lookup(r.Vars, s.ForEach); !ok {
			err = fmt.Errorf("var %q is not set", s.ForEach)
		}
	}
	list, ok := items.([]any)
	if err == nil && !ok {
		err = fmt.Errorf("for_each gave %T, want a list", items)
	}
	if err != nil {
		e.block(r, "stuck", "spawn in state "+r.State+" could not list its items", err.Error(), []string{"retry", "stop"})
		return
	}
	var kids []*Run
	for i, it := range list {
		in := map[string]any{"item": it}
		if s.InputFn != nil {
			out, err := kamui.Call(s.InputFn, it, e.guardVars(r))
			m, ok := out.(map[string]any)
			if err == nil && !ok {
				err = fmt.Errorf("input(...) gave %T, want a dict", out)
			}
			if err != nil {
				e.abortChildren(kids)
				e.block(r, "stuck", "spawn in state "+r.State+": input function failed", err.Error(), []string{"retry", "stop"})
				return
			}
			in = m
		}
		child, err := e.createRun(r.Kekkai, s.Workflow, "", in, r.AFK, r.ID, i)
		if err != nil {
			e.abortChildren(kids)
			e.block(r, "stuck", "spawn in state "+r.State+" could not start "+s.Workflow, err.Error(), []string{"retry", "stop"})
			return
		}
		kids = append(kids, child)
		r.Step.Children = append(r.Step.Children, child.ID)
	}
	r.Step.Phase, r.Status = "waiting", StWaiting
	e.saveRun(r, "spawned")
	e.event(r, "spawn.started", map[string]any{"children": r.Step.Children})
	if len(kids) == 0 {
		e.checkChildren(r)
		return
	}
	for _, k := range kids {
		e.begin(k)
	}
}

func (e *Engine) abortChildren(kids []*Run) {
	for _, k := range kids {
		e.finish(k, StCancelled, "sibling failed to start")
	}
}

// checkChildren completes a spawn once every child is terminal.
func (e *Engine) checkChildren(r *Run) {
	if r.Step == nil || r.Step.Kind != kamui.KSpawn || r.Step.Phase != "waiting" {
		return
	}
	out := make([]any, 0, len(r.Step.Children))
	failed := false
	for _, id := range r.Step.Children {
		c := e.runs[id]
		if c == nil {
			failed = true
			out = append(out, map[string]any{"run_id": id, "status": StFailed, "outcome": "", "report": "child run missing"})
			continue
		}
		if !Terminal(c.Status) {
			return
		}
		if c.Status != StDone {
			failed = true
		}
		out = append(out, map[string]any{"run_id": c.ID, "status": c.Status, "outcome": c.Outcome, "report": c.Report, "vars": c.Vars})
	}
	result := "all_done"
	if failed {
		result = "any_failed"
	}
	r.Step.Phase = "done"
	e.outcome(r, result, map[string]any{"children": out})
}

// --- cancel -------------------------------------------------------------------------

// Cancel ends a run and its children.
func (e *Engine) Cancel(id string) (*Run, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, err := e.findRun(id)
	if err != nil {
		return nil, err
	}
	if Terminal(r.Status) {
		return nil, uerr("CONFLICT", "run %s is already %s", r.ID, r.Status)
	}
	e.cancelTree(r)
	return r, nil
}

func (e *Engine) cancelTree(r *Run) {
	if r.Step != nil {
		r.Step.Phase = "cancelling" // keeps checkChildren from completing the spawn mid-cancel
		for _, cid := range r.Step.Children {
			if c := e.runs[cid]; c != nil && !Terminal(c.Status) {
				e.cancelTree(c)
			}
		}
	}
	e.finish(r, StCancelled, "cancelled")
}

// --- lookups ---------------------------------------------------------------------

func (e *Engine) findRun(id string) (*Run, error) {
	if r := e.runs[id]; r != nil {
		return r, nil
	}
	var match *Run
	for rid, r := range e.runs {
		if strings.HasPrefix(rid, id) && id != "" {
			if match != nil {
				return nil, uerr("AMBIGUOUS", "run id prefix %q matches more than one run", id)
			}
			match = r
		}
	}
	if match == nil {
		return nil, uerr("NOT_FOUND", "no run %q", id)
	}
	return match, nil
}

func (e *Engine) findContract(id string) (*Contract, error) {
	if c := e.contracts[id]; c != nil {
		return c, nil
	}
	var match *Contract
	for cid, c := range e.contracts {
		if strings.HasPrefix(cid, id) && id != "" {
			if match != nil {
				return nil, uerr("AMBIGUOUS", "contract id prefix %q matches more than one contract", id)
			}
			match = c
		}
	}
	if match == nil {
		return nil, uerr("NOT_FOUND", "no contract %q", id)
	}
	return match, nil
}

// --- timers ----------------------------------------------------------------------

// setTimer runs f under the lock once the wall clock reaches at. Go timers
// count monotonic time, which stops while macOS sleeps, so a timer armed for
// 09:30 the evening before would fire hours late. Each timer therefore waits
// at most a minute and re-checks the wall clock: after a sleep it fires
// within a minute of waking.
func (e *Engine) setTimer(key string, at time.Time, f func()) {
	e.stopTimer(key)
	at = at.Round(0) // wall clock only
	d := time.Until(at)
	if d < 0 {
		d = 0
	}
	if d > time.Minute {
		d = time.Minute
	}
	var t *time.Timer
	t = time.AfterFunc(d, func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		if e.closed || e.timers[key] != t {
			return // stopped or replaced meanwhile
		}
		if time.Now().Round(0).Before(at) {
			e.setTimer(key, at, f)
			return
		}
		delete(e.timers, key)
		f()
	})
	e.timers[key] = t
}

func (e *Engine) stopTimer(key string) {
	if t := e.timers[key]; t != nil {
		t.Stop()
		delete(e.timers, key)
	}
}

// Close stops timers and kills running processes. Runs stay as they are in
// memento and resume on the next boot.
func (e *Engine) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	for k, t := range e.timers {
		t.Stop()
		delete(e.timers, k)
	}
	for _, p := range e.procs {
		p.kill()
	}
	for _, p := range e.resolvers {
		p.kill()
	}
}

// runDir is $KAMI_HOME/runs/<id>.
func (e *Engine) runDir(r *Run) string { return filepath.Join(e.Home, "runs", r.ID) }
