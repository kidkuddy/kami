package engine

import (
	"strings"

	"kami/internal/kamui"
)

// BySystem is the resolver name on a contract answered by a system instruction.
const BySystem = "system"

// System is an instruction from outside to a run: it continues where it is, with a log and
// nobody pressing anything. With answer it answers the open contract; with state or vars it
// moves onto the latest workflow (as Upgrade); with neither it runs a waiting probe now. Text
// is the reason, kept as a system.instruction event and a run.say in the run's thread.
// A run busy with a process or children takes state and vars when its current step ends:
// they queue, stay on its pinned workflow, and override where the step's outcome would go.
// A locked contract stays the owner's: answering it, or the upgrade that would cancel it, is refused,
// and so is a state only reachable by passing through a human-only step.
func (e *Engine) System(id, text, state, answer string, vars map[string]any) (*Run, error) {
	if strings.TrimSpace(text) == "" {
		return nil, uerr("INVALID", "a system instruction needs its text: it is the log")
	}
	if answer != "" && (state != "" || len(vars) > 0) {
		return nil, uerr("INVALID", "--answer goes alone: the run continues from the answer")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	r, err := e.findRun(id)
	if err != nil {
		return nil, err
	}
	if Terminal(r.Status) {
		return nil, uerr("CONFLICT", "run %s is already %s", r.ID, r.Status)
	}
	var open *Contract
	for _, c := range e.contracts {
		if c.RunID != r.ID || c.Status != "open" {
			continue
		}
		if c.Lock {
			return nil, uerr("FORBIDDEN", "run %s waits on locked contract %s: only the owner can answer it", r.ID, c.ID)
		}
		if r.Step != nil && r.Step.Contract == c.ID {
			open = c
		}
	}
	switch {
	case answer != "":
		if open == nil {
			return nil, uerr("CONFLICT", "run %s has no open contract to answer", r.ID)
		}
		err = e.resolve(open, Resolution{Answer: answer, Note: text, By: BySystem})
	case (state != "" || len(vars) > 0) && e.busy(r):
		return r, e.queueSystem(r, text, state, vars)
	case state != "" || len(vars) > 0:
		if state != "" {
			_, w, lerr := LoadWorkflow(e.Home, r.Kekkai, r.Workflow)
			if lerr != nil {
				return nil, lerr
			}
			if w.States[state] != nil && !w.ReachableWithoutHuman(r.State, state) {
				return nil, uerr("FORBIDDEN", "run %s cannot get from %s to %s without a human-only step: only the owner can move it", r.ID, r.State, state)
			}
		}
		err = e.upgrade(r, state, vars)
	case r.Step != nil && r.Step.Kind == kamui.KWait && r.Step.Phase == "waiting" && !r.Step.probing && e.def(r).States[r.State].Step.Probe != "":
		t := now()
		r.Step.probeAt = &t
		e.scheduleRun(r)
	}
	if err != nil {
		return nil, err
	}
	e.event(r, "system.instruction", map[string]any{"text": text, "state": state, "answer": answer, "vars": vars})
	e.event(r, "run.say", map[string]any{"state": r.State, "text": "🛠 system: " + text})
	return r, nil
}

// queueSystem holds an instruction until r's current step ends. Whatever would refuse it then
// refuses it now, so a queued instruction never fails later, out of sight.
func (e *Engine) queueSystem(r *Run, text, state string, vars map[string]any) error {
	if state != "" {
		w := e.def(r)
		if w.States[state] == nil {
			return uerr("INVALID", "%s has no state %q", r.Workflow, state)
		}
		if !w.ReachableWithoutHuman(r.State, state) {
			return uerr("FORBIDDEN", "run %s cannot get from %s to %s without a human-only step: only the owner can move it", r.ID, r.State, state)
		}
	}
	r.SystemQueue = append(r.SystemQueue, QueuedSystem{Text: text, State: state, Vars: vars})
	e.saveRun(r, "system instruction queued")
	e.event(r, "system.queued", map[string]any{"text": text, "state": state, "vars": vars})
	e.event(r, "run.say", map[string]any{"state": r.State, "text": "🛠 system (after " + r.State + "): " + text})
	return nil
}

// applySystemQueue merges queued vars into r, in order, and returns where r goes next: next,
// unless an instruction named a state (the last one wins).
func (e *Engine) applySystemQueue(r *Run, next string) string {
	for _, q := range r.SystemQueue {
		for k, v := range q.Vars { // kami_* stay what the run's inputs made them, as in outcome
			if k != "visits" && !strings.HasPrefix(k, "kami_") {
				r.Vars[k] = v
			}
		}
		if q.State != "" {
			next = q.State
		}
		e.event(r, "system.instruction", map[string]any{"text": q.Text, "state": q.State, "vars": q.Vars, "queued": true})
	}
	r.SystemQueue = nil
	return next
}
