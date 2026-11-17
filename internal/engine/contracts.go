package engine

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"kami/internal/kamui"
	"kami/internal/kekkai"
)

func contractEvent(c *Contract) map[string]any {
	return map[string]any{"id": c.ID, "kind": c.Kind, "title": c.Title, "origin": c.Origin, "options": c.Options, "lock": c.Lock}
}

// openStateContract parks a decision state (ask, choice, form, blocker, gate, action).
func (e *Engine) openStateContract(r *Run, st *kamui.State) {
	s := st.Step
	w := e.def(r)
	title, _ := kamui.Render(s.Title, r.Vars)
	body := ""
	var err error
	switch {
	case s.BodyFn != nil:
		var out any
		if out, err = kamui.Call(s.BodyFn, e.guardVars(r)); err == nil {
			body = kamui.Stringify(out)
		}
	case s.BodyFile != "":
		body, err = e.readBodyFile(r, s.BodyFile)
	default:
		body, _ = kamui.Render(s.Body, r.Vars)
	}
	if err != nil {
		body = fmt.Sprintf("(body unavailable: %v)", err)
		e.event(r, "contract.body_failed", map[string]any{"state": st.Name, "error": err.Error()})
	}
	c := &Contract{ID: NewID(), RunID: r.ID, Step: st.Name, Kekkai: r.Kekkai, Origin: "state", Kind: s.Kind,
		Title: title, Body: body, Options: st.ContractOptions(), Fields: s.Fields, Default: s.Default,
		Lock: s.HumanOnly(), Mode: s.Mode, Resolver: s.Resolver, Status: "open", Created: now()}
	if c.Mode == nil && !c.Lock {
		c.Mode = w.AFK
	}
	if c.Resolver == "" && w.AFK != nil && !c.Lock {
		c.Resolver = w.AFK.Resolver
	}
	// an upgraded run re-entering its state keeps the question it already asks, unchanged
	for _, old := range e.contracts {
		if old.RunID == r.ID && old.Status == "open" && sameQuestion(old, c) {
			r.Step.Contract, r.Step.Phase, r.Status = old.ID, "waiting", StWaiting
			e.saveRun(r, "still waiting on "+old.ID)
			e.event(r, "contract.kept", map[string]any{"id": old.ID})
			e.applyMode(old, false)
			return
		}
	}
	e.contracts[c.ID] = c
	e.saveContract(c, "opened")
	r.Step.Contract, r.Step.Phase, r.Status = c.ID, "waiting", StWaiting
	e.saveRun(r, "waiting on "+c.ID)
	e.event(r, "contract.opened", contractEvent(c))
	e.applyMode(c, true)
}

// sameQuestion: b would ask what a asks, and be answered the same way.
func sameQuestion(a, b *Contract) bool {
	return a.Origin == b.Origin && a.Step == b.Step && a.Kind == b.Kind && a.Title == b.Title && a.Body == b.Body &&
		slices.Equal(a.Options, b.Options) && maps.Equal(a.Fields, b.Fields) && a.Default == b.Default &&
		a.Lock == b.Lock && a.Resolver == b.Resolver && reflect.DeepEqual(a.Mode, b.Mode)
}

func (e *Engine) readBodyFile(r *Run, tmpl string) (string, error) {
	p, missing := kamui.Render(tmpl, r.Vars)
	if len(missing) > 0 {
		return "", fmt.Errorf("body_file %q: vars not set: %s", tmpl, strings.Join(missing, ", "))
	}
	p = kekkai.ExpandHome(p)
	if !filepath.IsAbs(p) {
		p = filepath.Join(e.runDir(r), p)
	}
	raw, err := os.ReadFile(p)
	if err != nil {
		return "", err
	}
	if len(raw) > 64<<10 {
		raw = append(raw[:64<<10], []byte("\n…(truncated)")...)
	}
	return string(raw), nil
}

// EffectiveMode is the AFK mode a contract runs under right now, and why.
func (e *Engine) EffectiveMode(c *Contract) (*kamui.AFK, string) {
	if c.Lock {
		return &kamui.AFK{Kind: "attended"}, "locked: only a human can resolve it"
	}
	var m *kamui.AFK
	src := "default"
	switch {
	case c.Override != nil:
		m, src = c.Override, "contract override"
	case e.runs[c.RunID] != nil && e.runs[c.RunID].AFK != nil:
		m, src = e.runs[c.RunID].AFK, "run override"
	case c.Mode != nil:
		m, src = c.Mode, "workflow"
	default:
		if k, err := kekkai.Load(e.Home, c.Kekkai); err == nil && k.AFKDefault != nil {
			if pm, err := kamui.ParseMode(modeString(k.AFKDefault)); err == nil {
				m, src = pm, "kekkai default"
			}
		}
	}
	if m == nil || m.Kind == "attended" {
		return &kamui.AFK{Kind: "attended"}, src
	}
	if c.Resolver == "" {
		return &kamui.AFK{Kind: "attended"}, src + " asks for " + m.String() + ", but there is no resolver"
	}
	return m, src
}

func modeString(m *kekkai.Mode) string {
	if m.Kind == "grace" {
		return "grace=" + m.Grace
	}
	return m.Kind
}

// applyMode arms the contract for its effective mode. fresh means the grace
// window starts now; otherwise a stored deadline is kept (recovery).
func (e *Engine) applyMode(c *Contract, fresh bool) {
	if c.Status != "open" {
		return
	}
	e.stopTimer("contract:" + c.ID)
	m, src := e.EffectiveMode(c)
	switch m.Kind {
	case "attended":
		if c.Deadline != nil {
			c.Deadline = nil
			e.saveContract(c, "attended")
		}
	case "afk":
		e.runResolver(c)
	case "grace":
		if fresh || c.Deadline == nil {
			d := now().Add(m.Grace)
			c.Deadline = &d
			e.saveContract(c, "grace "+m.Grace.String())
		}
		id := c.ID
		var expire func()
		expire = func() {
			if e.offline != nil { // the owner cannot see it either: going online moves the deadline on
				e.setTimer("contract:"+id, now().Add(time.Minute), expire)
				return
			}
			if c := e.contracts[id]; c != nil && c.Status == "open" {
				e.event(e.runs[c.RunID], "contract.grace_expired", map[string]any{"id": id})
				e.runResolver(c)
			}
		}
		e.setTimer("contract:"+id, *c.Deadline, expire)
	}
	e.event(e.runs[c.RunID], "contract.mode", map[string]any{"id": c.ID, "mode": m.String(), "source": src})
}

// runResolver runs the contract's resolver command: ContractRequest JSON on
// stdin, {"answer","data","note"} on stdout.
func (e *Engine) runResolver(c *Contract) {
	if c.resolving || c.Status != "open" {
		return
	}
	r := e.runs[c.RunID]
	k, err := kekkai.Load(e.Home, c.Kekkai)
	if err != nil {
		e.resolverFailed(c, err.Error())
		return
	}
	args, _, err := commandArgs(c.Resolver, nil, k)
	if err != nil {
		e.resolverFailed(c, err.Error())
		return
	}
	in := map[string]any{"contract": c}
	if r != nil {
		in["run"] = map[string]any{"id": r.ID, "workflow": r.Workflow, "state": r.State, "vars": r.Vars}
	}
	stdin, _ := json.Marshal(in)
	dir := filepath.Join(e.Home, "runs", c.RunID, "resolvers")
	os.MkdirAll(dir, 0o700)
	env := k.WorkerEnv(map[string]string{"PATH": e.workerPath(), "KAMI_SOCK": e.Sock, "KAMI_CONTRACT": c.ID, "KAMI_RUN": c.RunID})
	c.resolving = true
	id := c.ID
	p, err := startProc(procSpec{Args: args, Dir: dir, Env: env, Stdin: stdin, Timeout: 5 * time.Minute}, id, func(res procResult) {
		e.resolverExited(id, res)
	})
	if err != nil {
		c.resolving = false
		e.resolverFailed(c, err.Error())
		return
	}
	e.resolvers[id] = p
	e.event(r, "resolver.started", map[string]any{"id": id, "resolver": c.Resolver})
}

func (e *Engine) resolverExited(id string, res procResult) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.resolvers, id)
	c := e.contracts[id]
	if e.closed || c == nil {
		return
	}
	c.resolving = false
	if c.Status != "open" {
		return // a human answered first
	}
	m, ok := lastJSON(res.Stdout)
	if res.Exit != 0 || res.TimedOut || !ok {
		why := fmt.Sprintf("exit %d", res.Exit)
		if res.TimedOut {
			why = "timed out"
		} else if res.Exit == 0 {
			why = "no JSON answer on stdout"
		}
		e.resolverFailed(c, why+": "+tail(res.Stderr, 400))
		return
	}
	ans, _ := m["answer"].(string)
	data, _ := m["data"].(map[string]any)
	note, _ := m["note"].(string)
	if err := e.resolve(c, Resolution{Answer: ans, Data: data, Note: note, By: "resolver", Resolver: c.Resolver}); err != nil {
		e.resolverFailed(c, "invalid answer: "+err.Error())
	}
}

// resolverFailed hands the contract back to a human. Never silent.
func (e *Engine) resolverFailed(c *Contract, why string) {
	c.Override = &kamui.AFK{Kind: "attended"}
	c.Deadline = nil
	e.saveContract(c, "resolver failed")
	e.event(e.runs[c.RunID], "contract.resolver_failed", map[string]any{"id": c.ID, "error": why})
}

// Resolve answers a contract as a human.
func (e *Engine) Resolve(id, answer string, data map[string]any, note string) (*Contract, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, err := e.findContract(id)
	if err != nil {
		return nil, err
	}
	if err := e.resolve(c, Resolution{Answer: answer, Data: data, Note: note, By: "human"}); err != nil {
		return nil, err
	}
	return c, nil
}

func (e *Engine) resolve(c *Contract, res Resolution) error {
	if c.Status != "open" {
		by := ""
		if c.Resolution != nil {
			by = " by " + c.Resolution.By
		}
		return uerr("CONFLICT", "contract %s is already %s%s", c.ID, c.Status, by)
	}
	if res.Answer == "" && c.Default != "" {
		res.Answer = c.Default
	}
	if res.Answer == "" && len(c.Options) == 1 {
		res.Answer = c.Options[0]
	}
	if strings.TrimSpace(res.Answer) == "" {
		return uerr("INVALID", "an answer is required")
	}
	if len(c.Options) > 0 && !contains(c.Options, res.Answer) {
		return uerr("INVALID", "answer %q is not one of: %s", res.Answer, strings.Join(c.Options, ", "))
	}
	for name, typ := range c.Fields {
		v, ok := res.Data[name]
		if !ok || v == nil {
			if !strings.HasSuffix(typ, "?") {
				return uerr("INVALID", "field %q (%s) is required", name, typ)
			}
			continue
		}
		if err := checkType(strings.TrimSuffix(typ, "?"), v); err != nil {
			return uerr("INVALID", "field %q: %v", name, err)
		}
	}
	res.At = now()
	c.Status, c.Resolution, c.Deadline = "resolved", &res, nil
	e.stopTimer("contract:" + c.ID)
	if p := e.resolvers[c.ID]; p != nil && (res.By == "human" || res.By == BySystem) {
		p.kill()
	}
	e.saveContract(c, "resolved by "+res.By)
	e.event(e.runs[c.RunID], "contract.resolved", map[string]any{"id": c.ID, "answer": res.Answer, "by": res.By})
	e.applyResolution(c)
	return nil
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

// applyResolution moves the run on. It is idempotent so recovery can call it
// for a contract that was resolved just before a crash.
func (e *Engine) applyResolution(c *Contract) {
	r := e.runs[c.RunID]
	if r == nil || Terminal(r.Status) || r.Step == nil || r.Step.Contract != c.ID {
		return
	}
	res := c.Resolution
	switch c.Origin {
	case "state":
		vars := map[string]any{}
		for k, v := range res.Data {
			vars[k] = v
		}
		vars[c.Step] = res.Answer
		out := res.Answer
		if c.Kind == kamui.KAsk {
			out = "answered"
		}
		r.Step.Phase = "done"
		e.outcome(r, out, vars)
	case "worker":
		if r.Step.Phase == "sleeping" {
			e.startAgent(r, &wake{resolution: c})
		}
		// still running: agentExited sees the resolved contract and wakes it
	case "engine":
		switch res.Answer {
		case "stop":
			e.finish(r, StFailed, "stopped at "+r.State+": "+c.Title)
		default: // continue | retry
			if r.Step.Purpose == "budget" {
				r.Visits[r.State] = 0
			}
			e.enter(r, r.State)
		}
	}
}

// SetAFK changes the mode of one open contract, or of a run (and its open contracts).
func (e *Engine) SetAFK(runID, contractID string, m *kamui.AFK) (any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if contractID != "" {
		c, err := e.findContract(contractID)
		if err != nil {
			return nil, err
		}
		if c.Lock {
			return nil, uerr("FORBIDDEN", "contract %s is locked: only a human can resolve it", c.ID)
		}
		if c.Status != "open" {
			return nil, uerr("CONFLICT", "contract %s is %s", c.ID, c.Status)
		}
		c.Override = m
		e.saveContract(c, "afk "+m.String())
		e.applyMode(c, true)
		return c, nil
	}
	r, err := e.findRun(runID)
	if err != nil {
		return nil, err
	}
	if Terminal(r.Status) {
		return nil, uerr("CONFLICT", "run %s is %s", r.ID, r.Status)
	}
	r.AFK = m
	e.saveRun(r, "afk "+m.String())
	for _, c := range e.contracts {
		if c.RunID == r.ID && c.Status == "open" && c.Override == nil && !c.Lock {
			e.applyMode(c, true)
		}
	}
	return r, nil
}
