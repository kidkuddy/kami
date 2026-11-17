package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"kami/internal/eventbus"
	"kami/internal/kamui"
)

// Envelope is every API response: {ok, data, error}.
type Envelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error *APIError       `json:"error,omitempty"`
}

// APIError is the error half of an Envelope.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

var codeStatus = map[string]int{"NOT_FOUND": 404, "INVALID": 422, "CONFLICT": 409, "FORBIDDEN": 403,
	"AMBIGUOUS": 409, "BAD_REQUEST": 400, "UNAVAILABLE": 503}

func reply(w http.ResponseWriter, v any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		var ue *UserError
		code, status := "INTERNAL", 500
		if errors.As(err, &ue) {
			code, status = ue.Code, codeStatus[ue.Code]
			if status == 0 {
				status = 400
			}
		}
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(Envelope{Error: &APIError{Code: code, Message: err.Error()}})
		return
	}
	raw, merr := json.Marshal(v)
	if merr != nil {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(Envelope{Error: &APIError{Code: "INTERNAL", Message: merr.Error()}})
		return
	}
	json.NewEncoder(w).Encode(Envelope{OK: true, Data: raw})
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 4<<20))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return uerr("BAD_REQUEST", "request body: %v", err)
	}
	return nil
}

// normalize turns json.Number into float64 so vars stay JSON-shaped.
func normalize(v any) any {
	switch x := v.(type) {
	case json.Number:
		f, _ := x.Float64()
		return f
	case map[string]any:
		for k, e := range x {
			x[k] = normalize(e)
		}
	case []any:
		for i, e := range x {
			x[i] = normalize(e)
		}
	}
	return v
}

func normMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	return normalize(m).(map[string]any)
}

// workerOnly refuses calls made from inside a worker (KAMI_TOKEN is set):
// decisions and run control belong to humans.
func workerOnly(r *http.Request) error {
	if r.Header.Get("X-Kami-Token") != "" {
		return uerr("FORBIDDEN", "workers cannot do this; raise a contract with `kami contract request` instead")
	}
	return nil
}

// Handler is the daemon's HTTP API.
func (e *Engine) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) { reply(w, e.Status(), nil) })

	m.HandleFunc("POST /runs", func(w http.ResponseWriter, r *http.Request) {
		if err := workerOnly(r); err != nil {
			reply(w, nil, err)
			return
		}
		var req struct {
			Kekkai, Workflow, Entry string
			Inputs                  map[string]any
			AFK                     string
		}
		if err := decode(r, &req); err != nil {
			reply(w, nil, err)
			return
		}
		var afk *kamui.AFK
		if req.AFK != "" {
			m, err := kamui.ParseMode(req.AFK)
			if err != nil {
				reply(w, nil, uerr("INVALID", "%v", err))
				return
			}
			afk = m
		}
		run, err := e.StartRun(req.Kekkai, req.Workflow, req.Entry, normMap(req.Inputs), afk)
		reply(w, e.view(run), err)
	})
	m.HandleFunc("GET /runs", func(w http.ResponseWriter, r *http.Request) {
		reply(w, e.ListRuns(r.URL.Query().Get("status"), r.URL.Query().Get("kekkai"), r.URL.Query()["var"]), nil)
	})
	m.HandleFunc("GET /runs/{id}", func(w http.ResponseWriter, r *http.Request) {
		v, err := e.ShowRun(r.PathValue("id"))
		reply(w, v, err)
	})
	m.HandleFunc("POST /runs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		if err := workerOnly(r); err != nil {
			reply(w, nil, err)
			return
		}
		run, err := e.Cancel(r.PathValue("id"))
		reply(w, e.view(run), err)
	})

	m.HandleFunc("POST /runs/{id}/upgrade", func(w http.ResponseWriter, r *http.Request) {
		if err := workerOnly(r); err != nil {
			reply(w, nil, err)
			return
		}
		var req struct {
			State string
			Vars  map[string]any
		}
		if err := decode(r, &req); err != nil {
			reply(w, nil, err)
			return
		}
		run, err := e.Upgrade(r.PathValue("id"), req.State, normMap(req.Vars))
		reply(w, e.view(run), err)
	})

	m.HandleFunc("POST /runs/{id}/system", func(w http.ResponseWriter, r *http.Request) {
		if err := workerOnly(r); err != nil {
			reply(w, nil, err)
			return
		}
		var req struct {
			Text, State, Answer string
			Vars                map[string]any
		}
		if err := decode(r, &req); err != nil {
			reply(w, nil, err)
			return
		}
		run, err := e.System(r.PathValue("id"), req.Text, req.State, req.Answer, normMap(req.Vars))
		reply(w, e.view(run), err)
	})

	m.HandleFunc("POST /signal", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Kekkai, Type, Key string
			Data              map[string]any
		}
		if err := decode(r, &req); err != nil {
			reply(w, nil, err)
			return
		}
		if req.Kekkai == "" || req.Type == "" {
			reply(w, nil, uerr("INVALID", "kekkai and type are required"))
			return
		}
		reply(w, map[string]any{"delivered": e.Publish(req.Kekkai, req.Type, req.Key, normMap(req.Data))}, nil)
	})

	m.HandleFunc("GET /contracts", func(w http.ResponseWriter, r *http.Request) {
		reply(w, e.ListContracts(r.URL.Query().Get("status"), r.URL.Query().Get("kekkai"), r.URL.Query().Get("run")), nil)
	})
	m.HandleFunc("GET /contracts/{id}", func(w http.ResponseWriter, r *http.Request) {
		v, err := e.ShowContract(r.PathValue("id"))
		reply(w, v, err)
	})
	m.HandleFunc("POST /contracts/{id}/resolve", func(w http.ResponseWriter, r *http.Request) {
		if err := workerOnly(r); err != nil {
			reply(w, nil, err)
			return
		}
		var req struct {
			Answer, Note string
			Data         map[string]any
		}
		if err := decode(r, &req); err != nil {
			reply(w, nil, err)
			return
		}
		c, err := e.Resolve(r.PathValue("id"), req.Answer, normMap(req.Data), req.Note)
		if err != nil {
			reply(w, nil, err)
			return
		}
		v, err := e.ShowContract(c.ID)
		reply(w, v, err)
	})
	m.HandleFunc("POST /afk", func(w http.ResponseWriter, r *http.Request) {
		if err := workerOnly(r); err != nil {
			reply(w, nil, err)
			return
		}
		var req struct{ Mode, Run, Contract string }
		if err := decode(r, &req); err != nil {
			reply(w, nil, err)
			return
		}
		mode, err := kamui.ParseMode(req.Mode)
		if err != nil {
			reply(w, nil, uerr("INVALID", "%v", err))
			return
		}
		if (req.Run == "") == (req.Contract == "") {
			reply(w, nil, uerr("INVALID", "give exactly one of run or contract"))
			return
		}
		if _, err := e.SetAFK(req.Run, req.Contract, mode); err != nil {
			reply(w, nil, err)
			return
		}
		if req.Contract != "" {
			v, err := e.ShowContract(req.Contract)
			reply(w, v, err)
			return
		}
		v, err := e.AFKView(req.Run)
		reply(w, v, err)
	})
	m.HandleFunc("GET /afk/{run}", func(w http.ResponseWriter, r *http.Request) {
		v, err := e.AFKView(r.PathValue("run"))
		reply(w, v, err)
	})
	m.HandleFunc("POST /kekkai/{k}/pause", func(w http.ResponseWriter, r *http.Request) {
		if err := workerOnly(r); err != nil {
			reply(w, nil, err)
			return
		}
		var req struct {
			Until  *time.Time
			Except []string
		}
		if err := decode(r, &req); err != nil {
			reply(w, nil, err)
			return
		}
		p, err := e.PauseKekkai(r.PathValue("k"), req.Until, req.Except)
		reply(w, p, err)
	})
	m.HandleFunc("POST /kekkai/{k}/allow", func(w http.ResponseWriter, r *http.Request) {
		if err := workerOnly(r); err != nil {
			reply(w, nil, err)
			return
		}
		var req struct {
			Thread string
			On     bool
		}
		if err := decode(r, &req); err != nil {
			reply(w, nil, err)
			return
		}
		p, ids, err := e.AllowThread(r.PathValue("k"), req.Thread, req.On)
		reply(w, map[string]any{"pause": p, "released": ids}, err)
	})
	m.HandleFunc("POST /kekkai/{k}/resume", func(w http.ResponseWriter, r *http.Request) {
		if err := workerOnly(r); err != nil {
			reply(w, nil, err)
			return
		}
		ids, err := e.ResumeKekkai(r.PathValue("k"))
		reply(w, map[string]any{"resumed": ids}, err)
	})
	m.HandleFunc("POST /triggers/reload", func(w http.ResponseWriter, r *http.Request) {
		reply(w, e.ReloadTriggers(), nil)
	})
	m.HandleFunc("GET /events", e.handleEvents)

	worker := func(w http.ResponseWriter, r *http.Request) (string, string) {
		return r.Header.Get("X-Kami-Run"), r.Header.Get("X-Kami-Token")
	}
	// memory: shared between the owner (no token) and every worker, filtered by
	// the caller's conversation
	m.HandleFunc("POST /memory", func(w http.ResponseWriter, r *http.Request) {
		run, tok := worker(w, r)
		var req struct{ About, Fact, Scope string }
		if err := decode(r, &req); err != nil {
			reply(w, nil, err)
			return
		}
		f, err := e.Remember(run, tok, req.About, req.Fact, req.Scope)
		reply(w, f, err)
	})
	m.HandleFunc("GET /memory", func(w http.ResponseWriter, r *http.Request) {
		run, tok := worker(w, r)
		fs, err := e.Recall(run, tok, r.URL.Query().Get("about"), r.URL.Query().Get("q"))
		reply(w, fs, err)
	})
	m.HandleFunc("DELETE /memory/{id}", func(w http.ResponseWriter, r *http.Request) {
		_, tok := worker(w, r)
		reply(w, map[string]any{"forgot": r.PathValue("id")}, e.Forget(tok, r.PathValue("id")))
	})
	m.HandleFunc("POST /worker/done", func(w http.ResponseWriter, r *http.Request) {
		run, tok := worker(w, r)
		var req struct {
			Outcome string
			Vars    map[string]any
		}
		if err := decode(r, &req); err != nil {
			reply(w, nil, err)
			return
		}
		err := e.StepDone(run, tok, req.Outcome, normMap(req.Vars))
		reply(w, map[string]any{"outcome": req.Outcome, "note": "recorded; exit now"}, err)
	})
	m.HandleFunc("POST /worker/contract", func(w http.ResponseWriter, r *http.Request) {
		run, tok := worker(w, r)
		var req ContractRequest
		if err := decode(r, &req); err != nil {
			reply(w, nil, err)
			return
		}
		c, err := e.WorkerContract(run, tok, req)
		if err != nil {
			reply(w, nil, err)
			return
		}
		reply(w, map[string]any{"id": c.ID, "note": "save an aura with `kami state snapshot`, then exit"}, nil)
	})
	m.HandleFunc("POST /worker/snapshot", func(w http.ResponseWriter, r *http.Request) {
		run, tok := worker(w, r)
		var a Aura
		if err := decode(r, &a); err != nil {
			reply(w, nil, err)
			return
		}
		a.Vars = normMap(a.Vars)
		out, err := e.Snapshot(run, tok, a)
		reply(w, out, err)
	})
	return m
}

func (e *Engine) handleEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	since, _ := strconv.ParseInt(q.Get("since"), 10, 64)
	f := eventbus.Filter{Kekkai: q.Get("kekkai"), Run: q.Get("run"), Type: q.Get("type")}
	// SSE when asked for: always a stream, and a reconnect resumes from Last-Event-ID.
	sse := strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	if id, err := strconv.ParseInt(r.Header.Get("Last-Event-ID"), 10, 64); sse && err == nil {
		since = id
	}
	follow := sse || q.Get("follow") == "1"
	var ch <-chan eventbus.Event
	if follow {
		var cancel func()
		ch, cancel = e.Bus.Subscribe(f)
		defer cancel()
	}
	enc := json.NewEncoder(w)
	send := enc.Encode
	keepalive := "\n" // readers skip blank lines
	w.Header().Set("Content-Type", "application/x-ndjson")
	if sse {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		keepalive = ": ping\n\n"
		send = func(v any) error {
			raw, err := json.Marshal(v)
			if err != nil {
				return err
			}
			name := "error"
			id := ""
			if ev, ok := v.(eventbus.Event); ok {
				name, id = ev.Type, fmt.Sprintf("id: %d\n", ev.ID)
			}
			_, err = fmt.Fprintf(w, "%sevent: %s\ndata: %s\n\n", id, name, raw)
			return err
		}
	}
	fl, _ := w.(http.Flusher)
	last := since
	for {
		batch, err := e.Bus.Since(last, f, 1000)
		if err != nil {
			send(map[string]any{"error": err.Error()})
			return
		}
		for _, ev := range batch {
			send(ev)
			last = ev.ID
		}
		if len(batch) < 1000 {
			break
		}
	}
	if fl != nil {
		fl.Flush()
	}
	if !follow {
		return
	}
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if ev.ID != 0 && ev.ID <= last {
				continue
			}
			if err := send(ev); err != nil {
				return
			}
			last = ev.ID
			if fl != nil {
				fl.Flush()
			}
		case <-ping.C:
			// keeps idle connections from being reaped
			w.Write([]byte(keepalive))
			if fl != nil {
				fl.Flush()
			}
		}
	}
}

// --- views (taken under the lock, marshalled after) ------------------------------

// RunView is a run as the CLI shows it.
type RunView struct {
	*Run
	Contracts []ContractView `json:"contracts,omitempty"`
	Auras     []*Aura        `json:"auras,omitempty"`
	Children  []RunBrief     `json:"children,omitempty"`
	Events    []any          `json:"events,omitempty"`
}

// RunBrief is one line of `kami run list`.
type RunBrief struct {
	ID       string    `json:"id"`
	Kekkai   string    `json:"kekkai"`
	Workflow string    `json:"workflow"`
	State    string    `json:"state"`
	Status   string    `json:"status"`
	Parent   string    `json:"parent,omitempty"`
	Updated  time.Time `json:"updated_at"`
}

func clone[T any](v *T) *T {
	if v == nil {
		return nil
	}
	raw, _ := json.Marshal(v)
	out := new(T)
	json.Unmarshal(raw, out)
	return out
}

func (e *Engine) view(r *Run) *Run {
	if r == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return clone(r)
}

func brief(r *Run) RunBrief {
	return RunBrief{r.ID, r.Kekkai, r.Workflow, r.State, r.Status, r.Parent, r.Updated}
}

// ListRuns filters runs; vars are "k=v" pairs matched as strings.
func (e *Engine) ListRuns(status, k string, vars []string) []RunBrief {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []RunBrief
	for _, r := range e.runs {
		if (status != "" && r.Status != status && !(status == "active" && !Terminal(r.Status))) || (k != "" && r.Kekkai != k) {
			continue
		}
		ok := true
		for _, kv := range vars {
			key, val, _ := strings.Cut(kv, "=")
			v, found := kamui.Lookup(r.Vars, key)
			if !found || kamui.Stringify(v) != val {
				ok = false
			}
		}
		if ok {
			out = append(out, brief(r))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ShowRun returns a run with its contracts, auras, children and recent events.
func (e *Engine) ShowRun(id string) (*RunView, error) {
	e.mu.Lock()
	r, err := e.findRun(id)
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	v := &RunView{Run: clone(r)}
	for _, c := range e.contracts {
		if c.RunID == r.ID {
			v.Contracts = append(v.Contracts, e.contractView(c))
		}
	}
	for _, a := range e.auras[r.ID] {
		v.Auras = append(v.Auras, clone(a))
	}
	var kids []*Run
	for _, c := range e.runs {
		if c.Parent == r.ID {
			kids = append(kids, c)
		}
	}
	sort.Slice(kids, func(i, j int) bool {
		if kids[i].ParentIndex != kids[j].ParentIndex {
			return kids[i].ParentIndex < kids[j].ParentIndex
		}
		return kids[i].ID < kids[j].ID
	})
	for _, c := range kids {
		v.Children = append(v.Children, brief(c))
	}
	runID := r.ID
	e.mu.Unlock()
	sort.Slice(v.Contracts, func(i, j int) bool { return v.Contracts[i].ID < v.Contracts[j].ID })
	if evs, err := e.Bus.Since(0, eventbus.Filter{Run: runID}, 100000); err == nil {
		if len(evs) > 200 {
			evs = evs[len(evs)-200:]
		}
		for _, ev := range evs {
			v.Events = append(v.Events, ev)
		}
	}
	return v, nil
}

// ContractView adds the effective AFK mode to a contract.
type ContractView struct {
	*Contract
	Effective string `json:"effective_mode"`
	Why       string `json:"mode_source"`
	Workflow  string `json:"workflow,omitempty"`
}

func (e *Engine) contractView(c *Contract) ContractView {
	m, why := e.EffectiveMode(c)
	v := ContractView{Contract: clone(c), Effective: m.String(), Why: why}
	if r := e.runs[c.RunID]; r != nil {
		v.Workflow = r.Workflow
	}
	return v
}

// ListContracts filters contracts.
func (e *Engine) ListContracts(status, k, run string) []ContractView {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := []ContractView{}
	for _, c := range e.contracts {
		if (status != "" && c.Status != status) || (k != "" && c.Kekkai != k) || (run != "" && !strings.HasPrefix(c.RunID, run)) {
			continue
		}
		out = append(out, e.contractView(c))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ShowContract returns one contract.
func (e *Engine) ShowContract(id string) (*ContractView, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, err := e.findContract(id)
	if err != nil {
		return nil, err
	}
	v := e.contractView(c)
	return &v, nil
}

// AFKView shows a run's AFK settings and its open contracts' effective modes.
func (e *Engine) AFKView(id string) (map[string]any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, err := e.findRun(id)
	if err != nil {
		return nil, err
	}
	w := e.def(r)
	var open []ContractView
	for _, c := range e.contracts {
		if c.RunID == r.ID && c.Status == "open" {
			open = append(open, e.contractView(c))
		}
	}
	def := "attended"
	if w != nil && w.AFK != nil {
		def = w.AFK.String()
	}
	over := ""
	if r.AFK != nil {
		over = r.AFK.String()
	}
	return map[string]any{"run": r.ID, "workflow_default": def, "run_override": over, "open_contracts": open}, nil
}

// Status summarises the daemon.
func (e *Engine) Status() map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	by := map[string]int{}
	for _, r := range e.runs {
		by[r.Status]++
	}
	open := 0
	for _, c := range e.contracts {
		if c.Status == "open" {
			open++
		}
	}
	trig := map[string]string{}
	for k, t := range e.timers {
		_ = t
		if strings.HasPrefix(k, "trigger:") {
			trig[strings.TrimPrefix(k, "trigger:")] = "armed"
		}
	}
	return map[string]any{"pid": os.Getpid(), "home": e.Home, "memento_namespace": e.Mem.NS, "runs": by,
		"open_contracts": open, "running_processes": len(e.procs), "triggers": trig, "paused": e.pauses,
		"version": fmt.Sprintf("kami %s", Version)}
}

// Version is set at build time with -ldflags "-X kami/internal/engine.Version=...".
var Version = "dev"
