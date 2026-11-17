// Command kami is the Kami CLI and daemon.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"kami/internal/engine"
	"kami/internal/kekkai"
)

// Exit codes, stable per error class.
const (
	exitOK       = 0
	exitInternal = 1
	exitUsage    = 2
	exitNotFound = 3
	exitInvalid  = 4
	exitConflict = 5
	exitDaemon   = 6
)

var codeExit = map[string]int{"NOT_FOUND": exitNotFound, "INVALID": exitInvalid, "BAD_REQUEST": exitUsage,
	"CONFLICT": exitConflict, "FORBIDDEN": exitConflict, "AMBIGUOUS": exitConflict, "UNAVAILABLE": exitDaemon,
	"USAGE": exitUsage, "DAEMON": exitDaemon}

var jsonOut bool

type cliError struct {
	Code string
	Msg  string
}

func (e *cliError) Error() string { return e.Msg }

func fail(code, f string, a ...any) error { return &cliError{Code: code, Msg: fmt.Sprintf(f, a...)} }

func main() {
	args := []string{}
	for _, a := range os.Args[1:] {
		if a == "--json" {
			jsonOut = true
			continue
		}
		args = append(args, a)
	}
	err := run(args)
	if err == nil {
		return
	}
	if se, ok := err.(*silentErr); ok {
		os.Exit(se.code)
	}
	code := "INTERNAL"
	if ce, ok := err.(*cliError); ok {
		code = ce.Code
	}
	if jsonOut {
		json.NewEncoder(os.Stdout).Encode(map[string]any{"ok": false, "error": map[string]string{"code": code, "message": err.Error()}})
	} else {
		fmt.Fprintln(os.Stderr, "kami: "+err.Error())
	}
	if c, ok := codeExit[code]; ok {
		os.Exit(c)
	}
	os.Exit(exitInternal)
}

func home() string {
	if h := os.Getenv("KAMI_HOME"); h != "" {
		return h
	}
	h, _ := os.UserHomeDir()
	return filepath.Join(h, ".kami")
}

func sockPath() string {
	if s := os.Getenv("KAMI_SOCK"); s != "" {
		return s
	}
	return filepath.Join(home(), "kami.sock")
}

const usage = `kami — the Kami agent OS

  kami daemon [start|stop|status]          run the daemon (foreground by default)
  kami status                              daemon, runs, open contracts

  kami kekkai list | current | switch <name> | validate [<name>] | reload
  kami kekkai pause <name> [--until HH:MM|RFC3339] [--except wf,wf] | resume <name> | allow|hold <name> <thread_ts|run id>
  kami workflow list | validate <file|name> | dry-run <file|name>   [--kekkai K]

  kami run start <workflow>[:<entry>] [--kekkai K] [--input k=v]... [--inputs JSON] [--afk MODE]
  kami run list [--status S|active] [--kekkai K] [--var k=v]...
  kami run show <id> | cancel <id> | upgrade <id> [--state S] [--vars JSON]

  kami system <run> <text> [--state S] [--vars JSON] [--answer A]
  kami signal pub <type> [--key K] [--data JSON] [--kekkai K]

  kami contract list [--status open|resolved|all] [--kekkai K] [--run R]
  kami contract show <id>
  kami contract resolve <id> [<answer>] [--field k=v]... [--data JSON] [--note T]
  kami afk show <run> | set <attended|afk|grace=10m> (--run ID | --contract ID)

  kami events tail [-f] [--since ID] [--type PREFIX] [--kekkai K] [--run R]

  kami memory save [--about WHO] [--scope public|private|conduct|dm:<user>|chan:<channel>] <fact...>
  kami memory load [--about WHO] [--q TEXT]      facts this conversation may hear (all, for you)
  kami memory forget <id>                         owner only
    shared by you and every worker; a worker in a DM or channel only hears public facts and
    its own conversation's, and what it saves stays in that conversation

  worker side (KAMI_RUN and KAMI_TOKEN set by the harness):
  kami step done --outcome O [--vars JSON]
  kami contract request --kind ask|choice|form|blocker --title T [--body B] [--options a,b] [--default a]
  kami state snapshot < aura.json
  kami state show <run>

  --json on any command prints the {ok, data, error} envelope.
  Ids accept any unique prefix.`

func run(args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		fmt.Println(usage)
		return nil
	}
	cmd, rest := args[0], args[1:]
	sub := ""
	if len(rest) > 0 {
		sub = rest[0]
	}
	switch cmd {
	case "daemon":
		return cmdDaemon(rest)
	case "status":
		return cmdStatus()
	case "kekkai":
		return cmdKekkai(sub, tailArgs(rest))
	case "workflow":
		return cmdWorkflow(sub, tailArgs(rest))
	case "run":
		return cmdRun(sub, tailArgs(rest))
	case "system":
		return cmdSystem(args[1:])
	case "signal":
		if sub != "pub" {
			return fail("USAGE", "usage: kami signal pub <type> [--key K] [--data JSON] [--kekkai K]")
		}
		return cmdSignal(tailArgs(rest))
	case "contract":
		return cmdContract(sub, tailArgs(rest))
	case "afk":
		return cmdAFK(sub, tailArgs(rest))
	case "events":
		if sub != "tail" {
			return fail("USAGE", "usage: kami events tail [-f] [--since ID] [--type T] [--kekkai K] [--run R]")
		}
		return cmdEvents(tailArgs(rest))
	case "step":
		if sub != "done" {
			return fail("USAGE", "usage: kami step done --outcome O [--vars JSON]")
		}
		return cmdStepDone(tailArgs(rest))
	case "state":
		return cmdState(sub, tailArgs(rest))
	case "memory":
		return cmdMemory(sub, tailArgs(rest))
	}
	return fail("USAGE", "unknown command %q (kami help)", cmd)
}

func tailArgs(a []string) []string {
	if len(a) == 0 {
		return nil
	}
	return a[1:]
}

// --- flag parsing: flags and positionals may interleave -------------------------

type flags struct {
	pos  []string
	vals map[string][]string
}

func (f flags) get(n string) string {
	if v := f.vals[n]; len(v) > 0 {
		return v[len(v)-1]
	}
	return ""
}

func (f flags) has(n string) bool { _, ok := f.vals[n]; return ok }

// parse reads args against spec: name -> takes a value. Short aliases map to long names.
func parse(args []string, spec map[string]bool, alias map[string]string) (flags, error) {
	f := flags{vals: map[string][]string{}}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			f.pos = append(f.pos, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(a, "-") || a == "-" {
			f.pos = append(f.pos, a)
			continue
		}
		name, val, hasEq := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if long, ok := alias[name]; ok {
			name = long
		}
		takes, ok := spec[name]
		if !ok {
			return f, fail("USAGE", "unknown flag %s", a)
		}
		if takes && !hasEq {
			if i+1 >= len(args) {
				return f, fail("USAGE", "flag --%s needs a value", name)
			}
			i++
			val = args[i]
		}
		f.vals[name] = append(f.vals[name], val)
	}
	return f, nil
}

// --- client ---------------------------------------------------------------------------

func client() *http.Client {
	return &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sockPath())
	}}}
}

// call sends a request to the daemon and decodes the envelope's data into out.
func call(method, path string, body any, out any) (json.RawMessage, error) {
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, "http://kami"+path, rd)
	if err != nil {
		return nil, err
	}
	if t := os.Getenv("KAMI_TOKEN"); t != "" {
		req.Header.Set("X-Kami-Token", t)
		req.Header.Set("X-Kami-Run", os.Getenv("KAMI_RUN"))
	}
	resp, err := client().Do(req)
	if err != nil {
		return nil, fail("DAEMON", "daemon not reachable at %s (start it with `kami daemon start`): %v", sockPath(), err)
	}
	defer resp.Body.Close()
	var env engine.Envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, fail("INTERNAL", "bad response from daemon: %v", err)
	}
	if !env.OK {
		if env.Error == nil {
			return nil, fail("INTERNAL", "daemon error (HTTP %d)", resp.StatusCode)
		}
		return nil, fail(env.Error.Code, "%s", env.Error.Message)
	}
	if out != nil {
		if err := json.Unmarshal(env.Data, out); err != nil {
			return nil, err
		}
	}
	return env.Data, nil
}

// emit prints data as the JSON envelope, or calls human.
func emit(data json.RawMessage, human func()) error {
	if jsonOut {
		json.NewEncoder(os.Stdout).Encode(engine.Envelope{OK: true, Data: data})
		return nil
	}
	human()
	return nil
}

func emitValue(v any, human func()) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return emit(raw, human)
}

func pretty(v any) string {
	raw, _ := json.MarshalIndent(v, "", "  ")
	return string(raw)
}

func ago(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	d := time.Since(t).Round(time.Second)
	switch {
	case d < time.Minute:
		return d.String() + " ago"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
	return t.Local().Format("2006-01-02 15:04")
}

func table(header []string, rows [][]string) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, strings.Join(header, "\t"))
	for _, r := range rows {
		fmt.Fprintln(tw, strings.Join(r, "\t"))
	}
	tw.Flush()
}

func activeKekkai(f flags) (string, error) {
	if k := f.get("kekkai"); k != "" {
		return k, nil
	}
	c, err := kekkai.LoadConfig(home())
	if err != nil {
		return "", err
	}
	if c.Active == "" {
		return "", fail("USAGE", "no kekkai given and none active (kami kekkai switch <name>, or --kekkai K)")
	}
	return c.Active, nil
}

// parseValue reads k=v; v is JSON when it parses as JSON, else a string.
func parseKV(s string) (string, any, error) {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return "", nil, fail("USAGE", "want key=value, got %q", s)
	}
	var j any
	if json.Unmarshal([]byte(v), &j) == nil {
		return k, j, nil
	}
	return k, v, nil
}

func jsonObject(s, what string) (map[string]any, error) {
	if s == "" {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, fail("USAGE", "%s must be a JSON object: %v", what, err)
	}
	return m, nil
}

// --- status -----------------------------------------------------------------------------

func cmdStatus() error {
	var st map[string]any
	data, err := call("GET", "/status", nil, &st)
	if err != nil {
		return err
	}
	return emit(data, func() {
		fmt.Printf("%v  pid %v  home %v  memento ns %v\n", st["version"], st["pid"], st["home"], st["memento_namespace"])
		runs, _ := st["runs"].(map[string]any)
		keys := make([]string, 0, len(runs))
		for k := range runs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var parts []string
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s %v", k, runs[k]))
		}
		if len(parts) == 0 {
			parts = []string{"none"}
		}
		fmt.Printf("runs: %s\nopen contracts: %v   running processes: %v\n", strings.Join(parts, ", "), st["open_contracts"], st["running_processes"])
		if tr, _ := st["triggers"].(map[string]any); len(tr) > 0 {
			var ids []string
			for id := range tr {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			fmt.Printf("cron triggers: %s\n", strings.Join(ids, ", "))
		}
	})
}

// --- runs ---------------------------------------------------------------------------------

func cmdRun(sub string, args []string) error {
	switch sub {
	case "start":
		f, err := parse(args, map[string]bool{"kekkai": true, "input": true, "inputs": true, "afk": true}, map[string]string{"k": "kekkai", "i": "input"})
		if err != nil {
			return err
		}
		if len(f.pos) != 1 {
			return fail("USAGE", "usage: kami run start <workflow>[:<entry>] [--kekkai K] [--input k=v]...")
		}
		k, err := activeKekkai(f)
		if err != nil {
			return err
		}
		wf, entry, _ := strings.Cut(f.pos[0], ":")
		inputs, err := jsonObject(f.get("inputs"), "--inputs")
		if err != nil {
			return err
		}
		if inputs == nil {
			inputs = map[string]any{}
		}
		for _, kv := range f.vals["input"] {
			key, v, err := parseKV(kv)
			if err != nil {
				return err
			}
			inputs[key] = v
		}
		var r engine.Run
		data, err := call("POST", "/runs", map[string]any{"kekkai": k, "workflow": wf, "entry": entry, "inputs": inputs, "afk": f.get("afk")}, &r)
		if err != nil {
			return err
		}
		return emit(data, func() {
			fmt.Printf("started %s  %s/%s  state %s (%s)\n", r.ID, r.Kekkai, r.Workflow, r.State, r.Status)
		})
	case "list":
		f, err := parse(args, map[string]bool{"status": true, "kekkai": true, "var": true}, map[string]string{"k": "kekkai"})
		if err != nil {
			return err
		}
		q := url.Values{}
		if s := f.get("status"); s != "" && s != "all" {
			q.Set("status", s)
		}
		if k := f.get("kekkai"); k != "" {
			q.Set("kekkai", k)
		}
		for _, v := range f.vals["var"] {
			q.Add("var", v)
		}
		var runs []engine.RunBrief
		data, err := call("GET", "/runs?"+q.Encode(), nil, &runs)
		if err != nil {
			return err
		}
		return emit(data, func() {
			rows := [][]string{}
			for _, r := range runs {
				parent := ""
				if r.Parent != "" {
					parent = "↳ " + r.Parent[:10]
				}
				rows = append(rows, []string{r.ID, r.Kekkai, r.Workflow, r.State, r.Status, ago(r.Updated), parent})
			}
			table([]string{"RUN", "KEKKAI", "WORKFLOW", "STATE", "STATUS", "UPDATED", "PARENT"}, rows)
		})
	case "show":
		if len(args) != 1 {
			return fail("USAGE", "usage: kami run show <id>")
		}
		var v struct {
			engine.Run
			Contracts []engine.ContractView `json:"contracts"`
			Auras     []engine.Aura         `json:"auras"`
			Children  []engine.RunBrief     `json:"children"`
			Events    []map[string]any      `json:"events"`
		}
		data, err := call("GET", "/runs/"+url.PathEscape(args[0]), nil, &v)
		if err != nil {
			return err
		}
		return emit(data, func() { showRun(&v.Run, v.Contracts, v.Auras, v.Children, v.Events) })
	case "cancel":
		if len(args) != 1 {
			return fail("USAGE", "usage: kami run cancel <id>")
		}
		var r engine.Run
		data, err := call("POST", "/runs/"+url.PathEscape(args[0])+"/cancel", map[string]any{}, &r)
		if err != nil {
			return err
		}
		return emit(data, func() { fmt.Printf("cancelled %s\n", r.ID) })
	case "upgrade":
		usage := fail("USAGE", "usage: kami run upgrade <id> [--state S] [--vars JSON]")
		if len(args) < 1 || len(args)%2 == 0 {
			return usage
		}
		body := map[string]any{}
		for i := 1; i < len(args); i += 2 {
			switch args[i] {
			case "--state":
				body["state"] = args[i+1]
			case "--vars":
				var vars map[string]any
				if err := json.Unmarshal([]byte(args[i+1]), &vars); err != nil {
					return fail("USAGE", "--vars: %v", err)
				}
				body["vars"] = vars
			default:
				return usage
			}
		}
		var r engine.Run
		data, err := call("POST", "/runs/"+url.PathEscape(args[0])+"/upgrade", body, &r)
		if err != nil {
			return err
		}
		return emit(data, func() { fmt.Printf("upgraded %s → %s\n", r.ID, r.State) })
	}
	return fail("USAGE", "usage: kami run start|list|show|cancel|upgrade")
}

func showRun(r *engine.Run, cs []engine.ContractView, auras []engine.Aura, kids []engine.RunBrief, events []map[string]any) {
	fmt.Printf("run       %s\nworkflow  %s/%s  (entry %s, def %s)\nstate     %s  [%s]\n", r.ID, r.Kekkai, r.Workflow, r.Entry, short(r.DefHash), r.State, r.Status)
	if r.Parent != "" {
		fmt.Printf("parent    %s\n", r.Parent)
	}
	if r.AFK != nil {
		fmt.Printf("afk       %s (run override)\n", r.AFK.String())
	}
	if r.Step != nil {
		s := r.Step
		line := s.Kind + " " + s.Phase
		if s.Attempt > 0 {
			line += fmt.Sprintf(", attempt %d", s.Attempt)
		}
		if s.Worker != "" {
			line += ", worker " + s.Worker + " (" + s.Provider + ")"
		}
		if s.Signal != "" {
			line += ", signal " + s.Signal
			if s.Key != "" {
				line += " key " + s.Key
			}
		}
		if s.Deadline != nil {
			line += ", times out " + s.Deadline.Local().Format("15:04:05")
		}
		fmt.Printf("step      %s\n", line)
		if s.Dir != "" {
			fmt.Printf("logs      %s\n", s.Dir)
		}
	}
	if r.Report != "" {
		fmt.Printf("report    %s\n", r.Report)
	}
	if len(r.Vars) > 0 {
		fmt.Printf("\nvars\n%s\n", indent(pretty(r.Vars)))
	}
	if len(kids) > 0 {
		fmt.Println("\nchildren")
		for _, k := range kids {
			fmt.Printf("  %s  %s  %s [%s]\n", k.ID, k.Workflow, k.State, k.Status)
		}
	}
	if len(cs) > 0 {
		fmt.Println("\ncontracts")
		for _, c := range cs {
			ans := ""
			if c.Resolution != nil {
				ans = fmt.Sprintf(" → %s (%s)", c.Resolution.Answer, c.Resolution.By)
			}
			fmt.Printf("  %s  %-8s %-9s %s%s\n", c.ID, c.Status, c.Kind, c.Title, ans)
		}
	}
	if len(auras) > 0 {
		a := auras[len(auras)-1]
		fmt.Printf("\nlatest aura (%s #%d)\n  goal: %s\n  next: %s\n", a.Step, a.Seq, a.Goal, a.Next)
	}
	if len(events) > 0 {
		fmt.Println("\nevents")
		start := 0
		if len(events) > 25 {
			start = len(events) - 25
		}
		for _, ev := range events[start:] {
			fmt.Println("  " + eventLine(ev, false))
		}
	}
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

func indent(s string) string { return "  " + strings.ReplaceAll(s, "\n", "\n  ") }

func eventLine(ev map[string]any, withRun bool) string {
	ts, _ := ev["ts"].(string)
	if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		ts = t.Local().Format("15:04:05")
	}
	typ, _ := ev["type"].(string)
	parts := []string{ts, fmt.Sprintf("%-20s", typ)}
	if withRun {
		if r, _ := ev["run"].(string); r != "" {
			parts = append(parts, short(r))
		}
	}
	if d, ok := ev["data"].(map[string]any); ok && len(d) > 0 {
		raw, _ := json.Marshal(d)
		s := string(raw)
		if len(s) > 160 {
			s = s[:157] + "..."
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, " ")
}

// --- signals -------------------------------------------------------------------------------

// cmdSystem sends a logged instruction to a run: see Engine.System.
func cmdSystem(args []string) error {
	usage := fail("USAGE", "usage: kami system <run> <text> [--state S] [--vars JSON] [--answer A]")
	if len(args) < 2 || len(args)%2 != 0 {
		return usage
	}
	body := map[string]any{"text": args[1]}
	for i := 2; i < len(args); i += 2 {
		switch args[i] {
		case "--state":
			body["state"] = args[i+1]
		case "--answer":
			body["answer"] = args[i+1]
		case "--vars":
			var vars map[string]any
			if err := json.Unmarshal([]byte(args[i+1]), &vars); err != nil {
				return fail("USAGE", "--vars: %v", err)
			}
			body["vars"] = vars
		default:
			return usage
		}
	}
	var r engine.Run
	data, err := call("POST", "/runs/"+url.PathEscape(args[0])+"/system", body, &r)
	if err != nil {
		return err
	}
	return emit(data, func() { fmt.Printf("system → %s (%s, %s)\n", r.ID, r.State, r.Status) })
}

func cmdSignal(args []string) error {
	f, err := parse(args, map[string]bool{"key": true, "data": true, "kekkai": true}, map[string]string{"k": "kekkai"})
	if err != nil {
		return err
	}
	if len(f.pos) != 1 {
		return fail("USAGE", "usage: kami signal pub <type> [--key K] [--data JSON] [--kekkai K]")
	}
	k, err := activeKekkai(f)
	if err != nil {
		return err
	}
	d, err := jsonObject(f.get("data"), "--data")
	if err != nil {
		return err
	}
	var out struct {
		Delivered []string `json:"delivered"`
	}
	data, err := call("POST", "/signal", map[string]any{"kekkai": k, "type": f.pos[0], "key": f.get("key"), "data": d}, &out)
	if err != nil {
		return err
	}
	return emit(data, func() {
		if len(out.Delivered) == 0 {
			fmt.Println("published; no run was waiting on it")
			return
		}
		fmt.Printf("delivered to %s\n", strings.Join(out.Delivered, ", "))
	})
}

// --- contracts -----------------------------------------------------------------------------

func cmdContract(sub string, args []string) error {
	switch sub {
	case "list":
		f, err := parse(args, map[string]bool{"status": true, "kekkai": true, "run": true}, map[string]string{"k": "kekkai"})
		if err != nil {
			return err
		}
		q := url.Values{}
		st := f.get("status")
		if st == "" {
			st = "open"
		}
		if st != "all" {
			q.Set("status", st)
		}
		if k := f.get("kekkai"); k != "" {
			q.Set("kekkai", k)
		}
		if r := f.get("run"); r != "" {
			q.Set("run", r)
		}
		var cs []engine.ContractView
		data, err := call("GET", "/contracts?"+q.Encode(), nil, &cs)
		if err != nil {
			return err
		}
		return emit(data, func() {
			if len(cs) == 0 {
				fmt.Println("no " + st + " contracts")
				return
			}
			rows := [][]string{}
			for _, c := range cs {
				mode := c.Effective
				if c.Lock {
					mode = "you only"
				}
				opts := strings.Join(c.Options, "|")
				if c.Kind == "ask" && c.Origin == "state" {
					opts = "<text>"
				}
				rows = append(rows, []string{c.ID, c.Kekkai + "/" + c.Workflow, c.Step, c.Kind, mode, opts, c.Title})
			}
			table([]string{"CONTRACT", "WORKFLOW", "STEP", "KIND", "MODE", "ANSWERS", "TITLE"}, rows)
		})
	case "show":
		if len(args) != 1 {
			return fail("USAGE", "usage: kami contract show <id>")
		}
		var c engine.ContractView
		data, err := call("GET", "/contracts/"+url.PathEscape(args[0]), nil, &c)
		if err != nil {
			return err
		}
		return emit(data, func() { showContract(&c) })
	case "resolve":
		f, err := parse(args, map[string]bool{"data": true, "field": true, "note": true}, map[string]string{"f": "field"})
		if err != nil {
			return err
		}
		if len(f.pos) < 1 || len(f.pos) > 2 {
			return fail("USAGE", "usage: kami contract resolve <id> [<answer>] [--field k=v]... [--data JSON] [--note T]")
		}
		d, err := jsonObject(f.get("data"), "--data")
		if err != nil {
			return err
		}
		if d == nil {
			d = map[string]any{}
		}
		for _, kv := range f.vals["field"] {
			k, v, err := parseKV(kv)
			if err != nil {
				return err
			}
			d[k] = v
		}
		answer := ""
		if len(f.pos) == 2 {
			answer = f.pos[1]
		}
		var c engine.ContractView
		data, err := call("POST", "/contracts/"+url.PathEscape(f.pos[0])+"/resolve", map[string]any{"answer": answer, "data": d, "note": f.get("note")}, &c)
		if err != nil {
			return err
		}
		return emit(data, func() { fmt.Printf("resolved %s → %s\n", c.ID, c.Resolution.Answer) })
	case "request":
		f, err := parse(args, map[string]bool{"kind": true, "title": true, "body": true, "options": true, "default": true, "fields": true}, nil)
		if err != nil {
			return err
		}
		if os.Getenv("KAMI_TOKEN") == "" {
			return fail("USAGE", "kami contract request runs inside a worker (KAMI_TOKEN is not set)")
		}
		req := map[string]any{"kind": f.get("kind"), "title": f.get("title"), "body": f.get("body"), "default": f.get("default")}
		if o := f.get("options"); o != "" {
			req["options"] = strings.Split(o, ",")
		}
		if fs := f.get("fields"); fs != "" {
			m := map[string]string{}
			for _, p := range strings.Split(fs, ",") {
				k, t, _ := strings.Cut(p, "=")
				m[k] = t
			}
			req["fields"] = m
		}
		var out map[string]any
		data, err := call("POST", "/worker/contract", req, &out)
		if err != nil {
			return err
		}
		return emit(data, func() {
			fmt.Printf("contract %v opened. Save an aura (kami state snapshot) and exit; you will be woken with the answer.\n", out["id"])
		})
	}
	return fail("USAGE", "usage: kami contract list|show|resolve|request")
}

func showContract(c *engine.ContractView) {
	fmt.Printf("contract  %s  [%s]\nrun       %s  (%s/%s, step %s)\nkind      %s  from %s\n", c.ID, c.Status, c.RunID, c.Kekkai, c.Workflow, c.Step, c.Kind, c.Origin)
	mode := c.Effective + "  (" + c.Why + ")"
	fmt.Printf("mode      %s\n", mode)
	if c.Deadline != nil {
		fmt.Printf("deadline  %s  (resolver %s runs then)\n", c.Deadline.Local().Format("15:04:05"), c.Resolver)
	}
	fmt.Printf("\n%s\n", c.Title)
	if c.Body != "" {
		fmt.Printf("\n%s\n", c.Body)
	}
	fmt.Println()
	switch {
	case c.Kind == "ask" && c.Origin == "state":
		fmt.Println("answer: free text")
	case len(c.Options) > 0:
		fmt.Printf("answers: %s\n", strings.Join(c.Options, ", "))
	}
	if len(c.Fields) > 0 {
		var fs []string
		for k, t := range c.Fields {
			fs = append(fs, k+" ("+t+")")
		}
		sort.Strings(fs)
		fmt.Printf("fields: %s\n", strings.Join(fs, ", "))
	}
	if c.Default != "" {
		fmt.Printf("default: %s\n", c.Default)
	}
	if c.Resolution != nil {
		fmt.Printf("\nresolved → %s by %s at %s\n", c.Resolution.Answer, c.Resolution.By, c.Resolution.At.Local().Format("15:04:05"))
		if len(c.Resolution.Data) > 0 {
			fmt.Println(indent(pretty(c.Resolution.Data)))
		}
		if c.Resolution.Note != "" {
			fmt.Println("note: " + c.Resolution.Note)
		}
	} else if c.Status == "open" {
		hint := "<answer>"
		if len(c.Options) > 0 {
			hint = c.Options[0]
		} else if c.Kind == "ask" {
			hint = `"<your answer>"`
		}
		fmt.Printf("\nkami contract resolve %s %s", c.ID, hint)
		if len(c.Fields) > 0 {
			fmt.Print(" --field name=value")
		}
		fmt.Println()
	}
}

// --- afk --------------------------------------------------------------------------------------

func cmdAFK(sub string, args []string) error {
	switch sub {
	case "show":
		if len(args) != 1 {
			return fail("USAGE", "usage: kami afk show <run>")
		}
		var v map[string]any
		data, err := call("GET", "/afk/"+url.PathEscape(args[0]), nil, &v)
		if err != nil {
			return err
		}
		return emit(data, func() { printAFK(v) })
	case "set":
		f, err := parse(args, map[string]bool{"run": true, "contract": true}, nil)
		if err != nil {
			return err
		}
		if len(f.pos) != 1 {
			return fail("USAGE", "usage: kami afk set <attended|afk|grace=10m> (--run ID | --contract ID)")
		}
		var v map[string]any
		data, err := call("POST", "/afk", map[string]any{"mode": f.pos[0], "run": f.get("run"), "contract": f.get("contract")}, &v)
		if err != nil {
			return err
		}
		return emit(data, func() {
			if f.get("contract") != "" {
				fmt.Printf("contract %v now %v\n", v["id"], v["effective_mode"])
				return
			}
			printAFK(v)
		})
	}
	return fail("USAGE", "usage: kami afk show|set")
}

func printAFK(v map[string]any) {
	fmt.Printf("run %v\n  workflow default: %v\n", v["run"], v["workflow_default"])
	if o, _ := v["run_override"].(string); o != "" {
		fmt.Printf("  run override:     %s\n", o)
	}
	if cs, _ := v["open_contracts"].([]any); len(cs) > 0 {
		for _, x := range cs {
			c, _ := x.(map[string]any)
			fmt.Printf("  %v  %v  (%v)  %v\n", c["id"], c["effective_mode"], c["mode_source"], c["title"])
		}
	}
}

// --- events -----------------------------------------------------------------------------------

func cmdEvents(args []string) error {
	f, err := parse(args, map[string]bool{"follow": false, "since": true, "type": true, "kekkai": true, "run": true}, map[string]string{"f": "follow", "k": "kekkai"})
	if err != nil {
		return err
	}
	q := url.Values{}
	for _, k := range []string{"since", "type", "kekkai", "run"} {
		if v := f.get(k); v != "" {
			q.Set(k, v)
		}
	}
	if f.has("follow") {
		q.Set("follow", "1")
	}
	req, _ := http.NewRequest("GET", "http://kami/events?"+q.Encode(), nil)
	resp, err := client().Do(req)
	if err != nil {
		return fail("DAEMON", "daemon not reachable at %s: %v", sockPath(), err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if jsonOut {
			os.Stdout.Write(append(line, '\n'))
			continue
		}
		var ev map[string]any
		if json.Unmarshal(line, &ev) == nil {
			fmt.Println(eventLine(ev, true))
		}
	}
	return nil
}

// --- worker side ------------------------------------------------------------------------------

func cmdStepDone(args []string) error {
	f, err := parse(args, map[string]bool{"outcome": true, "vars": true}, map[string]string{"o": "outcome"})
	if err != nil {
		return err
	}
	if os.Getenv("KAMI_TOKEN") == "" {
		return fail("USAGE", "kami step done runs inside a worker (KAMI_TOKEN is not set)")
	}
	if f.get("outcome") == "" {
		return fail("USAGE", "usage: kami step done --outcome O [--vars JSON]")
	}
	vars, err := jsonObject(f.get("vars"), "--vars")
	if err != nil {
		return err
	}
	data, err := call("POST", "/worker/done", map[string]any{"outcome": f.get("outcome"), "vars": vars}, nil)
	if err != nil {
		return err
	}
	return emit(data, func() { fmt.Printf("outcome %s recorded; exit now\n", f.get("outcome")) })
}

func cmdState(sub string, args []string) error {
	switch sub {
	case "snapshot":
		if os.Getenv("KAMI_TOKEN") == "" {
			return fail("USAGE", "kami state snapshot runs inside a worker (KAMI_TOKEN is not set)")
		}
		raw, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		var a map[string]any
		if err := json.Unmarshal(raw, &a); err != nil {
			return fail("USAGE", "aura on stdin must be a JSON object: %v", err)
		}
		var out engine.Aura
		data, err := call("POST", "/worker/snapshot", a, &out)
		if err != nil {
			return err
		}
		return emit(data, func() { fmt.Printf("aura #%d saved for %s\n", out.Seq, out.Step) })
	case "show":
		id := os.Getenv("KAMI_RUN")
		if len(args) == 1 {
			id = args[0]
		}
		if id == "" {
			return fail("USAGE", "usage: kami state show <run>")
		}
		var v struct {
			engine.Run
			Auras []engine.Aura `json:"auras"`
		}
		if _, err := call("GET", "/runs/"+url.PathEscape(id), nil, &v); err != nil {
			return err
		}
		out := map[string]any{"run": v.ID, "state": v.State, "status": v.Status, "vars": v.Vars}
		if len(v.Auras) > 0 {
			out["aura"] = v.Auras[len(v.Auras)-1]
		}
		return emitValue(out, func() { fmt.Println(pretty(out)) })
	}
	return fail("USAGE", "usage: kami state snapshot|show")
}

// --- memory ---------------------------------------------------------------------------

func cmdMemory(sub string, args []string) error {
	f, err := parse(args, map[string]bool{"about": true, "scope": true, "q": true}, nil)
	if err != nil {
		return err
	}
	switch sub {
	case "save":
		fact := strings.Join(f.pos, " ")
		if fact == "" {
			return fail("USAGE", "usage: kami memory save [--about WHO] [--scope S] <fact...>")
		}
		var m map[string]any
		data, err := call("POST", "/memory", map[string]any{"about": f.get("about"), "fact": fact, "scope": f.get("scope")}, &m)
		if err != nil {
			return err
		}
		return emit(data, func() { fmt.Printf("remembered %s (%s)\n", m["id"], m["scope"]) })
	case "load":
		var ms []map[string]any
		q := url.Values{}
		if v := f.get("about"); v != "" {
			q.Set("about", v)
		}
		if v := f.get("q"); v != "" {
			q.Set("q", v)
		}
		data, err := call("GET", "/memory?"+q.Encode(), nil, &ms)
		if err != nil {
			return err
		}
		return emit(data, func() {
			if len(ms) == 0 {
				fmt.Println("nothing remembered")
			}
			for _, m := range ms {
				about := m["about"]
				if about == "" {
					about = "-"
				}
				fmt.Printf("%s  %s  [%s]  %s\n", m["id"], about, m["scope"], m["fact"])
			}
		})
	case "forget":
		if len(f.pos) != 1 {
			return fail("USAGE", "usage: kami memory forget <id>")
		}
		data, err := call("DELETE", "/memory/"+url.PathEscape(f.pos[0]), nil, nil)
		if err != nil {
			return err
		}
		return emit(data, func() { fmt.Println("forgot " + f.pos[0]) })
	}
	return fail("USAGE", "usage: kami memory save|load|forget")
}
