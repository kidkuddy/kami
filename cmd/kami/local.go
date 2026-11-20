package main

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"kami/internal/engine"
	"kami/internal/eventbus"
	"kami/internal/kamui"
	"kami/internal/kekkai"
	"kami/internal/state"
)

// --- daemon -------------------------------------------------------------------------

func cmdDaemon(args []string) error {
	sub := ""
	if len(args) > 0 {
		sub = args[0]
	}
	switch sub {
	case "", "run", "--foreground":
		return serve()
	case "start":
		return daemonStart()
	case "stop":
		return daemonStop()
	case "status":
		return cmdStatus()
	}
	return fail("USAGE", "usage: kami daemon [start|stop|status]")
}

func serve() error {
	h := home()
	if err := os.MkdirAll(h, 0o700); err != nil {
		return err
	}
	sock := sockPath()
	if len(sock) > 103 {
		return fail("USAGE", "socket path %s is %d bytes; unix sockets allow 103. Set KAMI_SOCK or a shorter KAMI_HOME", sock, len(sock))
	}
	lock, err := os.OpenFile(filepath.Join(h, "daemon.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fail("CONFLICT", "another kami daemon holds %s", filepath.Join(h, "daemon.lock"))
	}
	pidFile := filepath.Join(h, "daemon.pid")
	os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o600)
	defer os.Remove(pidFile)

	bus, err := eventbus.Open(filepath.Join(h, "events.db"))
	if err != nil {
		return err
	}
	defer bus.Close()
	cfg, err := kekkai.LoadConfig(h)
	if err != nil {
		return err
	}
	ns := os.Getenv("KAMI_MEMENTO_NS")
	if ns == "" {
		ns = cfg.Memento
	}
	if ns == "" {
		ns = "kami"
	}
	mem := &state.Memento{NS: ns, Bin: os.Getenv("KAMI_MEMENTO_BIN")}
	bin, _ := os.Executable()
	os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		return err
	}
	os.Chmod(sock, 0o600)
	defer os.Remove(sock)

	eng := engine.New(h, mem, bus, bin, sock)
	var ready atomic.Bool
	srv := &http.Server{Handler: engine.Booting(&ready, eng.Handler()), ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }() // serving first: clients see "booting", not a dead socket
	if err := eng.Boot(); err != nil {
		srv.Close()
		return fail("INTERNAL", "boot: %v", err)
	}
	ready.Store(true)
	eng.WatchNetwork()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	fmt.Fprintf(os.Stderr, "kami daemon %s: pid %d, socket %s, memento ns %s\n", engine.Version, os.Getpid(), sock, ns)
	select {
	case <-stop:
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			eng.Close()
			return err
		}
	}
	eng.Close()
	srv.Close()
	return nil
}

func daemonStart() error {
	if _, err := call("GET", "/status", nil, nil); err == nil {
		fmt.Println("kami daemon already running")
		return nil
	}
	h := home()
	if err := os.MkdirAll(h, 0o700); err != nil {
		return err
	}
	logf, err := os.OpenFile(filepath.Join(h, "daemon.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer logf.Close()
	bin, _ := os.Executable()
	cmd := exec.Command(bin, "daemon")
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	cmd.Process.Release()
	for i := 0; i < 100; i++ {
		time.Sleep(100 * time.Millisecond)
		if _, err := call("GET", "/status", nil, nil); err == nil {
			fmt.Printf("kami daemon started (log %s)\n", filepath.Join(h, "daemon.log"))
			return nil
		}
	}
	return fail("DAEMON", "daemon did not come up in 10s; see %s", filepath.Join(h, "daemon.log"))
}

func daemonStop() error {
	raw, err := os.ReadFile(filepath.Join(home(), "daemon.pid"))
	if err != nil {
		fmt.Println("kami daemon not running")
		return nil
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
	if pid <= 0 || syscall.Kill(pid, syscall.SIGTERM) != nil {
		fmt.Println("kami daemon not running")
		return nil
	}
	for i := 0; i < 100; i++ {
		time.Sleep(100 * time.Millisecond)
		if syscall.Kill(pid, 0) != nil {
			fmt.Println("kami daemon stopped")
			return nil
		}
	}
	return fail("DAEMON", "daemon pid %d did not stop in 10s", pid)
}

// --- kekkai -----------------------------------------------------------------------------

func cmdKekkai(sub string, args []string) error {
	h := home()
	switch sub {
	case "list":
		names, err := kekkai.List(h)
		if err != nil {
			return err
		}
		cfg, _ := kekkai.LoadConfig(h)
		type item struct {
			Name, Description string
			Active            bool
		}
		var out []item
		for _, n := range names {
			k, err := kekkai.Load(h, n)
			d := ""
			if err != nil {
				d = "(invalid: " + err.Error() + ")"
			} else {
				d = k.Description
			}
			out = append(out, item{n, d, cfg.Active == n})
		}
		return emitValue(out, func() {
			if len(out) == 0 {
				fmt.Printf("no kekkai under %s\n", kekkai.Root(h))
			}
			for _, it := range out {
				mark := " "
				if it.Active {
					mark = "*"
				}
				fmt.Printf("%s %-12s %s\n", mark, it.Name, it.Description)
			}
		})
	case "current":
		cfg, err := kekkai.LoadConfig(h)
		if err != nil {
			return err
		}
		return emitValue(map[string]string{"active": cfg.Active}, func() {
			if cfg.Active == "" {
				fmt.Println("no active kekkai")
				return
			}
			fmt.Println(cfg.Active)
		})
	case "switch":
		if len(args) != 1 {
			return fail("USAGE", "usage: kami kekkai switch <name>")
		}
		if _, err := kekkai.Load(h, args[0]); err != nil {
			return fail("NOT_FOUND", "%v", err)
		}
		cfg, err := kekkai.LoadConfig(h)
		if err != nil {
			return err
		}
		prev := cfg.Active
		cfg.Active = args[0]
		if err := kekkai.SaveConfig(h, cfg); err != nil {
			return err
		}
		return emitValue(map[string]string{"active": cfg.Active, "previous": prev}, func() {
			fmt.Printf("active kekkai: %s (CLI default only; every kekkai keeps running)\n", cfg.Active)
		})
	case "validate":
		names := args
		if len(names) == 0 {
			var err error
			if names, err = kekkai.List(h); err != nil {
				return err
			}
		}
		report := map[string][]string{}
		bad := 0
		for _, n := range names {
			p := validateKekkai(h, n)
			report[n] = p
			bad += len(p)
		}
		if jsonOut && bad > 0 {
			var all []string
			for _, n := range names {
				for _, x := range report[n] {
					all = append(all, n+": "+x)
				}
			}
			return fail("INVALID", "%s", strings.Join(all, "\n"))
		}
		err := emitValue(report, func() {
			for _, n := range names {
				if len(report[n]) == 0 {
					fmt.Printf("ok   %s\n", n)
					continue
				}
				fmt.Printf("FAIL %s\n  %s\n", n, strings.Join(report[n], "\n  "))
			}
		})
		if err == nil && bad > 0 {
			return &silentErr{exitInvalid}
		}
		return err
	case "reload":
		var out map[string]string
		data, err := call("POST", "/triggers/reload", map[string]any{}, &out)
		if err != nil {
			return err
		}
		return emit(data, func() {
			if len(out) == 0 {
				fmt.Println("reloaded; no cron triggers")
			}
			ids := make([]string, 0, len(out))
			for id := range out {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				fmt.Printf("%s  next %s\n", id, out[id])
			}
		})
	case "pause":
		f, err := parse(args, map[string]bool{"until": true, "except": true}, nil)
		if err != nil || len(f.pos) != 1 {
			return fail("USAGE", "usage: kami kekkai pause <name> [--until HH:MM|RFC3339] [--except wf,wf]")
		}
		body := map[string]any{}
		if u := f.get("until"); u != "" {
			t, err := untilTime(u, time.Now())
			if err != nil {
				return err
			}
			body["until"] = t
		}
		if x := f.get("except"); x != "" {
			body["except"] = strings.Split(x, ",")
		}
		var p struct {
			Until  *time.Time `json:"until"`
			Except []string   `json:"except"`
		}
		data, err := call("POST", "/kekkai/"+url.PathEscape(f.pos[0])+"/pause", body, &p)
		if err != nil {
			return err
		}
		return emit(data, func() {
			until := "until `kami kekkai resume " + f.pos[0] + "`"
			if p.Until != nil {
				until = "until " + p.Until.Local().Format("Mon 15:04")
			}
			fmt.Printf("paused %s %s", f.pos[0], until)
			if len(p.Except) > 0 {
				fmt.Printf(" (still running: %s)", strings.Join(p.Except, ", "))
			}
			fmt.Println()
		})
	case "allow", "hold":
		if len(args) != 2 {
			return fail("USAGE", "usage: kami kekkai allow|hold <name> <thread_ts|run id>")
		}
		var out struct {
			Pause    struct{ Allow []string }
			Released []string
		}
		data, err := call("POST", "/kekkai/"+url.PathEscape(args[0])+"/allow", map[string]any{"thread": args[1], "on": sub == "allow"}, &out)
		if err != nil {
			return err
		}
		return emit(data, func() {
			fmt.Printf("%s %s %s: %d runs released; allowed now: %s\n", args[0], sub, args[1], len(out.Released), strings.Join(out.Pause.Allow, ", "))
		})
	case "resume":
		if len(args) != 1 {
			return fail("USAGE", "usage: kami kekkai resume <name>")
		}
		var out struct{ Resumed []string }
		data, err := call("POST", "/kekkai/"+url.PathEscape(args[0])+"/resume", map[string]any{}, &out)
		if err != nil {
			return err
		}
		return emit(data, func() { fmt.Printf("resumed %s: %d held runs continue\n", args[0], len(out.Resumed)) })
	}
	return fail("USAGE", "usage: kami kekkai list|current|switch|validate|reload|pause|resume|allow|hold")
}

// silentErr exits with a code after output was already printed.
type silentErr struct{ code int }

func (e *silentErr) Error() string { return "" }

func validateKekkai(h, name string) []string {
	k, err := kekkai.Load(h, name)
	if err != nil {
		return []string{err.Error()}
	}
	var p []string
	if k.Prompt != "" {
		if _, err := os.Stat(filepath.Join(k.Dir, k.Prompt)); err != nil {
			p = append(p, "prompt file "+k.Prompt+" not found")
		}
	}
	for _, s := range k.Skills {
		if _, err := os.Stat(filepath.Join(k.Dir, "skills", s, "SKILL.md")); err != nil {
			p = append(p, "skill "+s+" not found at skills/"+s+"/SKILL.md")
		}
	}
	for _, r := range k.CwdRoots {
		if _, err := os.Stat(r); err != nil {
			p = append(p, "cwd root "+r+" does not exist")
		}
	}
	for n, w := range k.Workers {
		switch w.Provider {
		case "claude", "codex":
		case "fake":
			if w.Command == "" {
				p = append(p, "worker "+n+": fake provider needs command")
			}
		default:
			p = append(p, fmt.Sprintf("worker %s: unknown provider %q", n, w.Provider))
		}
	}
	files, _ := filepath.Glob(filepath.Join(k.Dir, "workflows", "*.star"))
	for _, f := range files {
		wf := strings.TrimSuffix(filepath.Base(f), ".star")
		w, err := kamui.Load(kamui.WorkflowPath(wf), kamui.DirReader(k.Dir))
		if err != nil {
			p = append(p, wf+": "+err.Error())
			continue
		}
		if w.Name != wf {
			p = append(p, fmt.Sprintf("%s: file is %s.star but workflow(name = %q)", wf, wf, w.Name))
		}
		for _, x := range kamui.Validate(w, k) {
			p = append(p, wf+": "+x)
		}
	}
	return p
}

// --- workflows ------------------------------------------------------------------------------

// openWorkflow resolves a .star path or a workflow name in the active kekkai.
func openWorkflow(arg string, f flags) (*kekkai.Kekkai, *kamui.Workflow, error) {
	if strings.HasSuffix(arg, ".star") {
		abs, err := filepath.Abs(arg)
		if err != nil {
			return nil, nil, err
		}
		root := filepath.Dir(abs)
		if filepath.Base(root) == "workflows" {
			root = filepath.Dir(root)
		}
		rel, _ := filepath.Rel(root, abs)
		var k *kekkai.Kekkai
		if _, err := os.Stat(filepath.Join(root, "kekkai.yaml")); err == nil {
			if k, err = kekkai.LoadDir(root, ""); err != nil {
				return nil, nil, fail("INVALID", "%v", err)
			}
		}
		w, err := kamui.Load(filepath.ToSlash(rel), kamui.DirReader(root))
		if err != nil {
			return nil, nil, fail("INVALID", "%v", err)
		}
		return k, w, nil
	}
	name, err := activeKekkai(f)
	if err != nil {
		return nil, nil, err
	}
	k, err := kekkai.Load(home(), name)
	if err != nil {
		return nil, nil, fail("NOT_FOUND", "%v", err)
	}
	w, err := kamui.Load(kamui.WorkflowPath(arg), kamui.DirReader(k.Dir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, fail("NOT_FOUND", "workflow %q not found in kekkai %s", arg, name)
		}
		return nil, nil, fail("INVALID", "%v", err)
	}
	return k, w, nil
}

func cmdWorkflow(sub string, args []string) error {
	f, err := parse(args, map[string]bool{"kekkai": true}, map[string]string{"k": "kekkai"})
	if err != nil {
		return err
	}
	switch sub {
	case "list":
		name, err := activeKekkai(f)
		if err != nil {
			return err
		}
		k, err := kekkai.Load(home(), name)
		if err != nil {
			return fail("NOT_FOUND", "%v", err)
		}
		files, _ := filepath.Glob(filepath.Join(k.Dir, "workflows", "*.star"))
		type item struct {
			Name     string   `json:"name"`
			States   int      `json:"states"`
			Entries  []string `json:"entries,omitempty"`
			Triggers []string `json:"triggers,omitempty"`
			Error    string   `json:"error,omitempty"`
		}
		var out []item
		for _, file := range files {
			wf := strings.TrimSuffix(filepath.Base(file), ".star")
			w, err := kamui.Load(kamui.WorkflowPath(wf), kamui.DirReader(k.Dir))
			if err != nil {
				out = append(out, item{Name: wf, Error: err.Error()})
				continue
			}
			it := item{Name: wf, States: len(w.States)}
			for e := range w.Entries {
				it.Entries = append(it.Entries, e)
			}
			sort.Strings(it.Entries)
			for _, t := range w.Triggers {
				it.Triggers = append(it.Triggers, t.Cron)
			}
			out = append(out, it)
		}
		return emitValue(out, func() {
			for _, it := range out {
				if it.Error != "" {
					fmt.Printf("%-16s  error: %s\n", it.Name, it.Error)
					continue
				}
				extra := ""
				if len(it.Entries) > 0 {
					extra += "  entries: " + strings.Join(it.Entries, ", ")
				}
				if len(it.Triggers) > 0 {
					extra += "  cron: " + strings.Join(it.Triggers, ", ")
				}
				fmt.Printf("%-16s  %2d states%s\n", it.Name, it.States, extra)
			}
		})
	case "validate", "dry-run":
		if len(f.pos) != 1 {
			return fail("USAGE", "usage: kami workflow %s <file.star|name> [--kekkai K]", sub)
		}
		k, w, err := openWorkflow(f.pos[0], f)
		if err != nil {
			return err
		}
		problems := kamui.Validate(w, k)
		if problems == nil {
			problems = []string{}
		}
		edges := dryRun(w)
		out := map[string]any{"workflow": w.Name, "states": len(w.States), "hash": w.Hash, "problems": problems, "with_kekkai": k != nil}
		if sub == "dry-run" {
			out["edges"] = edges
		}
		if jsonOut && len(problems) > 0 {
			return fail("INVALID", "%s is invalid:\n  %s", w.Name, strings.Join(problems, "\n  "))
		}
		err = emitValue(out, func() {
			if sub == "dry-run" {
				for _, e := range edges {
					fmt.Println(e)
				}
				fmt.Println()
			}
			scope := "without a kekkai (workers, skills and paths not checked)"
			if k != nil {
				scope = "against kekkai " + k.Name
			}
			if len(problems) == 0 {
				fmt.Printf("ok  %s: %d states, validated %s\n", w.Name, len(w.States), scope)
				return
			}
			fmt.Printf("FAIL %s, validated %s:\n  %s\n", w.Name, scope, strings.Join(problems, "\n  "))
		})
		if err == nil && len(problems) > 0 {
			return &silentErr{exitInvalid}
		}
		return err
	}
	return fail("USAGE", "usage: kami workflow list|validate|dry-run")
}

// dryRun walks every edge from start and the entries, breadth first.
func dryRun(w *kamui.Workflow) []string {
	var lines []string
	seen := map[string]bool{}
	queue := []string{w.Start}
	for _, e := range w.Entries {
		queue = append(queue, e.State)
	}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		st, ok := w.States[n]
		if !ok || seen[n] {
			continue
		}
		seen[n] = true
		s := st.Step
		label := s.Kind
		if s.HumanOnly() && s.IsContract() {
			label += " (you only)"
		}
		switch {
		case s.Kind == kamui.KChoose:
			targets, _ := kamui.ChooseTargets(w, s.Fn)
			lines = append(lines, fmt.Sprintf("%-20s %-18s → %s", n, label, strings.Join(targets, " | ")))
			queue = append(queue, targets...)
		case s.IsTerminal():
			lines = append(lines, fmt.Sprintf("%-20s %-18s ■", n, label))
		default:
			for i, o := range st.Order {
				name := n
				if i > 0 {
					name, label = "", ""
				}
				lines = append(lines, fmt.Sprintf("%-20s %-18s %s → %s", name, label, o, st.Outcomes[o]))
				queue = append(queue, st.Outcomes[o])
			}
		}
	}
	return lines
}

// untilTime reads --until: RFC3339, or HH:MM meaning its next occurrence (tomorrow if passed).
func untilTime(s string, from time.Time) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	hm, err := time.ParseInLocation("15:04", s, from.Location())
	if err != nil {
		return time.Time{}, fail("USAGE", "--until wants HH:MM or RFC3339, got %q", s)
	}
	t := time.Date(from.Year(), from.Month(), from.Day(), hm.Hour(), hm.Minute(), 0, 0, from.Location())
	if !t.After(from) {
		t = t.AddDate(0, 0, 1)
	}
	return t, nil
}
