// Package e2e drives a real kami daemon through the synthetic workflows in
// testdata/kekkai. Each test gets its own KAMI_HOME and memento store; nothing
// touches the user's ~/.kami or memento config. Workers are fake shell
// scripts, so no LLM is called.
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"kami/internal/engine"
	"kami/internal/state"
)

var kamiBin string

func TestMain(m *testing.M) {
	if _, err := exec.LookPath("memento"); err != nil {
		fmt.Println("e2e: memento not on PATH; skipping")
		os.Exit(0)
	}
	dir, err := os.MkdirTemp("", "kami-bin-")
	if err != nil {
		panic(err)
	}
	kamiBin = filepath.Join(dir, "kami")
	args := []string{"build", "-o", kamiBin}
	if os.Getenv("KAMI_E2E_RACE") != "" {
		args = append(args, "-race") // the daemon runs with the race detector
	}
	build := exec.Command("go", append(args, "kami/cmd/kami")...)
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Println("e2e: build failed:", err)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type env struct {
	t      *testing.T
	root   string
	home   string
	vars   []string
	daemon *exec.Cmd
	log    *os.File
}

// newEnv makes a fresh home with the named test kekkai and starts a daemon.
func newEnv(t *testing.T, kekkais ...string) *env {
	t.Helper()
	t.Parallel()
	root, err := os.MkdirTemp("/tmp", "kami-e2e-") // short: unix socket paths cap at 103 bytes
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, root: root, home: filepath.Join(root, "home")}
	t.Cleanup(func() {
		e.stop()
		if t.Failed() {
			t.Logf("kept %s for inspection (daemon log: %s)", root, filepath.Join(root, "daemon.log"))
			return
		}
		os.RemoveAll(root)
	})
	if len(kekkais) == 0 {
		kekkais = []string{"test"}
	}
	for _, k := range kekkais {
		copyDir(t, filepath.Join("testdata", "kekkai", k), filepath.Join(e.home, "kekkai", k))
	}
	if raw, err := os.ReadFile(filepath.Join(e.home, "kekkai", "test", "kekkai.yaml")); err == nil {
		os.WriteFile(filepath.Join(e.home, "kekkai", "test", "kekkai.yaml"), bytes.ReplaceAll(raw, []byte("__ROOT__"), []byte(root)), 0o600)
	}
	os.MkdirAll(filepath.Join(root, "allowed"), 0o755)
	os.WriteFile(filepath.Join(e.home, "config.yaml"), []byte("active: "+kekkais[0]+"\n"), 0o600)
	base, err := state.TestStore(filepath.Join(root, "store"))
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range base {
		if strings.HasPrefix(kv, "KAMI_") {
			continue
		}
		e.vars = append(e.vars, kv)
	}
	e.vars = append(e.vars, "KAMI_HOME="+e.home, "DAEMON_SECRET=d1")
	e.log, _ = os.OpenFile(filepath.Join(root, "daemon.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	e.start()
	return e
}

func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, _ := d.Info()
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, raw, info.Mode().Perm())
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (e *env) start() {
	e.t.Helper()
	cmd := exec.Command(kamiBin, "daemon")
	cmd.Env = e.vars
	cmd.Stdout, cmd.Stderr = e.log, e.log
	if err := cmd.Start(); err != nil {
		e.t.Fatal(err)
	}
	e.daemon = cmd
	for i := 0; i < 200; i++ {
		if _, code, _ := e.kami("status"); code == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.t.Fatalf("daemon did not start; log at %s", filepath.Join(e.root, "daemon.log"))
}

// crash kills the daemon with SIGKILL: no cleanup, like a power cut.
func (e *env) crash() {
	if e.daemon != nil {
		e.daemon.Process.Kill()
		e.daemon.Wait()
		e.daemon = nil
	}
}

func (e *env) stop() {
	if e.daemon != nil {
		e.daemon.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { e.daemon.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			e.daemon.Process.Kill()
		}
		e.daemon = nil
	}
}

// kami runs the CLI with --json and returns data, exit code, error message.
func (e *env) kami(args ...string) (json.RawMessage, int, string) {
	cmd := exec.Command(kamiBin, append([]string{"--json"}, args...)...)
	cmd.Env = e.vars
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	}
	var env engine.Envelope
	if json.Unmarshal(out.Bytes(), &env) != nil {
		return nil, code, out.String()
	}
	if env.Error != nil {
		return nil, code, env.Error.Code + ": " + env.Error.Message
	}
	return env.Data, code, ""
}

func (e *env) must(args ...string) json.RawMessage {
	e.t.Helper()
	data, code, msg := e.kami(args...)
	if code != 0 {
		e.t.Fatalf("kami %s: exit %d: %s", strings.Join(args, " "), code, msg)
	}
	return data
}

func (e *env) refused(code int, want string, args ...string) {
	e.t.Helper()
	_, got, msg := e.kami(args...)
	if got != code || !strings.Contains(msg, want) {
		e.t.Fatalf("kami %s: want exit %d with %q, got exit %d: %s", strings.Join(args, " "), code, want, got, msg)
	}
}

type runView struct {
	engine.Run
	Auras     []engine.Aura         `json:"auras"`
	Contracts []engine.ContractView `json:"contracts"`
	Children  []engine.RunBrief     `json:"children"`
	Events    []map[string]any      `json:"events"`
}

func (e *env) start_(args ...string) string {
	e.t.Helper()
	var r engine.Run
	json.Unmarshal(e.must(append([]string{"run", "start"}, args...)...), &r)
	if r.ID == "" {
		e.t.Fatalf("run start %v returned no id", args)
	}
	return r.ID
}

func (e *env) show(id string) runView {
	e.t.Helper()
	var v runView
	json.Unmarshal(e.must("run", "show", id), &v)
	return v
}

// waitRun polls until cond holds or the timeout passes.
func (e *env) waitRun(id string, timeout time.Duration, what string, cond func(runView) bool) runView {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		v := e.show(id)
		if cond(v) {
			return v
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("run %s: timed out waiting for %s; now state %s status %s\nevents: %s", id, what, v.State, v.Status, eventTypes(v))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (e *env) waitStatus(id, status string, timeout time.Duration) runView {
	e.t.Helper()
	return e.waitRun(id, timeout, "status "+status, func(v runView) bool { return v.Status == status })
}

func (e *env) openContract(id string, timeout time.Duration) engine.ContractView {
	e.t.Helper()
	var c engine.ContractView
	e.waitRun(id, timeout, "an open contract", func(v runView) bool {
		for _, x := range v.Contracts {
			if x.Status == "open" {
				c = x
				return true
			}
		}
		return false
	})
	return c
}

func eventTypes(v runView) string {
	var out []string
	for _, ev := range v.Events {
		out = append(out, fmt.Sprint(ev["type"]))
	}
	return strings.Join(out, " ")
}

func hasEvent(v runView, typ string) bool {
	for _, ev := range v.Events {
		if ev["type"] == typ {
			return true
		}
	}
	return false
}

func expect(t *testing.T, what string, got, want any) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

const short = 10 * time.Second

// --- 1. linear ---------------------------------------------------------------------

func TestLinear(t *testing.T) {
	e := newEnv(t)
	id := e.start_("linear", "--input", "topic=walls")
	v := e.waitStatus(id, "done", short)
	expect(t, "report", v.Report, "finished walls ok ok")
	expect(t, "outcome", v.Outcome, "finished")
}

// --- 2. gate -----------------------------------------------------------------------

func TestGate(t *testing.T) {
	e := newEnv(t)
	id := e.start_("gate")
	c := e.openContract(id, short)
	expect(t, "options", strings.Join(c.Options, ","), "ship,hold")
	e.refused(4, "not one of", "contract", "resolve", c.ID, "maybe")
	e.must("contract", "resolve", c.ID[:12], "ship") // prefix ids work
	v := e.waitStatus(id, "done", short)
	expect(t, "report", v.Report, "shipped")
	expect(t, "vars.ship_it", v.Vars["ship_it"], "ship")
	e.refused(5, "already resolved", "contract", "resolve", c.ID, "hold")
}

// --- 3. blocker → sleep → wake from aura -------------------------------------------

func TestBlockerWake(t *testing.T) {
	e := newEnv(t)
	id := e.start_("blocker-wake")
	v := e.waitStatus(id, "sleeping", short)
	if len(v.Contracts) != 1 || v.Contracts[0].Origin != "worker" {
		t.Fatalf("want one worker contract, got %+v", v.Contracts)
	}
	c := v.Contracts[0]
	expect(t, "title", c.Title, "Which color?")
	e.must("contract", "resolve", c.ID, "blue")
	v = e.waitStatus(id, "done", short)
	expect(t, "report", v.Report, "blue aura=yes")
	starts := 0
	for _, ev := range v.Events {
		if ev["type"] == "worker.started" {
			starts++
		}
	}
	expect(t, "worker attempts", starts, 2)
}

// --- 4. afk grace ------------------------------------------------------------------

func TestAFKGrace(t *testing.T) {
	e := newEnv(t)
	// (a) nobody answers within 2s → the resolver does
	a := e.start_("afk-grace")
	c := e.openContract(a, short)
	expect(t, "effective mode", c.Effective, "grace=2s")
	v := e.waitStatus(a, "done", short)
	expect(t, "report", v.Report, "approved")
	expect(t, "resolved by", v.Contracts[0].Resolution.By, "resolver")

	// (b) a human answers inside the window → the resolver never runs
	b := e.start_("afk-grace-long")
	c = e.openContract(b, short)
	expect(t, "effective mode", c.Effective, "grace=30s")
	e.must("contract", "resolve", c.ID, "reject")
	time.Sleep(time.Second)
	v = e.waitStatus(b, "done", short)
	expect(t, "report", v.Report, "rejected")
	expect(t, "resolved by", v.Contracts[0].Resolution.By, "human")
	if hasEvent(v, "resolver.started") {
		t.Fatal("resolver ran after a human answered")
	}
}

// --- 5. afk full, and a failing resolver -----------------------------------------------

func TestAFKFull(t *testing.T) {
	e := newEnv(t)
	start := time.Now()
	a := e.start_("afk-full")
	v := e.waitStatus(a, "done", short)
	expect(t, "report", v.Report, "approved")
	if time.Since(start) > 5*time.Second {
		t.Fatalf("afk resolution took %v", time.Since(start))
	}

	b := e.start_("afk-broken")
	v = e.waitRun(b, short, "resolver failure", func(v runView) bool { return hasEvent(v, "contract.resolver_failed") })
	c := e.openContract(b, short)
	expect(t, "effective mode after failure", c.Effective, "attended")
	e.must("contract", "resolve", c.ID, "reject")
	v = e.waitStatus(b, "done", short)
	expect(t, "report", v.Report, "rejected")
}

// --- 6. live afk overrides -------------------------------------------------------------

func TestAFKOverride(t *testing.T) {
	e := newEnv(t)
	// on one open contract
	a := e.start_("afk-override")
	c := e.openContract(a, short)
	expect(t, "default mode", c.Effective, "attended")
	e.must("afk", "set", "afk", "--contract", c.ID)
	expect(t, "report", e.waitStatus(a, "done", short).Report, "approved")

	// on a run: its open contracts follow
	b := e.start_("afk-override")
	e.openContract(b, short)
	e.must("afk", "set", "grace=1s", "--run", b)
	expect(t, "report", e.waitStatus(b, "done", short).Report, "approved")

	// at start
	c3 := e.start_("afk-override", "--afk", "afk")
	expect(t, "report", e.waitStatus(c3, "done", short).Report, "approved")
}

// --- 7. signals with correlation keys --------------------------------------------------

func TestSignalWait(t *testing.T) {
	e := newEnv(t)
	a := e.start_("signal-wait", "--input", "key=a")
	b := e.start_("signal-wait", "--input", "key=b")
	e.waitStatus(a, "waiting", short)
	e.waitStatus(b, "waiting", short)
	var out struct{ Delivered []string }
	json.Unmarshal(e.must("signal", "pub", "go", "--key", "a", "--data", `{"payload": "x"}`), &out)
	expect(t, "delivered", strings.Join(out.Delivered, ","), a)
	expect(t, "a report", e.waitStatus(a, "done", short).Report, "got x")
	expect(t, "b report", e.waitStatus(b, "done", short).Report, "late") // timeout arm
}

// --- 8. timer --------------------------------------------------------------------------

func TestTimer(t *testing.T) {
	e := newEnv(t)
	start := time.Now()
	id := e.start_("timer")
	e.waitStatus(id, "done", short)
	if d := time.Since(start); d < 2*time.Second {
		t.Fatalf("timer fired after %v, want ≥ 2s", d)
	}
}

// --- 9. crash recovery -----------------------------------------------------------------

func TestCrashRecovery(t *testing.T) {
	e := newEnv(t)
	waitRun := e.start_("recovery")
	worker := e.start_("slow")
	e.waitRun(worker, short, "worker running", func(v runView) bool { return hasEvent(v, "worker.started") })
	time.Sleep(500 * time.Millisecond)
	e.crash() // mid-timer and mid-worker
	e.start()
	c := e.openContract(waitRun, short) // timer fired after restart
	expect(t, "mode", c.Effective, "grace=3s")
	time.Sleep(time.Second)
	e.crash() // mid-grace
	e.start()
	v := e.waitStatus(waitRun, "done", short)
	expect(t, "report", v.Report, "ok")
	expect(t, "resolved by", v.Contracts[0].Resolution.By, "resolver")
	v = e.waitStatus(worker, "done", short)
	expect(t, "slow report", v.Report, "recovered=yes")
}

// --- 10. kekkai isolation --------------------------------------------------------------

func TestKekkaiIsolation(t *testing.T) {
	e := newEnv(t, "test", "other")
	a := e.start_("env-probe")
	expect(t, "test env", e.waitStatus(a, "done", short).Report, "test=t1 other= daemon=")
	b := e.start_("env-probe", "--kekkai", "other")
	expect(t, "other env", e.waitStatus(b, "done", short).Report, "test= other=o1 daemon=")

	// switching changes the CLI default only; runs elsewhere keep going
	g := e.start_("gate")
	e.openContract(g, short)
	e.must("kekkai", "switch", "other")
	e.refused(3, "not found", "run", "start", "gate")
	var runs []engine.RunBrief
	json.Unmarshal(e.must("run", "list", "--kekkai", "test", "--status", "waiting"), &runs)
	expect(t, "test runs still waiting", len(runs), 1)
	e.must("kekkai", "switch", "test")
	e.must("run", "start", "gate")
}

// --- 11. loop budget -------------------------------------------------------------------

func TestLoopBudget(t *testing.T) {
	e := newEnv(t)
	id := e.start_("loop-budget")
	c := e.openContract(id, short)
	if !strings.Contains(c.Title, "Loop budget") {
		t.Fatalf("title = %q", c.Title)
	}
	e.must("contract", "resolve", c.ID, "continue")
	v := e.waitRun(id, short, "second budget contract", func(v runView) bool {
		open := 0
		for _, x := range v.Contracts {
			if x.Status == "open" {
				open++
			}
		}
		return len(v.Contracts) == 2 && open == 1
	})
	for _, x := range v.Contracts {
		if x.Status == "open" {
			e.must("contract", "resolve", x.ID, "stop")
		}
	}
	v = e.waitStatus(id, "failed", short)
	expect(t, "spin visits", v.Visits["spin"], 4)
}

// --- 12. script steps ------------------------------------------------------------------

func TestScriptStep(t *testing.T) {
	e := newEnv(t)
	marker := filepath.Join(e.root, "pwned")
	id := e.start_("script-step", "--input", "word=hi; touch "+marker)
	v := e.waitStatus(id, "done", short)
	expect(t, "report", v.Report, "hi; touch "+marker+"|yes")
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("a var was interpreted by a shell")
	}
}

// --- 12b. probe waits write nothing while pending --------------------------------------

func TestProbeWait(t *testing.T) {
	e := newEnv(t)
	id := e.start_("probe-wait")
	v := e.waitStatus(id, "done", short)
	expect(t, "report", v.Report, "3")
	out, err := exec.Command("git", "-C", filepath.Join(e.root, "store"), "log", "--format=%s").Output()
	if err != nil {
		t.Fatal(err)
	}
	// "created" and "waiting"; the two pending probes before "ready" write nothing
	commits := strings.Count(string(out), "kami: run "+id+" poll")
	expect(t, "memento commits in state poll", commits, 2)
}

// A probe that errors is retried with backoff; only max_errors (default 3) errors in a
// row open a blocker.
func TestProbeErrors(t *testing.T) {
	e := newEnv(t)
	id := e.start_("probe-flaky", "--input", "errors=2", "--input", "strict=false")
	v := e.waitStatus(id, "done", short)
	if len(v.Contracts) != 0 {
		t.Fatalf("two errors opened a blocker; events: %s", eventTypes(v))
	}
	id = e.start_("probe-flaky", "--input", "errors=2", "--input", "strict=true")
	c := e.openContract(id, short)
	if !strings.Contains(c.Title, "the probe failed") || !strings.Contains(c.Body, "upstream 503") {
		t.Fatalf("contract = %q / %q", c.Title, c.Body)
	}
	e.must("contract", "resolve", c.ID, "retry")
	expect(t, "report after retry", e.waitStatus(id, "done", short).Report, "ready")
}

// --- 13. inputs allowlist ---------------------------------------------------------------

func TestInputsScope(t *testing.T) {
	e := newEnv(t)
	id := e.start_("inputs-scope", "--input", "visible=SEEN-VALUE", "--input", "secret=HIDDEN-VALUE")
	expect(t, "report", e.waitStatus(id, "done", short).Report, "visible-only")
}

// --- 14. spawn ---------------------------------------------------------------------------

func TestSpawn(t *testing.T) {
	e := newEnv(t)
	id := e.start_("spawn", "--inputs", `{"items": [1, 2, 3]}`)
	v := e.waitStatus(id, "done", short)
	expect(t, "outcome", v.Outcome, "some_failed")
	kids, _ := v.Vars["children"].([]any)
	if len(kids) != 3 {
		t.Fatalf("children = %v", v.Vars["children"])
	}
	var outs []string
	for _, k := range kids {
		m := k.(map[string]any)
		outs = append(outs, fmt.Sprint(m["outcome"]))
	}
	expect(t, "child outcomes in order", strings.Join(outs, ","), "good,bad,good")
	expect(t, "children listed", len(v.Children), 3)
	expect(t, "child parent", e.show(v.Children[0].ID).Parent, id)
}

// --- 15. entries --------------------------------------------------------------------------

func TestEntry(t *testing.T) {
	e := newEnv(t)
	id := e.start_("entry:followup", "--input", "goal=g1")
	expect(t, "report", e.waitStatus(id, "done", short).Report, "followed g1")
	e.refused(4, "requires input", "run", "start", "entry:followup")
	e.refused(3, "no entry", "run", "start", "entry:nope")
	id = e.start_("entry")
	expect(t, "start report", e.waitStatus(id, "done", short).Report, "began")
}

// --- 16. locks ------------------------------------------------------------------------------

func TestLock(t *testing.T) {
	e := newEnv(t)
	id := e.start_("lock")
	c := e.openContract(id, short)
	if !c.Lock || c.Effective != "attended" {
		t.Fatalf("gate: lock=%v effective=%s", c.Lock, c.Effective)
	}
	e.refused(5, "locked", "afk", "set", "afk", "--contract", c.ID)
	e.must("afk", "set", "afk", "--run", id)
	time.Sleep(1500 * time.Millisecond)
	v := e.show(id)
	expect(t, "status after run override", v.Status, "waiting")
	if hasEvent(v, "resolver.started") {
		t.Fatal("a resolver ran on a gate")
	}
	e.must("contract", "resolve", c.ID, "approve")
	expect(t, "report", e.waitStatus(id, "done", short).Report, "merged")
}

// --- 17. cwd roots ---------------------------------------------------------------------------

func TestCwdRoots(t *testing.T) {
	e := newEnv(t)
	allowed, _ := filepath.EvalSymlinks(filepath.Join(e.root, "allowed"))
	id := e.start_("cwd-roots", "--input", "dir="+allowed)
	expect(t, "pwd", e.waitStatus(id, "done", short).Report, allowed)

	bad := e.start_("cwd-roots", "--input", "dir=/")
	c := e.openContract(bad, short)
	if !strings.Contains(c.Body, "outside kekkai test cwd_roots") {
		t.Fatalf("body = %q", c.Body)
	}

	// a literal cwd outside the roots fails validation and is refused at start
	os.WriteFile(filepath.Join(e.home, "kekkai", "test", "workflows", "cwd-bad.star"), []byte(`
workflow(name = "cwd-bad", start = "where")
state("where", script("scripts/pwd.sh", cwd = "/"), ok = "finished")
state("finished", done())
`), 0o600)
	e.refused(4, "outside kekkai test cwd_roots", "workflow", "validate", "cwd-bad")
	e.refused(4, "outside kekkai test cwd_roots", "run", "start", "cwd-bad")
}

// --- 18. cron ----------------------------------------------------------------------------------

func TestCron(t *testing.T) {
	e := newEnv(t, "cron")
	count := func() int {
		var runs []engine.RunBrief
		json.Unmarshal(e.must("run", "list", "--kekkai", "cron"), &runs)
		return len(runs)
	}
	deadline := time.Now().Add(short)
	for count() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("cron trigger never fired")
		}
		time.Sleep(200 * time.Millisecond)
	}
	e.stop()
	before := count2(e)
	e.start()
	deadline = time.Now().Add(short)
	for count() <= before {
		if time.Now().After(deadline) {
			t.Fatal("cron trigger did not fire after a restart")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// A paused kekkai's cron triggers skip their fires and do not catch up on resume.
func TestCronPaused(t *testing.T) {
	e := newEnv(t, "cron")
	count := func() int {
		var runs []engine.RunBrief
		json.Unmarshal(e.must("run", "list", "--kekkai", "cron"), &runs)
		return len(runs)
	}
	e.must("kekkai", "pause", "cron")
	before := count()
	deadline := time.Now().Add(short)
	for {
		cmd := exec.Command(kamiBin, "--json", "events", "tail", "--type", "trigger.skipped", "--kekkai", "cron")
		cmd.Env = e.vars
		if out, _ := cmd.Output(); bytes.Contains(out, []byte("kekkai paused")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no trigger.skipped while paused")
		}
		time.Sleep(200 * time.Millisecond)
	}
	expect(t, "runs started while paused", count()-before, 0)
	e.must("kekkai", "resume", "cron")
	expect(t, "runs caught up on resume", count()-before, 0)
}

// count2 counts cron runs straight from memento while the daemon is down.
func count2(e *env) int {
	cmd := exec.Command("memento", "--json", "db", "query", "kami", "SELECT count(*) AS n FROM runs WHERE kekkai = 'cron'")
	cmd.Env = e.vars
	out, err := cmd.Output()
	if err != nil {
		e.t.Fatal(err)
	}
	var env struct{ Data []map[string]float64 }
	json.Unmarshal(out, &env)
	return int(env.Data[0]["n"])
}

// --- a chat loop resumes the same session on every re-entry ------------------------------

func TestSessionLoop(t *testing.T) {
	e := newEnv(t)
	id := e.start_("chat-loop", "--input", "thread=t1", "--input", "text=hi")
	v := e.waitRun(id, short, "first answer", func(v runView) bool { return v.State == "listen" })
	expect(t, "first turn resumes", v.Vars["resumed_from"], "none")
	expect(t, "session remembered", v.Sessions["think"], "fake:sess-1")
	e.must("signal", "pub", "msg", "--key", "t1", "--data", `{"text": "again"}`)
	v = e.waitStatus(id, "done", 15*time.Second)
	expect(t, "second turn resumed", v.Report, "sess-1")

	// a later run picks up the session the first one went to sleep with
	next := e.start_("chat-loop", "--input", "thread=t2", "--input", "text=back", "--inputs", `{"kami_sessions": {"think": "fake:sess-1"}}`)
	v = e.waitRun(next, short, "first answer", func(v runView) bool { return v.State == "listen" })
	expect(t, "woken run resumes the old session", v.Vars["resumed_from"], "sess-1")
}

// worker.exited carries what claude's JSON result says the attempt cost
func TestWorkerCost(t *testing.T) {
	e := newEnv(t)
	id := e.start_("chat-loop", "--input", "thread=t1", "--input", "text=hi")
	v := e.waitRun(id, short, "first answer", func(v runView) bool { return v.State == "listen" })
	for _, ev := range v.Events {
		if ev["type"] == "worker.exited" {
			d := ev["data"].(map[string]any)
			expect(t, "cost", fmt.Sprint(d["session_cost_usd"], " ", d["output_tokens"], " ", d["duration_ms"], " ", d["session"]), "0.25 7 1200 sess-1")
			return
		}
	}
	t.Fatalf("no worker.exited; events: %s", eventTypes(v))
}

// --- say= milestones ------------------------------------------------------------------

func TestSay(t *testing.T) {
	e := newEnv(t)
	id := e.start_("say")
	c := e.openContract(id, short)
	e.must("contract", "resolve", c.ID, "ship", "--field", "note=today")
	v := e.waitStatus(id, "done", short)
	var said []string
	for _, ev := range v.Events {
		if ev["type"] == "run.say" {
			said = append(said, fmt.Sprint(ev["data"].(map[string]any)["text"]))
		}
	}
	expect(t, "milestones", strings.Join(said, " | "), "shipping today | worked: ok")
}

// A pending probe's say becomes run.say, only when it differs from the last one.
func TestProbeSay(t *testing.T) {
	e := newEnv(t)
	id := e.start_("probe-say")
	expect(t, "said", strings.Join(said(e.waitStatus(id, "done", short)), " | "), "building | testing")
}

// A heartbeat posts its trimmed stdout as run.say while the step runs, logs a
// failing beat without failing the step, and stops when the step ends.
func TestHeartbeat(t *testing.T) {
	e := newEnv(t)
	id := e.start_("heartbeat", "--input", "word=hi")
	v := e.waitStatus(id, "done", short)
	got := said(v)
	if len(got) < 2 || got[0] != "beat hi 1" || got[1] != "beat hi 3" || !hasEvent(v, "heartbeat.failed") {
		t.Fatalf("said %q; events: %s", got, eventTypes(v))
	}
	time.Sleep(time.Second)
	if n := len(said(e.show(id))); n != len(got) {
		t.Fatalf("beats went on after the step ended: %d, then %d", len(got), n)
	}
}

func said(v runView) []string {
	var out []string
	for _, ev := range v.Events {
		if ev["type"] == "run.say" {
			out = append(out, fmt.Sprint(ev["data"].(map[string]any)["text"]))
		}
	}
	return out
}

// --- a worker that never reports --------------------------------------------------------------

func TestSilentWorker(t *testing.T) {
	e := newEnv(t)
	id := e.start_("silent")
	c := e.openContract(id, short)
	if !strings.Contains(c.Title, "stopped without finishing") || !strings.Contains(c.Body, "forgot to report") {
		t.Fatalf("contract = %q / %q", c.Title, c.Body)
	}
	e.must("contract", "resolve", c.ID, "stop")
	e.waitStatus(id, "failed", short)
}

// --- smoke: real Claude workers (uses tokens) -----------------------------------------------
// KAMI_SMOKE=1 go test ./e2e -run Smoke -v   (or: make smoke)

func smoke(t *testing.T) {
	if os.Getenv("KAMI_SMOKE") == "" {
		t.Skip("set KAMI_SMOKE=1 to run real Claude workers")
	}
	if _, err := exec.LookPath("claude"); err != nil {
		t.Skip("claude not on PATH")
	}
}

func TestSmokeClaudeBlockerWake(t *testing.T) {
	smoke(t)
	e := newEnv(t)
	id := e.start_("claude-smoke")
	v := e.waitRun(id, 5*time.Minute, "the worker to sleep on a contract", func(v runView) bool {
		return v.Status == "sleeping" || v.Status == "failed" || v.Status == "waiting"
	})
	if v.Status != "sleeping" {
		t.Fatalf("status %s, contracts %+v; logs %s", v.Status, v.Contracts, v.Step.Dir)
	}
	c := v.Contracts[0]
	t.Logf("contract from claude: %q options %v", c.Title, c.Options)
	e.must("contract", "resolve", c.ID, "pear")
	v = e.waitRun(id, 5*time.Minute, "done", func(v runView) bool { return Terminal(v.Status) || v.Status == "waiting" })
	expect(t, "status", v.Status, "done")
	expect(t, "report", v.Report, "pear")
	if len(v.Auras) == 0 {
		t.Log("note: the worker did not save an aura before sleeping")
	}
}

func TestSmokeClaudeSessionResume(t *testing.T) {
	smoke(t)
	e := newEnv(t)
	id := e.start_("claude-resume")
	v := e.waitRun(id, 5*time.Minute, "the worker to sleep on a contract", func(v runView) bool {
		return v.Status == "sleeping" || v.Status == "failed" || v.Status == "waiting"
	})
	if v.Status != "sleeping" {
		t.Fatalf("status %s, contracts %+v", v.Status, v.Contracts)
	}
	if v.Step.Session == "" {
		t.Fatal("no claude session id recorded")
	}
	e.must("contract", "resolve", v.Contracts[0].ID, "apple")
	v = e.waitRun(id, 5*time.Minute, "done", func(v runView) bool { return Terminal(v.Status) || v.Status == "waiting" })
	expect(t, "status", v.Status, "done")
	expect(t, "report", v.Report, "apple lantern")
	resumed := false
	for _, ev := range v.Events {
		if d, _ := ev["data"].(map[string]any); ev["type"] == "worker.started" && d["session_resume"] == true {
			resumed = true
		}
	}
	if !resumed {
		t.Fatal("the wake did not resume the session")
	}
}

func TestSmokeCodexBlockerWake(t *testing.T) {
	smoke(t)
	if _, err := exec.LookPath("codex"); err != nil {
		t.Skip("codex not on PATH")
	}
	e := newEnv(t)
	id := e.start_("codex-smoke")
	v := e.waitRun(id, 8*time.Minute, "the worker to sleep on a contract", func(v runView) bool {
		return v.Status == "sleeping" || v.Status == "failed" || v.Status == "waiting"
	})
	if v.Status != "sleeping" {
		t.Fatalf("status %s, contracts %+v; logs %s", v.Status, v.Contracts, v.Step.Dir)
	}
	e.must("contract", "resolve", v.Contracts[0].ID, "pear")
	v = e.waitRun(id, 8*time.Minute, "done", func(v runView) bool { return Terminal(v.Status) || v.Status == "waiting" })
	expect(t, "status", v.Status, "done")
	expect(t, "report", v.Report, "pear")
}

func Terminal(s string) bool { return engine.Terminal(s) }

// --- memory -----------------------------------------------------------------------------

// The owner hears everything; a DM hears public and its own DM; a channel
// hears public and its own channel; what a worker saves stays in its
// conversation whatever scope it asks for; no worker can set kami_* vars.
func TestMemoryScopes(t *testing.T) {
	e := newEnv(t)
	for _, s := range []string{"public", "private", "conduct", "dm:U1", "dm:U2", "chan:C1"} {
		e.must("memory", "save", "--about", "haithem", "--scope", s, "fact "+s)
	}
	dm := e.waitStatus(e.start_("memory", "--input", "kami_conv=dm", "--input", "kami_peer=U1"), "done", short)
	expect(t, "dm hears", dm.Report, "[conduct] [dm:U1] [dm:U1] [public] | promoted=no")
	group := e.waitStatus(e.start_("memory", "--input", "kami_conv=group", "--input", "kami_channel=C1"), "done", short)
	expect(t, "channel hears", group.Report, "[chan:C1] [chan:C1] [conduct] [public] | promoted=no")
	owner := e.waitStatus(e.start_("memory", "--input", "kami_conv=owner"), "done", short)
	expect(t, "owner chat hears", owner.Report, "[conduct] [private] [private] [public] | promoted=no")
	forged := e.waitStatus(e.start_("memory", "--input", "kami_conv=cli"), "done", short)
	expect(t, "a run cannot claim the terminal", forged.Report, "none | promoted=no")
	peerless := e.waitStatus(e.start_("memory", "--input", "kami_conv=dm"), "done", short)
	expect(t, "a DM without its peer has no memory", peerless.Report, "none | promoted=no")
	system := e.waitStatus(e.start_("memory"), "done", short)
	expect(t, "a run with no conversation has no memory", system.Report, "none | promoted=no")
	sig := e.start_("memory", "--input", "kami_conv=group", "--input", "kami_channel=SIG")
	e.waitRun(sig, short, "waiting on promote", func(v runView) bool { return v.State == "listen" })
	e.must("signal", "pub", "promote", "--data", `{"kami_conv": "owner"}`)
	expect(t, "signal cannot promote", e.waitStatus(sig, "done", short).Report, "group")
	var facts []state.Memory
	json.Unmarshal(e.must("memory", "load", "--about", "haithem"), &facts)
	if len(facts) != 6 { // the terminal hears every scope
		t.Fatalf("owner load about haithem: %+v", facts)
	}
}

// A worker that reported and then died with the daemon is not run again: the
// reported outcome was saved and recovery applies it.
func TestCrashAfterReport(t *testing.T) {
	e := newEnv(t)
	id := e.start_("linger")
	e.waitRun(id, short, "reported", func(v runView) bool { return hasEvent(v, "step.reported") })
	e.crash()
	e.start()
	v := e.waitStatus(id, "done", short)
	n := 0
	for _, ev := range v.Events {
		if ev["type"] == "worker.started" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("worker started %d times; report %q", n, v.Report)
	}
}

// A run parked on an old definition moves onto the edited workflow: its contract is
// withdrawn and it carries on from the named state of the new definition.
func TestUpgrade(t *testing.T) {
	e := newEnv(t)
	id := e.start_("lock")
	e.openContract(id, short)
	e.refused(4, "no state", "run", "upgrade", id, "--state", "fixed")
	wf := filepath.Join(e.home, "kekkai", "test", "workflows", "lock.star")
	raw, _ := os.ReadFile(wf)
	os.WriteFile(wf, bytes.Replace(raw, []byte(`done("merged")`), []byte(`done("upgraded: {note}{kami_x}")`), 1), 0o600)
	e.must("run", "upgrade", id, "--state", "merged", "--vars", `{"note": "make it bigger", "kami_x": "no"}`)
	v := e.waitStatus(id, "done", short)
	expect(t, "report", v.Report, "upgraded: make it bigger")
	if !hasEvent(v, "run.upgraded") || !hasEvent(v, "contract.cancelled") {
		t.Fatalf("events: %s", eventTypes(v))
	}
	e.refused(5, "already", "run", "upgrade", id)
}

// An upgrade that re-enters a state asking the same question keeps the open contract;
// a changed question replaces it.
func TestUpgradeKeepsQuestion(t *testing.T) {
	e := newEnv(t)
	id := e.start_("gate")
	c := e.openContract(id, short)
	e.must("run", "upgrade", id)
	v := e.show(id)
	if hasEvent(v, "contract.cancelled") || !hasEvent(v, "contract.kept") || len(v.Contracts) != 1 || v.Step.Contract != c.ID {
		t.Fatalf("contracts %d, step contract %s (was %s), events: %s", len(v.Contracts), v.Step.Contract, c.ID, eventTypes(v))
	}
	wf := filepath.Join(e.home, "kekkai", "test", "workflows", "gate.star")
	raw, _ := os.ReadFile(wf)
	os.WriteFile(wf, bytes.Replace(raw, []byte(`"Ship it?"`), []byte(`"Ship it now?"`), 1), 0o600)
	e.must("run", "upgrade", id)
	v = e.show(id)
	if !hasEvent(v, "contract.cancelled") || v.Step.Contract == c.ID {
		t.Fatalf("a changed question was not re-asked; events: %s", eventTypes(v))
	}
	e.must("contract", "resolve", v.Step.Contract, "ship")
	expect(t, "report", e.waitStatus(id, "done", short).Report, "shipped")
}

// --- pause: a paused kekkai parks runs (across a restart) until resume -------------------

func TestPause(t *testing.T) {
	e := newEnv(t)
	e.must("kekkai", "pause", "test", "--except", "script-step")
	id := e.start_("probe-wait")
	v := e.waitRun(id, short, "parked", func(v runView) bool { return hasEvent(v, "run.paused") })
	expect(t, "status while paused", v.Status, "waiting")
	free := e.start_("script-step", "--input", "word=hi")
	e.waitStatus(free, "done", short) // an excepted workflow keeps going
	e.crash()
	e.start()
	time.Sleep(time.Second)
	expect(t, "status after restart", e.show(id).Status, "waiting")
	e.must("kekkai", "resume", "test")
	expect(t, "report", e.waitStatus(id, "done", short).Report, "3")
	e.refused(5, "not paused", "kekkai", "resume", "test")
	e.must("kekkai", "pause", "test", "--until", time.Now().Add(2*time.Second).Format(time.RFC3339))
	id = e.start_("probe-wait")
	expect(t, "report after --until", e.waitStatus(id, "done", short).Report, "3")

	id = e.start_("probe-wait") // already waiting when the pause starts: its probes stop
	e.must("kekkai", "pause", "test")
	time.Sleep(3 * time.Second)
	expect(t, "status with probes held", e.show(id).Status, "waiting")
	e.must("kekkai", "resume", "test")
	expect(t, "report after resume", e.waitStatus(id, "done", short).Report, "3")
	// a wait's timer does not run out while paused: the pause's length is added back
	id = e.start_("timer")
	time.Sleep(500 * time.Millisecond)
	e.must("kekkai", "pause", "test")
	time.Sleep(3 * time.Second) // past the 2s timer
	expect(t, "timer held", e.show(id).Status, "waiting")
	e.must("kekkai", "resume", "test")
	time.Sleep(800 * time.Millisecond)
	expect(t, "timer still has its 1.5s left", e.show(id).Status, "waiting")
	e.waitStatus(id, "done", short)
}

// --- system instructions and SSE -------------------------------------------------------

func sayText(v runView, want string) bool {
	for _, ev := range v.Events {
		if d, _ := ev["data"].(map[string]any); ev["type"] == "run.say" && d["text"] == want {
			return true
		}
	}
	return false
}

func TestSystemInstruction(t *testing.T) {
	e := newEnv(t)

	// no state: the waiting probe runs now, an hour early
	id := e.start_("system-wait")
	e.waitStatus(id, "waiting", short)
	e.waitRun(id, short, "first probe", func(v runView) bool { return hasEvent(v, "wait.armed") })
	time.Sleep(time.Second) // the first pending has landed
	e.refused(4, "text", "system", id, " ")
	e.must("system", id, "wake up")
	v := e.waitStatus(id, "done", short)
	expect(t, "woken report", v.Report, "woke")
	if !hasEvent(v, "system.instruction") || !sayText(v, "🛠 system: wake up") {
		t.Fatalf("not logged; events: %s", eventTypes(v))
	}

	// state and vars: the run continues from there
	id = e.start_("system-wait")
	e.waitStatus(id, "waiting", short)
	e.must("system", id, "move on", "--state", "other", "--vars", `{"note": "x"}`)
	expect(t, "moved report", e.waitStatus(id, "done", short).Report, "moved x")

	// an open contract is answered, as system
	id = e.start_("gate")
	e.openContract(id, short)
	e.refused(4, "not one of", "system", id, "maybe", "--answer", "maybe")
	e.must("system", id, "go", "--answer", "ship")
	v = e.waitStatus(id, "done", short)
	expect(t, "answered report", v.Report, "shipped")

	// a locked contract stays the owner's, whichever way the instruction comes
	id = e.start_("system-lock")
	lock := e.openContract(id, short)
	e.refused(5, "only the owner", "system", id, "go", "--answer", "ship")
	e.refused(5, "only the owner", "system", id, "go", "--state", "shipped")
	e.must("contract", "resolve", lock.ID, "ship")
	e.waitStatus(id, "done", short)

	// a jump cannot skip a gate on the way; it can arrive at one, and the gate then holds
	id = e.start_("system-jump")
	e.waitStatus(id, "waiting", short)
	e.refused(5, "human-only", "system", id, "skip it", "--state", "shipped")
	e.must("system", id, "to the gate", "--state", "ship_it")
	lock = e.openContract(id, short)
	e.refused(5, "only the owner", "system", id, "go", "--state", "shipped")
	e.must("contract", "resolve", lock.ID, "ship")
	expect(t, "gated report", e.waitStatus(id, "done", short).Report, "shipped")
}

func TestSystemQueuedWhileBusy(t *testing.T) {
	e := newEnv(t)

	// vars sent mid-step apply when it ends, over the step's own, and the edge is unchanged
	id := e.start_("system-busy")
	e.waitRun(id, short, "the script", func(v runView) bool { return hasEvent(v, "state.entered") })
	e.must("system", id, "use this", "--vars", `{"note": "queued"}`)
	v := e.waitStatus(id, "done", short)
	expect(t, "queued vars report", v.Report, "next queued")
	if !hasEvent(v, "system.queued") || !hasEvent(v, "system.instruction") {
		t.Fatalf("not logged; events: %s", eventTypes(v))
	}

	// a queued state overrides where the step would go
	id = e.start_("system-busy")
	e.waitRun(id, short, "the script", func(v runView) bool { return hasEvent(v, "state.entered") })
	e.must("system", id, "go there", "--state", "other", "--vars", `{"note": "moved"}`)
	expect(t, "queued state report", e.waitStatus(id, "done", short).Report, "other moved")

	// what would refuse it later refuses it now
	id = e.start_("system-busy")
	e.waitRun(id, short, "the script", func(v runView) bool { return hasEvent(v, "state.entered") })
	e.refused(4, "no state", "system", id, "nope", "--state", "missing")
	e.waitStatus(id, "done", short)
}

func TestWorkerCannotSendSystem(t *testing.T) {
	e := newEnv(t)
	id := e.start_("system-wait")
	e.waitStatus(id, "waiting", short)
	e.vars = append(e.vars, "KAMI_TOKEN=worker")
	if _, code, msg := e.kami("system", id, "from a worker"); code == 0 || !strings.Contains(msg, "workers cannot") {
		t.Fatalf("worker sent a system instruction: code %d, %q", code, msg)
	}
}

func TestEventsSSE(t *testing.T) {
	e := newEnv(t)
	id := e.start_("gate")
	c := e.openContract(id, short)
	sock := filepath.Join(e.home, "kami.sock")
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), short)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://kami/events?run="+id, nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	expect(t, "content type", resp.Header.Get("Content-Type"), "text/event-stream")
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	lastID, resolved := "", false
	for sc.Scan() && !resolved {
		line := sc.Text()
		if v, ok := strings.CutPrefix(line, "id: "); ok {
			lastID = v
		}
		if line == "event: contract.opened" { // the backlog is there: what follows arrives live
			e.must("contract", "resolve", c.ID, "ship")
		}
		resolved = line == "event: contract.resolved"
	}
	if !resolved || lastID == "" {
		t.Fatalf("stream ended before contract.resolved (last id %q): %v", lastID, sc.Err())
	}

	// Last-Event-ID replays only what came after it
	req, _ = http.NewRequestWithContext(ctx, "GET", "http://kami/events?run="+id, nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Last-Event-ID", lastID)
	resp2, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	sc = bufio.NewScanner(resp2.Body)
	if sc.Scan() {
		got, _ := strings.CutPrefix(sc.Text(), "id: ")
		first, err1 := strconv.ParseInt(got, 10, 64)
		after, err2 := strconv.ParseInt(lastID, 10, 64)
		if err1 != nil || err2 != nil || first <= after {
			t.Fatalf("after Last-Event-ID %s the first line is %q", lastID, sc.Text())
		}
	}
}
