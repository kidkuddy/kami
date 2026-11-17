package engine

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"kami/internal/kamui"
	"kami/internal/kekkai"
	"kami/skills"
)

// proc is a running worker, script, probe or resolver.
type proc struct {
	cmd   *exec.Cmd
	token string
	done  chan struct{}
}

func (p *proc) kill() {
	if p.cmd.Process != nil {
		syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL) // the whole process group
	}
}

// sweepMarked kills every process whose environment holds marker (KAMI_PROC=<token>).
// ponytail: one `ps -E` scan per worker exit; macOS/BSD ps prints the environment after the command.
func sweepMarked(marker string) {
	out, err := exec.Command("ps", "-axEww", "-o", "pid=,command=").Output()
	if err != nil {
		return
	}
	self := os.Getpid()
	for _, l := range strings.Split(string(out), "\n") {
		f := strings.Fields(l)
		if len(f) < 2 || !slices.Contains(f[1:], marker) {
			continue
		}
		if pid, err := strconv.Atoi(f[0]); err == nil && pid != self {
			syscall.Kill(pid, syscall.SIGKILL)
		}
	}
}

type procResult struct {
	Exit     int
	TimedOut bool
	Stdout   []byte
	Stderr   []byte
	Err      error
}

type procSpec struct {
	Args    []string
	Dir     string
	Env     []string
	Stdin   []byte
	Timeout time.Duration
	LogDir  string // stdout/stderr are kept here when set
	// Sweep kills, when the process ends, every process still carrying its KAMI_PROC marker:
	// what a worker started detached (nohup, a backgrounded shell) left its process group and
	// outlives it otherwise, still hitting dev with nobody reading its results.
	Sweep bool
}

// startProc launches spec; onExit runs in its own goroutine when it ends.
func startProc(spec procSpec, tok string, onExit func(procResult)) (*proc, error) {
	cmd := exec.Command(spec.Args[0], spec.Args[1:]...)
	cmd.Dir, cmd.Env = spec.Dir, spec.Env
	marker := ""
	if spec.Sweep {
		marker = "KAMI_PROC=" + token()
		cmd.Env = append(cmd.Env, marker)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if spec.Stdin != nil {
		cmd.Stdin = bytes.NewReader(spec.Stdin)
	}
	var out, errb bytes.Buffer
	var outW, errW io.Writer = &out, &errb
	var files []*os.File
	if spec.LogDir != "" {
		if f, err := os.Create(filepath.Join(spec.LogDir, "stdout.log")); err == nil {
			outW, files = io.MultiWriter(&out, f), append(files, f)
		}
		if f, err := os.Create(filepath.Join(spec.LogDir, "stderr.log")); err == nil {
			errW, files = io.MultiWriter(&errb, f), append(files, f)
		}
	}
	cmd.Stdout, cmd.Stderr = outW, errW
	if err := cmd.Start(); err != nil {
		for _, f := range files {
			f.Close()
		}
		return nil, err
	}
	p := &proc{cmd: cmd, token: tok, done: make(chan struct{})}
	go func() {
		var timedOut atomic.Bool
		var timer *time.Timer
		if spec.Timeout > 0 {
			timer = time.AfterFunc(spec.Timeout, func() { timedOut.Store(true); p.kill() })
		}
		err := cmd.Wait()
		if timer != nil {
			timer.Stop()
		}
		if marker != "" {
			sweepMarked(marker)
		}
		for _, f := range files {
			f.Close()
		}
		res := procResult{TimedOut: timedOut.Load(), Stdout: out.Bytes(), Stderr: errb.Bytes()}
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				res.Exit = ee.ExitCode()
			} else {
				res.Exit, res.Err = -1, err
			}
		}
		close(p.done)
		onExit(res)
	}()
	return p, nil
}

// failureBody is the short body of a "step failed" blocker: the one line that
// says why, then where the full logs are. The logs keep everything.
func failureBody(res procResult, dir string) string {
	why := ""
	// claude -p --output-format json carries its error, or why it stopped, in "result"
	var cj struct {
		Result string `json:"result"`
	}
	if json.Unmarshal(bytes.TrimSpace(res.Stdout), &cj) == nil && cj.Result != "" {
		why = "Claude: " + firstLine(cj.Result)
	}
	if why == "" {
		if l := lastLine(res.Stderr); l != "" {
			why = l
		} else if l := lastLine(res.Stdout); l != "" {
			why = l
		} else {
			why = "no output"
		}
	}
	if len(why) > 300 {
		why = why[:300] + "…"
	}
	return fmt.Sprintf("%s\n\nLogs: %s", why, dir)
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func lastLine(b []byte) string {
	s := strings.TrimSpace(string(b))
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[i+1:])
	}
	return s
}

func tail(b []byte, n int) string {
	s := strings.TrimSpace(string(b))
	if len(s) > n {
		s = "…" + s[len(s)-n:]
	}
	return s
}

// lastJSON parses the last stdout line that is a JSON object.
func lastJSON(out []byte) (map[string]any, bool) {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		l := strings.TrimSpace(lines[i])
		if l == "" {
			continue
		}
		if !strings.HasPrefix(l, "{") {
			return nil, false
		}
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) != nil {
			return nil, false
		}
		return m, true
	}
	return nil, false
}

// commandArgs splits and renders a command template. A local first word
// ("scripts/x") resolves inside the kekkai.
func commandArgs(tmpl string, vars map[string]any, k *kekkai.Kekkai) ([]string, []string, error) {
	parts, err := kamui.SplitCommand(tmpl)
	if err != nil {
		return nil, nil, err
	}
	var missing []string
	for i, p := range parts {
		var m []string
		parts[i], m = kamui.Render(p, vars)
		missing = append(missing, m...)
	}
	if kamui.CommandIsLocal(parts[0]) {
		parts[0] = filepath.Join(k.Dir, parts[0])
	} else if strings.HasPrefix(parts[0], "~") {
		parts[0] = kekkai.ExpandHome(parts[0])
	}
	return parts, missing, nil
}

// workerPath puts $KAMI_HOME/bin (a `kami` symlink to this binary) first, so
// workers reach the right kami whatever the binary is called.
func (e *Engine) workerPath() string {
	p := os.Getenv("PATH")
	if e.KamiBin != "" {
		p = filepath.Join(e.Home, "bin") + string(os.PathListSeparator) + p
	}
	return p
}

func (e *Engine) linkBin() error {
	if e.KamiBin == "" {
		return nil
	}
	dir := filepath.Join(e.Home, "bin")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	link := filepath.Join(dir, "kami")
	if cur, err := os.Readlink(link); err == nil && cur == e.KamiBin {
		return nil
	}
	os.Remove(link)
	return os.Symlink(e.KamiBin, link)
}

// stepVars is what a step may see: everything, or only its inputs allowlist.
func stepVars(r *Run, s *kamui.Step) map[string]any {
	out := map[string]any{}
	if !s.InputsSet {
		for k, v := range r.Vars {
			out[k] = v
		}
		return out
	}
	for _, name := range s.Inputs {
		if v, ok := kamui.Lookup(r.Vars, name); ok {
			out[name] = v
		}
	}
	return out
}

// stepCwd resolves a step's cwd= against the kekkai roots, or uses fallback.
func stepCwd(s *kamui.Step, vars map[string]any, k *kekkai.Kekkai, fallback string) (string, error) {
	if s.Cwd == "" {
		return fallback, nil
	}
	dir, missing := kamui.Render(s.Cwd, vars)
	if len(missing) > 0 {
		return "", fmt.Errorf("cwd %q: vars not set: %s", s.Cwd, strings.Join(missing, ", "))
	}
	return k.CheckCwd(dir)
}

func (e *Engine) attemptDir(r *Run) (string, error) {
	d := filepath.Join(e.runDir(r), fmt.Sprintf("%s.%d.%d", r.State, r.Visits[r.State], r.Step.Attempt))
	return d, os.MkdirAll(d, 0o700)
}

// --- agents --------------------------------------------------------------------

type wake struct {
	resolution *Contract
	note       string
}

func (e *Engine) startAgent(r *Run, wk *wake) {
	w := e.def(r)
	st := w.States[r.State]
	s := st.Step
	fail := func(err error) {
		e.block(r, "stuck", "agent step "+r.State+" could not start", err.Error(), []string{"retry", "stop"})
	}
	k, err := kekkai.Load(e.Home, r.Kekkai)
	if err != nil {
		fail(err)
		return
	}
	name := s.Worker
	if s.WorkerFn != nil {
		out, err := kamui.Call(s.WorkerFn, e.guardVars(r))
		if err != nil {
			fail(fmt.Errorf("worker function: %w", err))
			return
		}
		name, _ = out.(string)
	}
	spec, ok := k.Workers[name]
	if !ok {
		fail(fmt.Errorf("worker %q is not defined in kekkai %s", name, k.Name))
		return
	}
	r.Step.Attempt++
	dir, err := e.attemptDir(r)
	if err != nil {
		fail(err)
		return
	}
	vars := stepVars(r, s)
	cwd, err := stepCwd(s, r.Vars, k, dir)
	if err != nil {
		fail(err)
		return
	}
	resume := s.Resume
	if resume == "" {
		resume = w.Resume
	}
	// resume = "session" continues the state's last session: on a wake from a
	// contract (with only the answer as the prompt), and on every re-entry of
	// the state (with the full prompt: a chat loop keeps its conversation).
	session := r.Step.Session
	if session == "" && resume == "session" {
		last := r.Sessions[r.State]
		if last == "" { // a run that picks up where an earlier one went to sleep (kami_sessions input)
			if prev, ok := r.Vars["kami_sessions"].(map[string]any); ok {
				last, _ = prev[r.State].(string)
			}
		}
		if prov, id, ok := strings.Cut(last, ":"); ok && prov == spec.Provider {
			session = id
		}
	}
	wakeResume := wk != nil && wk.resolution != nil && resume == "session" && session != "" && r.Step.Provider == spec.Provider
	sessionResume := resume == "session" && session != "" && (wakeResume || wk == nil)
	var latest *Aura
	if wk != nil {
		latest = e.latestAura(r.ID, r.State)
	}
	prompt := e.buildPrompt(r, st, k, vars, wk, latest, spec)
	if wakeResume {
		prompt = resumeMessage(wk)
	}
	if err := os.WriteFile(filepath.Join(dir, "prompt.md"), []byte(prompt), 0o600); err != nil {
		fail(err)
		return
	}
	tok := token()
	env := k.WorkerEnv(map[string]string{
		"PATH": e.workerPath(), "KAMI_SOCK": e.Sock, "KAMI_RUN": r.ID, "KAMI_STEP": r.State,
		"KAMI_TOKEN": tok, "KAMI_STEP_DIR": dir, "KAMI_PROMPT_FILE": filepath.Join(dir, "prompt.md"),
		"KAMI_KEKKAI_DIR": k.Dir,
	})
	if sessionResume && spec.Provider == "fake" {
		env = append(env, "KAMI_RESUME_SESSION="+session) // tests check which session a fake would resume
	}
	args, err := e.providerArgs(spec, k, s, dir, cwd, prompt, sessionResume, session)
	if err != nil {
		fail(err)
		return
	}
	timeout := s.Timeout
	if timeout == 0 {
		timeout = time.Hour
	}
	if wk != nil && wk.resolution != nil {
		r.Step.Contract = "" // answered; the resolution travels in the prompt
	}
	r.Step.Token, r.Step.Phase, r.Step.Dir, r.Step.Worker, r.Step.Provider = tok, "running", dir, name, spec.Provider
	r.Step.Pending = nil
	r.Status = StRunning
	runID := r.ID
	p, err := startProc(procSpec{Args: args, Dir: cwd, Env: env, Timeout: timeout, LogDir: dir, Sweep: true}, tok, func(res procResult) {
		e.agentExited(runID, tok, res)
	})
	if err != nil {
		fail(fmt.Errorf("start %s: %w", args[0], err))
		return
	}
	e.procs[r.ID] = p
	e.saveRun(r, fmt.Sprintf("agent attempt %d", r.Step.Attempt))
	e.event(r, "worker.started", map[string]any{"state": r.State, "attempt": r.Step.Attempt, "worker": name,
		"provider": spec.Provider, "wake": wk != nil, "session_resume": sessionResume, "dir": dir})
}

func (e *Engine) latestAura(runID, step string) *Aura {
	l := e.auras[runID]
	for i := len(l) - 1; i >= 0; i-- {
		if l[i].Step == step {
			return l[i]
		}
	}
	return nil
}

func resumeMessage(wk *wake) string {
	raw, _ := json.MarshalIndent(wk.resolution.Resolution, "", "  ")
	return fmt.Sprintf("The contract you raised (%q) was resolved:\n\n```json\n%s\n```\n\nContinue the step from where you stopped. Finish with `kami step done --outcome <outcome>`.",
		wk.resolution.Title, raw)
}

func (e *Engine) buildPrompt(r *Run, st *kamui.State, k *kekkai.Kekkai, vars map[string]any, wk *wake, aura *Aura, spec kekkai.Worker) string {
	var b strings.Builder
	if spec.Provider == "codex" && promptFile(k, spec) != "" {
		if p, err := os.ReadFile(filepath.Join(k.Dir, promptFile(k, spec))); err == nil {
			b.Write(p)
			b.WriteString("\n\n")
		}
	}
	instr, missing := kamui.Render(st.Step.Instruction, r.Vars)
	if len(missing) > 0 {
		e.event(r, "template.missing", map[string]any{"state": st.Name, "vars": missing})
	}
	fmt.Fprintf(&b, "You are a Kami worker running one step of a workflow.\n\nRun: %s  Workflow: %s  Kekkai: %s  State: %s  Attempt: %d\n\n",
		r.ID, r.Workflow, r.Kekkai, st.Name, r.Step.Attempt)
	fmt.Fprintf(&b, "## Instruction\n\n%s\n\n", instr)
	raw, _ := json.MarshalIndent(vars, "", "  ")
	fmt.Fprintf(&b, "## Inputs\n\n```json\n%s\n```\n\n", raw)
	if st.Step.InputsSet {
		var absent []string
		for _, n := range st.Step.Inputs {
			if _, ok := vars[n]; !ok {
				absent = append(absent, n)
			}
		}
		if len(absent) > 0 {
			fmt.Fprintf(&b, "Not set yet: %s.\n\n", strings.Join(absent, ", "))
		}
	}
	fmt.Fprintf(&b, "## Finishing\n\nAllowed outcomes: %s\n\nReport with `kami step done --outcome <outcome> [--vars '<json object>']`.\n\n",
		strings.Join(st.Order, ", "))
	if spec.Provider == "codex" {
		var paths []string
		for _, sk := range skillNames(k, st.Step) {
			paths = append(paths, filepath.Join(k.Dir, "skills", sk, "SKILL.md"))
		}
		if len(paths) > 0 {
			fmt.Fprintf(&b, "## Skills\n\nRead these before starting:\n- %s\n\n", strings.Join(paths, "\n- "))
		}
	}
	if spec.Provider != "fake" {
		// the protocol rides in the prompt too: a worker that cannot call kami cannot finish
		b.WriteString(protocolBody())
		b.WriteString("\n")
	}
	if wk != nil {
		b.WriteString("\n## Waking up\n\n")
		if wk.note != "" {
			b.WriteString(wk.note + "\n\n")
		}
		if aura != nil {
			raw, _ := json.MarshalIndent(aura, "", "  ")
			fmt.Fprintf(&b, "The previous worker saved this Aura snapshot. Continue from its \"next\"; do not redo its \"progress\".\n\n```json\n%s\n```\n\n", raw)
		}
		if wk.resolution != nil {
			c := wk.resolution
			raw, _ := json.MarshalIndent(c.Resolution, "", "  ")
			fmt.Fprintf(&b, "It slept on the contract %q (%s). It was resolved:\n\n```json\n%s\n```\n", c.Title, c.ID, raw)
		}
	}
	return b.String()
}

// protocolBody is the kami-worker skill without its frontmatter.
func protocolBody() string {
	body := skills.Worker
	if strings.HasPrefix(body, "---") {
		if i := strings.Index(body[3:], "\n---"); i >= 0 {
			body = strings.TrimLeft(body[3+i+4:], "\n")
		}
	}
	return body
}

func skillNames(k *kekkai.Kekkai, s *kamui.Step) []string {
	seen := map[string]bool{}
	var out []string
	for _, l := range [][]string{k.Skills, s.Skills} {
		for _, n := range l {
			if !seen[n] {
				seen[n] = true
				out = append(out, n)
			}
		}
	}
	sort.Strings(out)
	return out
}

// promptFile is the system prompt file a worker gets: its own, else the kekkai's.
func promptFile(k *kekkai.Kekkai, spec kekkai.Worker) string {
	if spec.Prompt != "" {
		return spec.Prompt
	}
	return k.Prompt
}

// providerArgs builds the worker command line.
func (e *Engine) providerArgs(spec kekkai.Worker, k *kekkai.Kekkai, s *kamui.Step, dir, cwd, prompt string, resume bool, session string) ([]string, error) {
	switch spec.Provider {
	case "fake":
		args, err := kamui.SplitCommand(spec.Command)
		if err != nil {
			return nil, fmt.Errorf("fake worker command: %w", err)
		}
		if kamui.CommandIsLocal(args[0]) {
			args[0] = filepath.Join(k.Dir, args[0])
		}
		return args, nil
	case "claude", "":
		plugin, err := buildPlugin(dir, k, s)
		if err != nil {
			return nil, err
		}
		args := []string{binOr(spec.Bin, "claude"), "-p", prompt, "--output-format", "json", "--setting-sources", "project", "--plugin-dir", plugin}
		if resume {
			args = append(args, "--resume", session)
		}
		if spec.PermissionMode != "" {
			args = append(args, "--permission-mode", spec.PermissionMode)
		} else {
			args = append(args, "--dangerously-skip-permissions")
		}
		if len(spec.Available) > 0 {
			args = append(args, "--tools", strings.Join(spec.Available, ","))
		}
		if len(spec.Tools) > 0 || len(spec.Kami) > 0 {
			kami := []string{"Bash(kami:*)"}
			if len(spec.Kami) > 0 {
				kami = nil
				for _, c := range spec.Kami {
					kami = append(kami, "Bash(kami "+c+":*)")
				}
			}
			// one argument per tool; the flag is variadic, so another flag must follow
			args = append(args, "--allowedTools")
			args = append(args, append(append([]string{}, spec.Tools...), kami...)...)
		}
		if spec.Model != "" {
			args = append(args, "--model", spec.Model)
		}
		if spec.Effort != "" {
			args = append(args, "--effort", spec.Effort)
		}
		if promptFile(k, spec) != "" {
			if p, err := os.ReadFile(filepath.Join(k.Dir, promptFile(k, spec))); err == nil {
				args = append(args, "--append-system-prompt", string(p))
			}
		}
		return args, nil
	case "codex":
		args := []string{binOr(spec.Bin, "codex"), "exec"}
		if resume {
			args = append(args, "resume")
		}
		// --ignore-user-config/--ignore-rules: only the kekkai steers a step, never ~/.codex
		args = append(args, "--json", "--skip-git-repo-check", "--dangerously-bypass-approvals-and-sandbox",
			"--ignore-user-config", "--ignore-rules")
		if spec.Model != "" {
			args = append(args, "--model", spec.Model)
		}
		if spec.Effort != "" {
			args = append(args, "--config", fmt.Sprintf("model_reasoning_effort=%q", spec.Effort))
		}
		if !resume {
			args = append(args, "--cd", cwd)
		} else {
			args = append(args, session)
		}
		return append(args, prompt), nil
	}
	return nil, fmt.Errorf("unknown provider %q (want claude, codex or fake)", spec.Provider)
}

func binOr(bin, def string) string {
	if bin != "" {
		return bin
	}
	return def
}

// buildPlugin makes a per-attempt Claude Code plugin carrying the kekkai's
// skills plus the worker protocol, loaded with --plugin-dir.
func buildPlugin(dir string, k *kekkai.Kekkai, s *kamui.Step) (string, error) {
	plugin := filepath.Join(dir, "plugin")
	if err := os.MkdirAll(filepath.Join(plugin, ".claude-plugin"), 0o700); err != nil {
		return "", err
	}
	manifest := fmt.Sprintf(`{"name": "kami-%s", "version": "0.0.0", "description": "Kami kekkai %s skills"}`, k.Name, k.Name)
	if err := os.WriteFile(filepath.Join(plugin, ".claude-plugin", "plugin.json"), []byte(manifest), 0o600); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Join(plugin, "skills", "kami-worker"), 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(plugin, "skills", "kami-worker", "SKILL.md"), []byte(skills.Worker), 0o600); err != nil {
		return "", err
	}
	for _, n := range skillNames(k, s) {
		link := filepath.Join(plugin, "skills", n)
		os.Remove(link)
		if err := os.Symlink(filepath.Join(k.Dir, "skills", n), link); err != nil {
			return "", err
		}
	}
	return plugin, nil
}

func sessionID(provider string, stdout []byte) string {
	switch provider {
	case "claude", "", "fake": // a fake worker may print {"session_id": ...} like claude does
		var v struct {
			SessionID string `json:"session_id"`
		}
		if json.Unmarshal(bytes.TrimSpace(stdout), &v) == nil {
			return v.SessionID
		}
	case "codex":
		sc := bufio.NewScanner(bytes.NewReader(stdout))
		sc.Buffer(make([]byte, 1<<20), 1<<24)
		for sc.Scan() {
			var v struct {
				Type     string `json:"type"`
				ThreadID string `json:"thread_id"`
			}
			if json.Unmarshal(sc.Bytes(), &v) == nil && v.Type == "thread.started" {
				return v.ThreadID
			}
		}
	}
	return ""
}

// workerUsage is what claude's JSON result says the attempt cost. Its total_cost_usd counts
// the whole session, so a resumed session reports a running total (session_cost_usd: take
// the last one per session); tokens and duration are this attempt's. Codex reports no cost.
func workerUsage(provider string, stdout []byte) map[string]any {
	if provider != "claude" && provider != "" && provider != "fake" {
		return nil
	}
	var v struct {
		Cost     *float64 `json:"total_cost_usd"`
		Duration int64    `json:"duration_ms"`
		Usage    struct {
			Input      int64 `json:"input_tokens"`
			Output     int64 `json:"output_tokens"`
			CacheRead  int64 `json:"cache_read_input_tokens"`
			CacheWrite int64 `json:"cache_creation_input_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(bytes.TrimSpace(stdout), &v) != nil || v.Cost == nil {
		return nil
	}
	return map[string]any{"session_cost_usd": *v.Cost, "duration_ms": v.Duration, "input_tokens": v.Usage.Input,
		"output_tokens": v.Usage.Output, "cache_read_tokens": v.Usage.CacheRead, "cache_write_tokens": v.Usage.CacheWrite}
}

func (e *Engine) agentExited(runID, tok string, res procResult) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if p := e.procs[runID]; p != nil && p.token == tok {
		delete(e.procs, runID)
	}
	e.stopTimer("kill:" + runID)
	r := e.runs[runID]
	if e.closed || r == nil || r.Step == nil || r.Step.Token != tok || Terminal(r.Status) {
		return
	}
	if sid := sessionID(r.Step.Provider, res.Stdout); sid != "" {
		r.Step.Session = sid
		if r.Sessions == nil {
			r.Sessions = map[string]string{}
		}
		r.Sessions[r.State] = r.Step.Provider + ":" + sid
	}
	ev := map[string]any{"state": r.State, "attempt": r.Step.Attempt, "exit": res.Exit,
		"timed_out": res.TimedOut, "stderr": tail(res.Stderr, 400)}
	if u := workerUsage(r.Step.Provider, res.Stdout); u != nil {
		maps.Copy(ev, u)
		ev["session"] = r.Step.Session
	}
	e.event(r, "worker.exited", ev)
	r.Step.Token = ""
	if p := r.Step.Pending; p != nil {
		r.Step.Pending = nil
		r.Step.Phase = "done"
		e.outcome(r, p.Outcome, p.Vars)
		return
	}
	if c := e.contracts[r.Step.Contract]; c != nil && c.Attempt == r.Step.Attempt {
		if c.Status == "resolved" {
			e.startAgent(r, &wake{resolution: c})
			return
		}
		r.Step.Phase, r.Status = "sleeping", StSleeping
		e.saveRun(r, "sleeping on "+c.ID)
		e.event(r, "run.sleeping", map[string]any{"state": r.State, "contract": c.ID, "aura": e.latestAura(r.ID, r.State) != nil})
		return
	}
	r.Step.Silent++
	why := fmt.Sprintf("exit %d", res.Exit)
	if res.TimedOut {
		why = "timed out"
	}
	if r.Step.Silent < 2 {
		e.startAgent(r, &wake{note: fmt.Sprintf("The previous attempt ended (%s) without running `kami step done`. Its last output:\n\n```\n%s\n```",
			why, tail(append(res.Stdout, res.Stderr...), 1500))})
		return
	}
	e.block(r, "stuck", fmt.Sprintf("%s: the worker stopped without finishing (%s)", r.State, why),
		failureBody(res, r.Step.Dir), []string{"retry", "stop"})
}

// --- worker callbacks -----------------------------------------------------------

func (e *Engine) workerRun(runID, tok string) (*Run, error) {
	r := e.runs[runID]
	if r == nil || r.Step == nil || tok == "" || r.Step.Token != tok {
		return nil, uerr("FORBIDDEN", "stale or unknown worker token for run %s", runID)
	}
	if r.Step.Kind != kamui.KAgent || r.Step.Phase != "running" {
		return nil, uerr("CONFLICT", "run %s is not running an agent step", runID)
	}
	return r, nil
}

// StepDone records a worker's outcome; it applies once the process exits.
func (e *Engine) StepDone(runID, tok, out string, vars map[string]any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, err := e.workerRun(runID, tok)
	if err != nil {
		return err
	}
	for k := range vars {
		if strings.HasPrefix(k, "kami_") { // the conversation a run speaks for: inputs and scripts only
			return uerr("FORBIDDEN", "var %s: kami_* vars are set by Kami, not by workers", k)
		}
	}
	st := e.def(r).States[r.State]
	if _, ok := st.Outcomes[out]; !ok {
		return uerr("INVALID", "outcome %q is not allowed in state %s; allowed: %s", out, r.State, strings.Join(st.Order, ", "))
	}
	if r.Step.Pending != nil {
		return uerr("CONFLICT", "outcome already reported (%s)", r.Step.Pending.Outcome)
	}
	if c := e.contracts[r.Step.Contract]; c != nil && c.Attempt == r.Step.Attempt && c.Status == "open" {
		return uerr("CONFLICT", "you raised contract %s this attempt; exit and wait for it instead of finishing", c.ID)
	}
	r.Step.Pending = &pending{Outcome: out, Vars: vars}
	e.saveRun(r, "reported "+out) // a restart before the worker exits must not lose it
	e.event(r, "step.reported", map[string]any{"state": r.State, "outcome": out})
	e.setTimer("kill:"+runID, now().Add(60*time.Second), func() {
		if p := e.procs[runID]; p != nil && p.token == tok {
			e.event(r, "worker.killed", map[string]any{"why": "still running 60s after kami step done"})
			p.kill()
		}
	})
	return nil
}

// ContractRequest is what a worker sends with `kami contract request`.
type ContractRequest struct {
	Kind    string            `json:"kind"`
	Title   string            `json:"title"`
	Body    string            `json:"body"`
	Options []string          `json:"options"`
	Default string            `json:"default"`
	Fields  map[string]string `json:"fields"`
}

// WorkerContract opens a contract raised mid-step. The worker then exits and sleeps.
func (e *Engine) WorkerContract(runID, tok string, req ContractRequest) (*Contract, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, err := e.workerRun(runID, tok)
	if err != nil {
		return nil, err
	}
	switch req.Kind {
	case kamui.KAsk, kamui.KChoice, kamui.KBlock, kamui.KForm:
	default:
		return nil, uerr("INVALID", "kind must be ask, choice, form or blocker, got %q", req.Kind)
	}
	if strings.TrimSpace(req.Title) == "" {
		return nil, uerr("INVALID", "title is required")
	}
	if r.Step.Pending != nil {
		return nil, uerr("CONFLICT", "outcome already reported; a finished step cannot raise a contract")
	}
	if c := e.contracts[r.Step.Contract]; c != nil && c.Attempt == r.Step.Attempt && c.Status == "open" {
		return nil, uerr("CONFLICT", "contract %s is already open for this attempt", c.ID)
	}
	for f, t := range req.Fields {
		if !kamui.ValidType(t) {
			return nil, uerr("INVALID", "field %q has unknown type %q", f, t)
		}
	}
	w := e.def(r)
	c := &Contract{ID: NewID(), RunID: r.ID, Step: r.State, Kekkai: r.Kekkai, Origin: "worker", Kind: req.Kind,
		Title: req.Title, Body: req.Body, Options: req.Options, Default: req.Default, Fields: req.Fields,
		Mode: w.AFK, Status: "open", Attempt: r.Step.Attempt, Created: now()}
	if w.AFK != nil {
		c.Resolver = w.AFK.Resolver
	}
	e.contracts[c.ID] = c
	r.Step.Contract = c.ID
	e.saveContract(c, "opened by worker")
	e.saveRun(r, "raised "+c.ID)
	e.event(r, "contract.opened", contractEvent(c))
	e.applyMode(c, true)
	return c, nil
}

// Snapshot stores an Aura for the worker's current step.
func (e *Engine) Snapshot(runID, tok string, a Aura) (*Aura, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, err := e.workerRun(runID, tok)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(a.Goal) == "" || strings.TrimSpace(a.Next) == "" {
		return nil, uerr("INVALID", "an aura needs goal and next")
	}
	a.RunID, a.Step, a.Created = r.ID, r.State, now()
	a.Seq = 1
	for _, x := range e.auras[r.ID] {
		if x.Step == r.State && x.Seq >= a.Seq {
			a.Seq = x.Seq + 1
		}
	}
	if c := e.contracts[r.Step.Contract]; c != nil && c.Attempt == r.Step.Attempt {
		a.WaitingOn = "contract:" + c.ID
	}
	w := e.def(r)
	resume := w.States[r.State].Step.Resume
	if resume == "" {
		resume = w.Resume
	}
	a.Session = &AuraSession{Provider: r.Step.Provider, ID: r.Step.Session, ResumeAllowed: resume == "session"}
	if err := e.Mem.AddAura(a.RunID, a.Step, a.Seq, a.Created.Format(time.RFC3339Nano), a); err != nil {
		return nil, fmt.Errorf("save aura: %w", err)
	}
	e.auras[r.ID] = append(e.auras[r.ID], &a)
	e.event(r, "aura.saved", map[string]any{"state": r.State, "seq": a.Seq})
	return &a, nil
}

// --- scripts and probes ---------------------------------------------------------

func (e *Engine) startScript(r *Run) {
	s := e.def(r).States[r.State].Step
	fail := func(err error) {
		e.block(r, "stuck", "script step "+r.State+" could not start", err.Error(), []string{"retry", "stop"})
	}
	k, err := kekkai.Load(e.Home, r.Kekkai)
	if err != nil {
		fail(err)
		return
	}
	r.Step.Attempt++
	dir, err := e.attemptDir(r)
	if err != nil {
		fail(err)
		return
	}
	spec, err := e.scriptSpec(r, s, s.Cmd, k, dir)
	if err != nil {
		fail(err)
		return
	}
	tok := token()
	r.Step.Token, r.Step.Phase, r.Step.Dir, r.Status = tok, "running", dir, StRunning
	runID := r.ID
	p, err := startProc(spec, tok, func(res procResult) { e.scriptExited(runID, tok, res) })
	if err != nil {
		fail(fmt.Errorf("start %s: %w", spec.Args[0], err))
		return
	}
	e.procs[r.ID] = p
	e.saveRun(r, "script")
	e.event(r, "script.started", map[string]any{"state": r.State, "args": spec.Args})
}

func (e *Engine) scriptSpec(r *Run, s *kamui.Step, cmd string, k *kekkai.Kekkai, dir string) (procSpec, error) {
	args, missing, err := commandArgs(cmd, r.Vars, k)
	if err != nil {
		return procSpec{}, err
	}
	if len(missing) > 0 {
		e.event(r, "template.missing", map[string]any{"state": r.State, "vars": missing})
	}
	cwd, err := stepCwd(s, r.Vars, k, dir)
	if err != nil {
		return procSpec{}, err
	}
	stdin, _ := json.Marshal(stepVars(r, s))
	timeout := s.Timeout
	if timeout == 0 {
		timeout = 10 * time.Minute
	}
	env := k.WorkerEnv(map[string]string{"PATH": e.workerPath(), "KAMI_SOCK": e.Sock, "KAMI_RUN": r.ID,
		"KAMI_STEP": r.State, "KAMI_STEP_DIR": dir, "KAMI_KEKKAI_DIR": k.Dir})
	return procSpec{Args: args, Dir: cwd, Env: env, Stdin: stdin, Timeout: timeout, LogDir: dir}, nil
}

// scriptOutcome reads {"outcome","vars"} from stdout, else ok/error by exit code.
func scriptOutcome(res procResult) (string, map[string]any) {
	if res.TimedOut {
		return "timeout", nil
	}
	if m, ok := lastJSON(res.Stdout); ok {
		if o, _ := m["outcome"].(string); o != "" {
			v, _ := m["vars"].(map[string]any)
			return o, v
		}
	}
	if res.Exit == 0 && res.Err == nil {
		return "ok", nil
	}
	return "error", nil
}

func (e *Engine) scriptExited(runID, tok string, res procResult) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if p := e.procs[runID]; p != nil && p.token == tok {
		delete(e.procs, runID)
	}
	r := e.runs[runID]
	if e.closed || r == nil || r.Step == nil || r.Step.Token != tok || Terminal(r.Status) {
		return
	}
	out, vars := scriptOutcome(res)
	r.Step.Token, r.Step.Phase = "", "done"
	e.event(r, "script.exited", map[string]any{"state": r.State, "exit": res.Exit, "outcome": out, "stderr": tail(res.Stderr, 400)})
	if _, ok := e.def(r).States[r.State].Outcomes[out]; !ok {
		e.block(r, "stuck", fmt.Sprintf("%s: the script ended with %q (exit %d), which has no next step", r.State, out, res.Exit),
			failureBody(res, r.Step.Dir), []string{"retry", "stop"})
		return
	}
	e.outcome(r, out, vars)
}
