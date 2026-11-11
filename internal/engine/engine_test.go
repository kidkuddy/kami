package engine

import (
	"errors"
	"testing"

	"kami/internal/eventbus"
	"kami/internal/kamui"
	"kami/internal/state"
)

// newTestEngine returns an engine wired for in-memory unit tests: a real event
// bus (sqlite, no external binary) and a memento whose writes fail silently
// (memento is not required to run these tests).
func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	bus, err := eventbus.Open(t.TempDir() + "/events.db")
	if err != nil {
		t.Fatalf("open eventbus: %v", err)
	}
	t.Cleanup(func() { bus.Close() })
	return New(t.TempDir(), &state.Memento{}, bus, "", "")
}

// workflow returns a tiny two-state workflow: a --ok--> b(done).
func wf(t *testing.T) *kamui.Workflow {
	t.Helper()
	return &kamui.Workflow{
		Name:  "t",
		Start: "a",
		States: map[string]*kamui.State{
			"a": {Name: "a", Step: &kamui.Step{Kind: kamui.KAgent},
				Outcomes: map[string]string{"ok": "b"}, Order: []string{"ok"}},
			"b": {Name: "b", Step: &kamui.Step{Kind: kamui.KDone}},
		},
		Order: []string{"a", "b"},
	}
}

func TestOutcomeFiltersKamiVars(t *testing.T) {
	e := newTestEngine(t)
	w := wf(t)
	e.defs["hash"] = w
	r := &Run{ID: "r1", State: "a", Status: StRunning, DefHash: "hash",
		Vars: map[string]any{}, Visits: map[string]int{}}

	e.outcome(r, "ok", map[string]any{
		"kami_foo": "refused",
		"good":     "kept",
		"kami_bar": "refused too",
	})

	if r.Vars["good"] != "kept" {
		t.Fatalf("good var not kept: %v", r.Vars)
	}
	if _, ok := r.Vars["kami_foo"]; ok {
		t.Fatalf("kami_* var was not filtered: %v", r.Vars)
	}
	if _, ok := r.Vars["kami_bar"]; ok {
		t.Fatalf("kami_* var was not filtered: %v", r.Vars)
	}
	if r.Status != StDone {
		t.Fatalf("run did not finish: %s", r.Status)
	}
}

// wfSpawn returns a workflow where state "a" is a spawn with any_failed ->
// fail(KFail) and all_done -> done(KDone).
func wfSpawn(t *testing.T) *kamui.Workflow {
	t.Helper()
	return &kamui.Workflow{
		Name:  "t",
		Start: "a",
		States: map[string]*kamui.State{
			"a": {Name: "a", Step: &kamui.Step{Kind: kamui.KSpawn},
				Outcomes: map[string]string{"any_failed": "fail", "all_done": "done"},
				Order:    []string{"all_done", "any_failed"}},
			"fail": {Name: "fail", Step: &kamui.Step{Kind: kamui.KFail}},
			"done": {Name: "done", Step: &kamui.Step{Kind: kamui.KDone}},
		},
		Order: []string{"a", "fail", "done"},
	}
}

func TestCheckChildrenMissingChild(t *testing.T) {
	e := newTestEngine(t)
	w := wfSpawn(t)
	e.defs["hash"] = w
	r := &Run{ID: "r1", State: "a", Status: StRunning, DefHash: "hash",
		Step: &StepState{Kind: kamui.KSpawn, Phase: "waiting", Children: []string{"missing"}},
		Vars: map[string]any{}, Visits: map[string]int{}}

	e.checkChildren(r)

	if r.Status != StFailed {
		t.Fatalf("missing child should fail the run: %s", r.Status)
	}
	if r.State != "fail" {
		t.Fatalf("run should have moved to fail state: %s", r.State)
	}
	if r.Step == nil || r.Step.Phase != "done" {
		t.Fatalf("step phase should be done: %+v", r.Step)
	}
}

func TestSystemRefusesLockedContract(t *testing.T) {
	e := newTestEngine(t)
	r := &Run{ID: "r1", Status: StRunning}
	e.runs["r1"] = r
	e.contracts["c1"] = &Contract{ID: "c1", RunID: "r1", Status: "open", Lock: true}

	_, err := e.System("r1", "a system instruction", "", "", nil)
	if err == nil {
		t.Fatal("expected an error for a locked contract")
	}
	var ue *UserError
	if !errors.As(err, &ue) || ue.Code != "FORBIDDEN" {
		t.Fatalf("expected FORBIDDEN, got %v", err)
	}
}

func TestChooseTargetsAndValidate(t *testing.T) {
	src := `
workflow(name = "t", start = "a")
def pick(v):
    return "b"
state("a", choose(pick))
state("b", done())
`
	w, err := kamui.Load("workflows/t.star", kamui.BundleReader(map[string]string{
		"workflows/t.star": src,
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	targets, complete := kamui.ChooseTargets(w, w.States["a"].Step.Fn)
	if !complete {
		t.Fatalf("choose targets not complete: %v", targets)
	}
	if len(targets) != 1 || targets[0] != "b" {
		t.Fatalf("unexpected choose targets: %v", targets)
	}

	if problems := kamui.Validate(w, nil); len(problems) > 0 {
		t.Fatalf("unexpected validation problems: %v", problems)
	}
}
