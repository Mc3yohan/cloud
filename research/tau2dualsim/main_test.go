// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0 OR MIT

package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	tc "github.com/hanzoai/cloud/research/tau2dualcontrol"
)

func TestCLIEndToEndJSONAndTableOutput(t *testing.T) {
	tmp := t.TempDir()
	binName := "tau2dualsim"
	goBinName := "go"
	if runtime.GOOS == "windows" {
		binName += ".exe"
		goBinName += ".exe"
	}
	bin := filepath.Join(tmp, binName)
	outJSON := filepath.Join(tmp, "results.json")

	goCmdPath := filepath.Join(runtime.GOROOT(), "bin", goBinName)
	buildCmd := exec.Command(goCmdPath, "build", "-o", bin, ".")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, string(out))
	}

	runCmd := exec.Command(bin, "-episodes", "10", "-out", outJSON)
	stdout, err := runCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cli execution failed: %v\n%s", err, string(stdout))
	}
	if len(stdout) == 0 {
		t.Fatal("expected formatted summary table on stdout")
	}

	raw, err := os.ReadFile(outJSON)
	if err != nil {
		t.Fatalf("read output json: %v", err)
	}
	var rep tc.MatrixReport
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatalf("unmarshal output json: %v", err)
	}
	if rep.EpisodesPerCell != 10 || len(rep.Cells) != 36 {
		t.Fatalf("unexpected report dimensions: episodes=%d cells=%d", rep.EpisodesPerCell, len(rep.Cells))
	}
}
