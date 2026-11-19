// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0 OR MIT

// Command tau2dualsim runs the 36-cell dual-control business-agent benchmark
// matrix from research/tau2dualcontrol (hanzoai/cloud#30, HIP-1348 / HIP-1347
// §2.9), prints the before/after summary tables, and optionally writes the raw
// machine-readable JSON results to disk.
package main

import (
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	tc "github.com/hanzoai/cloud/research/tau2dualcontrol"
)

func main() {
	out := flag.String("out", "", "optional path to write raw JSON results")
	episodes := flag.Int("episodes", tc.DefaultEpisodesPerCell, "episodes per (regime, mode, seed) cell")
	flag.Parse()

	start := time.Now()
	report := tc.RunMatrix(*episodes)
	elapsed := time.Since(start)

	if *out != "" {
		raw, err := tc.MarshalMatrixJSON(report)
		if err != nil {
			fmt.Fprintln(os.Stderr, "marshal results:", err)
			os.Exit(1)
		}
		if err := os.WriteFile(*out, raw, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "write results:", err)
			os.Exit(1)
		}
	}

	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintf(w, "=== Aggregate Summary Across All Regimes & Seeds (%d episodes/mode, elapsed %s) ===\n",
		*episodes*len(tc.AllRegimes())*len(report.Seeds), elapsed.Round(time.Millisecond))
	fmt.Fprintln(w, "MODE\tEPISODES\tSUCCESS\tSAFETY_VIOL\tLOST_UPD\tDBL_REFUND\tDUP_RETRY\tPERM_VIOL\tSAFE_HANDOFF\tREPLAY_OK\tMEAN_CALLS")
	for _, mode := range tc.AllModes() {
		ms := report.AggregatedByMode[string(mode)]
		fmt.Fprintf(w, "%s\t%d\t%.2f%%\t%.2f%%\t%d\t%d\t%d\t%d\t%d\t%.2f%%\t%.2f\n",
			ms.Mode,
			ms.TotalEpisodes,
			ms.TaskSuccessRate*100,
			ms.SafetyViolationRate*100,
			ms.LostUpdateCount,
			ms.DoubleRefundCount,
			ms.DuplicateEffectCount,
			ms.PermViolationCount,
			ms.SafeHandoffCount,
			ms.ReplayVerifiedRate*100,
			ms.MeanStoreCallsPerEp,
		)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "=== Per-Regime Breakdown (Averaged Across 3 Seeds) ===")
	fmt.Fprintln(w, "REGIME\tMODE\tSUCCESS\tSAFETY_VIOL\tLOST_UPD\tDBL_REFUND\tDUP_RETRY\tPERM_VIOL\tHANDOFF\tP95_CALLS")
	for _, regime := range tc.AllRegimes() {
		for _, mode := range tc.AllModes() {
			var succ, viol float64
			var lost, dbl, dup, perm, handoff, p95Max int
			n := 0
			for _, cell := range report.Cells {
				if cell.Regime == regime && cell.Mode == mode {
					n++
					succ += cell.TaskSuccessRate
					viol += cell.SafetyViolationRate
					lost += cell.LostUpdateCount
					dbl += cell.DoubleRefundCount
					dup += cell.DuplicateEffectCount
					perm += cell.PermViolationCount
					handoff += cell.SafeLeaseHandoffCount
					if cell.P95StoreCallsPerEp > p95Max {
						p95Max = cell.P95StoreCallsPerEp
					}
				}
			}
			if n > 0 {
				fmt.Fprintf(w, "%s\t%s\t%.2f%%\t%.2f%%\t%d\t%d\t%d\t%d\t%d\t%d\n",
					regime, mode, (succ/float64(n))*100, (viol/float64(n))*100,
					lost, dbl, dup, perm, handoff, p95Max)
			}
		}
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "=== Concurrent Goroutine Race Check (%d overlapping race trials/mode) ===\n", report.ConcurrentRaceCheck.TrialsPerMode)
	fmt.Fprintf(w, "dual_rbac_lww double-refund violations:  %d / %d\n",
		report.ConcurrentRaceCheck.LWWDoubleRefundViolations, report.ConcurrentRaceCheck.TrialsPerMode)
	fmt.Fprintf(w, "dual_cas_lease double-refund violations: %d / %d (replay verified: %d / %d)\n",
		report.ConcurrentRaceCheck.CASLeaseDoubleRefundViolations,
		report.ConcurrentRaceCheck.TrialsPerMode,
		report.ConcurrentRaceCheck.CASLeaseReplayVerified,
		report.ConcurrentRaceCheck.TrialsPerMode)
	_ = w.Flush()
}
