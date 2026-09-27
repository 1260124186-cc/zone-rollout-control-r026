package workflowprobe

import (
	"net/http"
	"sync"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runLeaseReleaseReasons() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-west-01", "west"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-west-02", "west"); err != nil {
		return err
	}
	plan, _, err := createActivePlan(harness, "release.example", 3077, 1, 1, 100)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "release-lease-0001", "west", 2)
	if err != nil {
		return err
	}
	if len(group.LeaseIDs) != 2 {
		return ensure(false, "release scenario needs two leases")
	}
	leaseA := group.LeaseIDs[0]
	leaseB := group.LeaseIDs[1]

	// Unknown reason categories are rejected with a stable validation error.
	err = releaseWithReason(harness, leaseA, "bogus-category", "not a controlled category")
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}

	// A release may carry a reason category and a note.
	released, err := releaseLease(harness, leaseA, map[string]any{
		"reason_category": "handover",
		"reason_note":     "site handover to night shift",
	})
	if err != nil {
		return err
	}
	if released.State != domain.LeaseReleased {
		return ensure(false, "release with reason did not update state")
	}
	if released.ReleaseReasonCategory != "handover" || released.ReleaseReasonNote != "site handover to night shift" {
		return ensure(false, "release reason was not stored on the lease")
	}

	// The release audit event links the reason to lease, plan, and request.
	if err := expectReleaseAudit(harness, leaseA, plan.ID, "handover", "site handover to night shift"); err != nil {
		return err
	}

	// A bodiless repeat returns the original lease and reuses the first reason.
	replay, err := releaseLease(harness, leaseA, nil)
	if err != nil {
		return err
	}
	if replay.ID != released.ID || replay.State != domain.LeaseReleased {
		return ensure(false, "bodiless repeat release did not return the original lease")
	}
	if replay.ReleaseReasonCategory != "handover" || replay.ReleaseReasonNote != "site handover to night shift" {
		return ensure(false, "repeat release did not reuse the first reason")
	}
	if replay.FinishedAt == nil || !replay.FinishedAt.Equal(*released.FinishedAt) {
		return ensure(false, "repeat release rewrote the terminal state")
	}

	// Repeating with the identical reason is also idempotent.
	identical, err := releaseLease(harness, leaseA, map[string]any{
		"reason_category": "handover",
		"reason_note":     "site handover to night shift",
	})
	if err != nil {
		return err
	}
	if identical.ID != released.ID || !identical.FinishedAt.Equal(*released.FinishedAt) {
		return ensure(false, "identical repeat release changed the lease")
	}

	// A repeat carrying a different reason is a stable conflict.
	_, err = releaseLease(harness, leaseA, map[string]any{
		"reason_category": "maintenance",
	})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeConflict)); err != nil {
		return err
	}

	// Concurrent releases with different reasons: exactly one wins, the other
	// receives the stable conflict error.
	if err := expectConcurrentRelease(harness, leaseB); err != nil {
		return err
	}

	// A reported lease cannot be released, with or without a reason.
	probeGroup, err := allocate(harness, plan.ID, "release-lease-0002", "west", 2)
	if err != nil {
		return err
	}
	reportedLease := probeGroup.LeaseIDs[0]
	if _, err := submitProbe(harness, reportedLease, "release-probe-0001", true, 3077, 12); err != nil {
		return err
	}
	_, err = releaseLease(harness, reportedLease, map[string]any{
		"reason_category": "handover",
	})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	// Bodiless releases keep working on a claimed lease.
	bodilessGroup, err := allocate(harness, plan.ID, "release-lease-0003", "west", 1)
	if err != nil {
		return err
	}
	plain, err := releaseLease(harness, bodilessGroup.LeaseIDs[0], nil)
	if err != nil {
		return err
	}
	if plain.State != domain.LeaseReleased || plain.ReleaseReasonCategory != "" || plain.ReleaseReasonNote != "" {
		return ensure(false, "bodiless release must succeed without a reason")
	}
	return nil
}

func releaseLease(harness *harness, leaseID string, body any) (domain.NodeLease, error) {
	var lease domain.NodeLease
	err := harness.call(http.MethodPatch, "/v1/leases/"+leaseID+"/release", body, &lease)
	return lease, err
}

func releaseWithReason(harness *harness, leaseID string, category string, note string) error {
	return harness.call(http.MethodPatch, "/v1/leases/"+leaseID+"/release", map[string]any{
		"reason_category": category,
		"reason_note":     note,
	}, nil)
}

func expectReleaseAudit(harness *harness, leaseID string, planID string, category string, note string) error {
	events, err := harness.auditEvents()
	if err != nil {
		return err
	}
	for _, event := range events {
		if event.EntityType != "lease" || event.EntityID != leaseID || event.Action != "released" {
			continue
		}
		if event.Attributes["lease_id"] != leaseID {
			return ensure(false, "release audit missed the lease reference")
		}
		if event.Attributes["plan_id"] != planID {
			return ensure(false, "release audit missed the plan reference")
		}
		if event.Attributes["node_id"] == "" {
			return ensure(false, "release audit missed the node reference")
		}
		if event.Attributes["reason_category"] != category || event.Attributes["reason_note"] != note {
			return ensure(false, "release audit missed the submitted reason")
		}
		return nil
	}
	return ensure(false, "release audit event was not appended")
}

func expectConcurrentRelease(harness *harness, leaseID string) error {
	type outcome struct {
		lease domain.NodeLease
		err   error
	}
	results := make(chan outcome, 2)
	var wg sync.WaitGroup
	for _, category := range []string{"maintenance", "node_fault"} {
		wg.Add(1)
		go func(category string) {
			defer wg.Done()
			lease, err := releaseLease(harness, leaseID, map[string]any{
				"reason_category": category,
				"reason_note":     "concurrent release probe",
			})
			results <- outcome{lease: lease, err: err}
		}(category)
	}
	wg.Wait()
	close(results)

	succeeded := 0
	conflicted := 0
	var winner domain.NodeLease
	for result := range results {
		if result.err == nil {
			succeeded++
			winner = result.lease
			continue
		}
		if err := expectFailure(result.err, http.StatusConflict, string(domain.CodeConflict)); err != nil {
			return err
		}
		conflicted++
	}
	if succeeded != 1 || conflicted != 1 {
		return ensure(false, "concurrent releases must produce exactly one success and one conflict")
	}
	if winner.State != domain.LeaseReleased || winner.ReleaseReasonCategory == "" {
		return ensure(false, "concurrent release winner lost its reason")
	}
	return nil
}
