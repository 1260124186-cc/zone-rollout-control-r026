package workflowprobe

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runLeaseReleaseReason() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	for _, name := range []string{"resolver-rel-01", "resolver-rel-02", "resolver-rel-03"} {
		if _, err := enrollNode(harness, name, "north"); err != nil {
			return err
		}
	}
	plan, _, err := createActivePlan(harness, "release.example", 5001, 1, 1, 60)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "rel-lease-0001", "north", 3)
	if err != nil {
		return err
	}
	if len(group.LeaseIDs) != 3 {
		return ensure(false, "release setup must allocate three leases")
	}

	// A keyed release records the reason category, note, and operation request.
	keyed := domain.ReleaseInput{
		RequestKey:     "rel-req-0001",
		ReasonCategory: "node_maintenance",
		ReasonNote:     "rack power maintenance window",
	}
	var first domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[0]+"/release", keyed, &first); err != nil {
		return err
	}
	if first.State != domain.LeaseReleased {
		return ensure(false, "keyed release did not finish the lease")
	}
	if first.ReleaseReasonCategory != domain.ReleaseReasonNodeMaintenance ||
		first.ReleaseReasonNote != keyed.ReasonNote ||
		first.ReleaseRequestKey != keyed.RequestKey ||
		first.FinishedAt == nil {
		return ensure(false, "keyed release did not persist the first reason")
	}

	// Replaying the identical request returns the original lease and reuses the
	// first reason; the terminal state and timestamp must not change.
	var replay domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[0]+"/release", keyed, &replay); err != nil {
		return err
	}
	if replay.ID != first.ID ||
		replay.State != first.State ||
		replay.ReleaseReasonCategory != first.ReleaseReasonCategory ||
		replay.ReleaseReasonNote != first.ReleaseReasonNote ||
		replay.ReleaseRequestKey != first.ReleaseRequestKey ||
		!replay.FinishedAt.Equal(*first.FinishedAt) {
		return ensure(false, "release replay did not return the original lease and reason")
	}

	// Same request key with different content is an idempotency conflict.
	differentContent := keyed
	differentContent.ReasonNote = "note was rewritten after handoff"
	err = harness.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[0]+"/release", differentContent, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}

	// A different operation request against the finished lease is a stable
	// conflict and cannot rewrite the terminal state.
	otherRequest := keyed
	otherRequest.RequestKey = "rel-req-0002"
	err = harness.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[0]+"/release", otherRequest, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseReleaseConflict)); err != nil {
		return err
	}

	// The node is ready again and the plan/node reads keep their old shape.
	var nodes domain.NodePage
	if err := harness.call(http.MethodGet, "/v1/nodes?region=north", nil, &nodes); err != nil {
		return err
	}
	nodeReady := false
	for _, node := range nodes.Items {
		if node.ID == first.NodeID {
			nodeReady = node.State == domain.NodeReady
		}
	}
	if !nodeReady {
		return ensure(false, "released node was not returned to ready")
	}
	var fetchedPlan domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &fetchedPlan); err != nil {
		return err
	}
	if fetchedPlan.State != domain.PlanActive {
		return ensure(false, "plan read changed shape after release")
	}

	// Legacy releases without a body still work, repeat safely, and carry no
	// reason; a later keyed request cannot claim the finished lease.
	var legacy domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[1]+"/release", nil, &legacy); err != nil {
		return err
	}
	if legacy.State != domain.LeaseReleased || legacy.ReleaseReasonCategory != "" || legacy.ReleaseRequestKey != "" {
		return ensure(false, "legacy release should finish without a reason")
	}
	var legacyReplay domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[1]+"/release", nil, &legacyReplay); err != nil {
		return err
	}
	if legacyReplay.ID != legacy.ID || !legacyReplay.FinishedAt.Equal(*legacy.FinishedAt) {
		return ensure(false, "legacy release replay changed the terminal lease")
	}
	err = harness.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[1]+"/release", domain.ReleaseInput{
		RequestKey:     "rel-req-0003",
		ReasonCategory: "unspecified",
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseReleaseConflict)); err != nil {
		return err
	}

	// Concurrent releases against one claimed lease serialize: exactly one
	// succeeds and the other receives the stable release conflict.
	concurrent := []domain.ReleaseInput{
		{RequestKey: "conc-rel-0001", ReasonCategory: "planned_complete", ReasonNote: "first handoff"},
		{RequestKey: "conc-rel-0002", ReasonCategory: "planned_complete", ReasonNote: "second handoff"},
	}
	results := make([]struct {
		lease domain.NodeLease
		err   error
	}, len(concurrent))
	var wg sync.WaitGroup
	for index, input := range concurrent {
		wg.Add(1)
		go func(index int, input domain.ReleaseInput) {
			defer wg.Done()
			results[index].err = harness.call(
				http.MethodPatch,
				"/v1/leases/"+group.LeaseIDs[2]+"/release",
				input,
				&results[index].lease,
			)
		}(index, input)
	}
	wg.Wait()
	successes, conflicts := 0, 0
	for _, outcome := range results {
		switch {
		case outcome.err == nil:
			successes++
		case expectFailure(outcome.err, http.StatusConflict, string(domain.CodeLeaseReleaseConflict)) == nil:
			conflicts++
		default:
			return outcome.err
		}
	}
	if successes != 1 || conflicts != 1 {
		return ensure(false, "concurrent releases must produce one success and one stable conflict")
	}

	// Unknown reason categories, overlong notes, short request keys, and unknown
	// fields are all validation failures before any state changes.
	invalid := []any{
		domain.ReleaseInput{RequestKey: "bad-rel-0001", ReasonCategory: "mystery", ReasonNote: "x"},
		domain.ReleaseInput{RequestKey: "bad-rel-0002", ReasonCategory: "unspecified", ReasonNote: strings.Repeat("长", 501)},
		domain.ReleaseInput{RequestKey: "short", ReasonCategory: "unspecified"},
		map[string]any{"request_key": "bad-rel-0003", "reason_category": "unspecified", "bogus": true},
	}
	for _, body := range invalid {
		if err := harness.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[0]+"/release", body, nil); err != nil {
			if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
				return err
			}
			continue
		}
		return ensure(false, "invalid release body must be rejected")
	}

	// An expired lease is a system terminal: release stays an idempotent read,
	// records no release reason, and appends no release audit event.
	expiredGroup, err := allocate(harness, plan.ID, "rel-lease-0002", "north", 1)
	if err != nil {
		return err
	}
	expiringLeaseID := expiredGroup.LeaseIDs[0]
	harness.clock.Advance(601 * time.Second)
	// A subsequent allocation runs the expiry sweep and frees the expired node.
	if _, err := allocate(harness, plan.ID, "rel-lease-0003", "north", 1); err != nil {
		return err
	}
	var expired domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+expiringLeaseID+"/release", domain.ReleaseInput{
		RequestKey:     "exp-rel-0001",
		ReasonCategory: "unspecified",
	}, &expired); err != nil {
		return err
	}
	if expired.State != domain.LeaseExpired || expired.ReleaseReasonCategory != "" || expired.ReleaseRequestKey != "" {
		return ensure(false, "expired lease release must preserve the expired terminal")
	}

	// Audit snapshot: each release appends exactly one event carrying lease,
	// plan, operation request, and reason; replays and expiries append nothing.
	snapshot, err := readSnapshot(harness)
	if err != nil {
		return err
	}
	firstEvents := releaseAuditEvents(snapshot, group.LeaseIDs[0])
	if len(firstEvents) != 1 {
		return ensure(false, "keyed release replay appended a second audit event")
	}
	attributes := firstEvents[0].Attributes
	if attributes["lease_id"] != group.LeaseIDs[0] ||
		attributes["plan_id"] != plan.ID ||
		attributes["request_key"] != keyed.RequestKey ||
		attributes["reason_category"] != keyed.ReasonCategory ||
		attributes["reason_note"] != keyed.ReasonNote {
		return ensure(false, "release audit must capture lease, plan, request key, category, and note")
	}
	legacyEvents := releaseAuditEvents(snapshot, group.LeaseIDs[1])
	if len(legacyEvents) != 1 {
		return ensure(false, "legacy release must append exactly one audit event")
	}
	if _, ok := legacyEvents[0].Attributes["reason_category"]; ok {
		return ensure(false, "legacy release audit must not carry a reason")
	}
	concurrentEvents := releaseAuditEvents(snapshot, group.LeaseIDs[2])
	if len(concurrentEvents) != 1 {
		return ensure(false, "concurrent losing release must not append an audit event")
	}
	if count := len(releaseAuditEvents(snapshot, expiringLeaseID)); count != 0 {
		return ensure(false, "expired lease release must not append a release audit event")
	}
	return nil
}

func readSnapshot(harness *harness) (domain.State, error) {
	payload, err := os.ReadFile(harness.tempDir + "/state.json")
	if err != nil {
		return domain.State{}, err
	}
	var state domain.State
	if err := json.Unmarshal(payload, &state); err != nil {
		return domain.State{}, err
	}
	return state, nil
}

func releaseAuditEvents(state domain.State, leaseID string) []domain.AuditEvent {
	events := make([]domain.AuditEvent, 0)
	for _, event := range state.AuditEvents {
		if event.EntityType == "lease" && event.EntityID == leaseID && event.Action == "released" {
			events = append(events, event)
		}
	}
	return events
}
