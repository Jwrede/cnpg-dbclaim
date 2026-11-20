package controller

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Run reconciles every resync interval and soon after any watched object
// changes, until ctx is done.
func (c *Controller) Run(ctx context.Context, resync time.Duration) {
	trigger := make(chan struct{}, 1)
	poke := func() {
		select {
		case trigger <- struct{}{}:
		default:
		}
	}
	type w struct {
		gvr      schema.GroupVersionResource
		selector string
	}
	watches := []w{{ClaimGVR, ""}, {GrantGVR, ""}, {DatabaseGVR, LabelManagedBy + "=" + ManagedBy}, {RoleGVR, LabelManagedBy + "=" + ManagedBy}}
	nss := c.ClusterNamespaces
	for _, x := range watches {
		targets := []string{""}
		if x.gvr != ClaimGVR && len(nss) > 0 {
			targets = nss
		}
		for _, ns := range targets {
			go c.watch(ctx, x.gvr, ns, x.selector, poke)
		}
	}
	t := time.NewTicker(resync)
	defer t.Stop()
	for {
		if err := c.Reconcile(ctx); err != nil {
			c.logf("reconcile: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-trigger:
			// Let a burst of events settle into one pass.
			select {
			case <-ctx.Done():
				return
			case <-time.After(300 * time.Millisecond):
			}
			select {
			case <-trigger:
			default:
			}
		}
	}
}

func (c *Controller) watch(ctx context.Context, gvr schema.GroupVersionResource, ns, selector string, poke func()) {
	for ctx.Err() == nil {
		wi, err := c.Client.Resource(gvr).Namespace(ns).Watch(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			c.logf("watch %s: %v", gvr.Resource, err)
		} else {
			for range wi.ResultChan() {
				poke()
			}
			wi.Stop()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}
