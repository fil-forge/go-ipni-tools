# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

- `make build` — `go build ./...`
- `make test` — `go test ./...`
- `make vet` — `go vet ./...`
- Single test: `go test ./pkg/publisher/ -run TestName`

Go 1.25.7. CI runs `go-test` and `go-check` (vet/lint/staticcheck via ipdxco unified workflows).

## What this is

A Go **library** (no `main`/`cmd`) of building blocks for operating an IPNI
(InterPlanetary Network Indexer) advertisement publisher: generating, signing,
chaining, storing, serving, and announcing advertisements, plus tracking when
remote indexers have synced. Consumers wire the packages together.

## Architecture

The data model is an IPNI advertisement chain: each advertisement links to its
predecessor via `PreviousID`, and a signed **head** points at the latest one.
Advertisement `Entries` are themselves a chunked linked list of multihashes
(chunk size `store.MaxEntryChunkSize`).

**`pkg/store`** — the storage layer and the spine everything else depends on. It
defines a deliberate hierarchy of narrow interfaces (`SimpleStore` → `Store` →
`PublisherStore` → `FullStore`); each consuming package takes the narrowest one
it needs. `AdStore` is the concrete implementation, constructed via
`FromDatastore` (go-datastore backend) or `FromLocalStore` (filesystem
directory for blocks + datastore for the index tables). It persists four things:
advertisements, entry chunks, the signed head, and `(provider, contextID) →
chunkLink` / `→ metadata` tables. `Store.Replace` / `ReplaceHead` implement
**conditional writes** (`ErrPreconditionFailed`) — the mechanism that guards
against losing advertisements under concurrent publishing.

**`pkg/publisher`** — turns content into published advertisements.
`GenerateAd` builds a `schema.Advertisement`: it reuses an existing chunk link
for a known `(provider, contextID)`, returns `ErrAlreadyAdvertised` when
metadata is unchanged, and produces removal ads (`IsRm`). `AdvertisementPublisher`
batches ads (`AddToBatch` → `Commit`): on commit it links each ad to the current
head, signs, stores, swaps the signed head, and optionally fires HTTP
announcements to indexers. `IPNIPublisher` is the high-level synchronous
`Publisher`; `AsyncFrom` adapts it to `AsyncPublisher`.
**Concurrency caveat:** `Publish` is not safe for concurrent use — callers must
serialize it (mutex) or advertisements can be lost.

**`pkg/queue`** — async + batched publishing on top of `libforge/jobqueue`.
`Queue[Job]` is a generic queue interface; `QueuePoller[Job]` polls a queue and
dispatches to a single- or batch-`Handler`. `QueuePublisher` and
`AdvertisementQueuePublisher` implement `publisher.AsyncPublisher` by enqueueing
jobs instead of publishing inline — this is how you publish from many goroutines
safely (the poller drains the queue serially).

**`pkg/server`** — `http.Handler` that serves advertisements and the head by CID
at `/ipni/v1/ad/{ad}` (path `head` serves the signed head). This is the endpoint
indexers fetch the advertisement chain from.

**`pkg/notifier`** — polls a remote IPNI indexer's `GetProvider` to detect when
it has ingested the provider's newest advertisement, then invokes registered
`NotifyRemoteSyncFunc` callbacks. `HeadState` persists the last-seen remote head
in a `store.SimpleStore`.

**`pkg/client`** — `TieredFinder` wraps multiple `client.Finder`s and tries them
in order; an empty result (no error) falls through to the next tier.

**`internal/testutil`** — random multihash/peer generators for tests.
