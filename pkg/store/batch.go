package store

import (
	"context"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipni/go-libipni/dagsync/ipnisync/head"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ContextRecord is what a store maps a provider and context ID to: the
// entries chunk advertised under the context ID and the metadata last
// advertised with it. A nil field is a missing mapping.
type ContextRecord struct {
	ChunkLink ipld.Link
	Metadata  *metadata.Metadata
}

// ContextChange is one context ID's record before and after a batch.
type ContextChange struct {
	ContextID []byte
	Old       ContextRecord
	New       ContextRecord
}

// BatchStore is what a batch of advertisements is published through: one
// read of the records it builds on, one write of its adverts, and one commit
// of its records and the head.
type BatchStore interface {
	// LoadContexts returns the record of every given context ID that has a
	// mapping, keyed by the context ID as a string. A context ID with no
	// mapping may be absent from the map.
	LoadContexts(ctx context.Context, p peer.ID, contextIDs [][]byte) (map[string]ContextRecord, error)
	// PutAdverts writes signed advertisements. They are content addressed and
	// nothing links to them until the head does, so adverts a failed batch
	// wrote can stay.
	PutAdverts(ctx context.Context, advs []schema.Advertisement) error
	// CommitBatch writes the New record of every change, then replaces
	// oldHead with newHead, and fails with [ErrPreconditionFailed] when the
	// head is no longer oldHead. A nil oldHead means no head exists yet. When
	// it fails, the store must hold the Old records, by rolling back a
	// transaction or by writing them back.
	CommitBatch(ctx context.Context, p peer.ID, changes []ContextChange, oldHead, newHead *head.SignedHead) error
}
