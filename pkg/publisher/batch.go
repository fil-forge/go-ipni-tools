package publisher

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/dagsync/ipnisync/head"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/fil-forge/go-ipni-tools/pkg/store"
)

// publishBatch is PublishBatch: it reads the records of every spec with one
// [store.BatchStore.LoadContexts] call, plans the adverts in memory, writes
// them with one [store.BatchStore.PutAdverts] call, and writes the records
// and moves the head with one [store.BatchStore.CommitBatch] call.
func (p *IPNIPublisher) publishBatch(ctx context.Context, provider peer.AddrInfo, specs []AdvertSpec) (ipld.Link, error) {
	oldHead, err := p.store.Head(ctx)
	if err != nil && !store.IsNotFound(err) {
		return nil, fmt.Errorf("reading IPNI head: %w", err)
	}
	var prevLink ipld.Link
	if oldHead != nil {
		prevLink = oldHead.Head
	}

	contextIDs := make([][]byte, len(specs))
	for i, spec := range specs {
		contextIDs[i] = []byte(spec.ContextID)
	}
	records, err := p.batchStore.LoadContexts(ctx, provider.ID, contextIDs)
	if err != nil {
		return nil, fmt.Errorf("reading IPNI mappings: %w", err)
	}

	var stringAddrs []string
	for _, addr := range provider.Addrs {
		stringAddrs = append(stringAddrs, addr.String())
	}
	// changeIndex finds a context ID's change, so a context ID advertised
	// twice in one batch has one change, from the record it was loaded with to
	// the one its last advertisement left.
	changeIndex := map[string]int{}
	var changes []store.ContextChange
	var advs []schema.Advertisement
	for _, spec := range specs {
		contextID := []byte(spec.ContextID)
		current := records[spec.ContextID]
		i, seen := changeIndex[spec.ContextID]
		if seen {
			current = changes[i].New
		}
		log := log.With("providerID", provider.ID).With("contextID", base64.StdEncoding.EncodeToString(contextID))
		next, err := nextRecord(log, current, spec.Metadata, func() (ipld.Link, error) {
			return p.store.PutEntries(ctx, spec.Digests)
		})
		if errors.Is(err, ErrAlreadyAdvertised) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("generating IPNI advert: %w", err)
		}
		if seen {
			changes[i].New = next
		} else {
			changeIndex[spec.ContextID] = len(changes)
			changes = append(changes, store.ContextChange{ContextID: contextID, Old: current, New: next})
		}

		mdBytes, err := spec.Metadata.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("encoding IPNI metadata: %w", err)
		}
		adv := schema.Advertisement{
			PreviousID: prevLink,
			Provider:   provider.ID.String(),
			Addresses:  stringAddrs,
			Entries:    next.ChunkLink,
			ContextID:  contextID,
			Metadata:   mdBytes,
		}
		if err := adv.Sign(p.batchPublisher.key); err != nil {
			return nil, fmt.Errorf("signing IPNI advert: %w", err)
		}
		if err := adv.Validate(); err != nil {
			return nil, fmt.Errorf("validating IPNI advert: %w", err)
		}
		if prevLink, err = store.PutAdvert(ctx, linkOnly{}, adv); err != nil {
			return nil, fmt.Errorf("encoding IPNI advert: %w", err)
		}
		advs = append(advs, adv)
	}

	if len(advs) == 0 {
		log.Info("No new advertisements to commit")
		return prevLink, nil
	}
	if err := p.batchStore.PutAdverts(ctx, advs); err != nil {
		return nil, fmt.Errorf("writing %d IPNI adverts: %w", len(advs), err)
	}
	bp := p.batchPublisher
	newHead, err := head.NewSignedHead(prevLink.(cidlink.Link).Cid, bp.topic, bp.key)
	if err != nil {
		return nil, fmt.Errorf("signing IPNI head: %w", err)
	}
	if err := p.batchStore.CommitBatch(ctx, provider.ID, changes, oldHead, newHead); err != nil {
		return nil, fmt.Errorf("committing %d IPNI mappings and the head: %w", len(changes), err)
	}
	log.Info("Updated reference to the latest advertisement successfully")

	if bp.sender != nil {
		if err := announce.Send(ctx, prevLink.(cidlink.Link).Cid, bp.pubHTTPAnnounceAddrs, bp.sender); err != nil {
			log.Warnw("Failed to announce advertisement", "err", err)
		}
	}
	return prevLink, nil
}

// linkOnly is a block store that keeps nothing, for computing an advert's
// link before the advert is written.
type linkOnly struct{}

func (linkOnly) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, errors.New("linkOnly keeps no blocks")
}

func (linkOnly) Put(context.Context, string, uint64, io.Reader) error {
	return nil
}

// perItemBatchStore publishes a batch through a PublisherStore's own calls,
// one per mapping. It has no transaction to roll back, so a commit that fails
// writes back the records it changed.
type perItemBatchStore struct {
	store.PublisherStore
}

var _ store.BatchStore = perItemBatchStore{}

func (s perItemBatchStore) LoadContexts(ctx context.Context, p peer.ID, contextIDs [][]byte) (map[string]store.ContextRecord, error) {
	records := make(map[string]store.ContextRecord, len(contextIDs))
	for _, contextID := range contextIDs {
		if _, ok := records[string(contextID)]; ok {
			continue
		}
		var record store.ContextRecord
		chunkLink, err := s.ChunkLinkForProviderAndContextID(ctx, p, contextID)
		switch {
		case err == nil:
			record.ChunkLink = chunkLink
		case !store.IsNotFound(err):
			return nil, fmt.Errorf("reading entries mapping for context %x: %w", contextID, err)
		}
		md, err := s.MetadataForProviderAndContextID(ctx, p, contextID)
		switch {
		case err == nil:
			record.Metadata = &md
		case !store.IsNotFound(err):
			return nil, fmt.Errorf("reading metadata mapping for context %x: %w", contextID, err)
		}
		records[string(contextID)] = record
	}
	return records, nil
}

func (s perItemBatchStore) PutAdverts(ctx context.Context, advs []schema.Advertisement) error {
	for _, adv := range advs {
		if _, err := s.PutAdvert(ctx, adv); err != nil {
			return err
		}
	}
	return nil
}

// CommitBatch writes the records, then moves the head. When either fails, it
// writes back the Old record of every change it began writing, and reports
// any write-back that failed beside the cause.
func (s perItemBatchStore) CommitBatch(ctx context.Context, p peer.ID, changes []store.ContextChange, oldHead, newHead *head.SignedHead) error {
	for i, c := range changes {
		if err := s.writeRecord(ctx, p, c); err != nil {
			return s.rollback(ctx, p, changes[:i+1], fmt.Errorf("writing mappings for context %x: %w", c.ContextID, err))
		}
	}
	if _, err := s.ReplaceHead(ctx, oldHead, newHead); err != nil {
		return s.rollback(ctx, p, changes, fmt.Errorf("replacing head: %w", err))
	}
	return nil
}

// writeRecord writes the mappings that differ between the change's Old and
// New records.
func (s perItemBatchStore) writeRecord(ctx context.Context, p peer.ID, c store.ContextChange) error {
	switch {
	case c.New.ChunkLink == nil && c.Old.ChunkLink != nil:
		if err := s.DeleteChunkLinkForProviderAndContextID(ctx, p, c.ContextID); err != nil {
			return err
		}
	case c.New.ChunkLink != nil && (c.Old.ChunkLink == nil || c.New.ChunkLink.String() != c.Old.ChunkLink.String()):
		if err := s.PutChunkLinkForProviderAndContextID(ctx, p, c.ContextID, c.New.ChunkLink); err != nil {
			return err
		}
	}
	switch {
	case c.New.Metadata == nil && c.Old.Metadata != nil:
		return s.DeleteMetadataForProviderAndContextID(ctx, p, c.ContextID)
	case c.New.Metadata != nil && (c.Old.Metadata == nil || !c.New.Metadata.Equal(*c.Old.Metadata)):
		return s.PutMetadataForProviderAndContextID(ctx, p, c.ContextID, *c.New.Metadata)
	}
	return nil
}

// rollback writes back the Old record of every change, under a context that
// outlives the caller's, and returns the cause joined with any write-back
// that failed.
func (s perItemBatchStore) rollback(ctx context.Context, p peer.ID, changes []store.ContextChange, cause error) error {
	cctx, cancel := cleanupContext(ctx)
	defer cancel()
	var errs []error
	for _, c := range changes {
		var prevMeta metadata.Metadata
		if c.Old.Metadata != nil {
			prevMeta = *c.Old.Metadata
		}
		undo := restoreMappings(s.PublisherStore, p, c.ContextID, c.Old.ChunkLink, c.Old.Metadata != nil, prevMeta)
		if err := undo(cctx); err != nil {
			errs = append(errs, fmt.Errorf("restoring context %x: %w", c.ContextID, err))
		}
	}
	if len(errs) == 0 {
		return cause
	}
	return errors.Join(cause, fmt.Errorf("rolling back %d of %d mappings failed: %w", len(errs), len(changes), errors.Join(errs...)))
}
