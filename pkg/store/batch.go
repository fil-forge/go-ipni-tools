package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Block is one content-addressed block for [BatchStore.PutMany].
type Block struct {
	Key  string
	Data []byte
}

// BatchStore is a Store that can write many blocks in one call.
type BatchStore interface {
	Store
	// PutMany writes every block, or returns an error. Blocks written
	// before a failure may stay; they are content addressed.
	PutMany(ctx context.Context, blocks []Block) error
}

// ContextEntry is one mapping for [BatchProviderContextTable.PutMany].
type ContextEntry struct {
	ContextID []byte
	Data      []byte
}

// BatchProviderContextTable is a ProviderContextTable that can read and
// write many context IDs in one call.
type BatchProviderContextTable interface {
	ProviderContextTable
	// GetMany returns the data of every given context ID that has any,
	// keyed by the context ID as a string. A missing context ID is absent
	// from the map.
	GetMany(ctx context.Context, p peer.ID, contextIDs [][]byte) (map[string][]byte, error)
	PutMany(ctx context.Context, p peer.ID, entries []ContextEntry) error
	DeleteMany(ctx context.Context, p peer.ID, contextIDs [][]byte) error
}

// Batch is a PublisherStore that holds every write in memory until the head
// is replaced. ReplaceHead then writes the blocks with one PutMany, the
// mappings with one PutMany (and one DeleteMany) per table, and only then
// moves the head. Reads come from what [Batch.Prefetch] loaded, from the
// writes held, and otherwise from the store.
//
// When moving the head fails after the mappings were written, the batch
// writes the mappings it replaced back, so publishing the same content
// again is not reported as already advertised. Blocks are content addressed
// and stay.
type Batch struct {
	*AdStore
	blocks     *batchBlocks
	chunkLinks *batchTable
	metadata   *batchTable
}

// NewBatch returns a Batch over the store, and false when its block store or
// either of its tables cannot work in batches.
func (s *AdStore) NewBatch() (*Batch, bool) {
	blocks, ok := s.store.(BatchStore)
	if !ok {
		return nil, false
	}
	chunkLinks, ok := s.chunkLinks.(BatchProviderContextTable)
	if !ok {
		return nil, false
	}
	metadataTable, ok := s.metadata.(BatchProviderContextTable)
	if !ok {
		return nil, false
	}
	b := &Batch{
		chunkLinks: newBatchTable(chunkLinks),
		metadata:   newBatchTable(metadataTable),
	}
	b.blocks = &batchBlocks{
		under:   blocks,
		pending: map[string][]byte{},
		tables:  []*batchTable{b.chunkLinks, b.metadata},
	}
	b.AdStore = &AdStore{
		store:           b.blocks,
		chunkLinks:      b.chunkLinks,
		metadata:        b.metadata,
		metadataContext: s.metadataContext,
		identityEntries: s.identityEntries,
	}
	return b, true
}

// Batcher is implemented by a store that can publish in batches.
type Batcher interface {
	NewBatch() (*Batch, bool)
}

var _ Batcher = (*AdStore)(nil)

// Prefetch loads both mappings of every given context ID with one GetMany
// per table, so generating their advertisements reads nothing more.
func (b *Batch) Prefetch(ctx context.Context, p peer.ID, contextIDs [][]byte) error {
	if err := b.chunkLinks.prefetch(ctx, p, contextIDs); err != nil {
		return fmt.Errorf("prefetching entries mappings: %w", err)
	}
	if err := b.metadata.prefetch(ctx, p, contextIDs); err != nil {
		return fmt.Errorf("prefetching metadata mappings: %w", err)
	}
	return nil
}

// batchBlocks holds block writes until the head is replaced.
type batchBlocks struct {
	under   BatchStore
	pending map[string][]byte
	order   []string
	tables  []*batchTable
}

func (s *batchBlocks) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if data, ok := s.pending[key]; ok {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	return s.under.Get(ctx, key)
}

func (s *batchBlocks) Put(ctx context.Context, key string, size uint64, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if _, ok := s.pending[key]; !ok {
		s.order = append(s.order, key)
	}
	s.pending[key] = data
	return nil
}

// Replace writes the blocks and mappings held, then replaces key.
func (s *batchBlocks) Replace(ctx context.Context, key string, old io.Reader, size uint64, new io.Reader) error {
	if err := s.flushBlocks(ctx); err != nil {
		return err
	}
	err := s.flushTables(ctx)
	if err == nil {
		err = s.under.Replace(ctx, key, old, size, new)
		if err == nil {
			return nil
		}
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
	defer cancel()
	var errs []error
	for _, t := range s.tables {
		if rerr := t.restore(cctx); rerr != nil {
			errs = append(errs, rerr)
		}
	}
	if len(errs) > 0 {
		return errors.Join(err, fmt.Errorf("restoring mappings: %w", errors.Join(errs...)))
	}
	return err
}

func (s *batchBlocks) flushBlocks(ctx context.Context) error {
	if len(s.order) == 0 {
		return nil
	}
	blocks := make([]Block, len(s.order))
	for i, key := range s.order {
		blocks[i] = Block{Key: key, Data: s.pending[key]}
	}
	if err := s.under.PutMany(ctx, blocks); err != nil {
		return fmt.Errorf("writing %d blocks: %w", len(blocks), err)
	}
	s.pending = map[string][]byte{}
	s.order = nil
	return nil
}

func (s *batchBlocks) flushTables(ctx context.Context) error {
	for _, t := range s.tables {
		if err := t.flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

// rollbackTimeout bounds how long restoring mappings after a failed head
// move may take, since it runs even when the caller's context has ended.
const rollbackTimeout = 30 * time.Second

// tableKey identifies a mapping in a batchTable.
type tableKey struct {
	provider  peer.ID
	contextID string
}

// tableValue is a mapping's data, or its absence.
type tableValue struct {
	data    []byte
	present bool
}

// batchTable holds mapping writes until the head is replaced, and remembers
// what the store held before, to write it back if the head does not move.
type batchTable struct {
	under   BatchProviderContextTable
	orig    map[tableKey]tableValue
	pending map[tableKey]tableValue
	order   []tableKey
	flushed []tableKey
}

func newBatchTable(under BatchProviderContextTable) *batchTable {
	return &batchTable{
		under:   under,
		orig:    map[tableKey]tableValue{},
		pending: map[tableKey]tableValue{},
	}
}

var _ ProviderContextTable = (*batchTable)(nil)

func (t *batchTable) prefetch(ctx context.Context, p peer.ID, contextIDs [][]byte) error {
	var missing [][]byte
	for _, id := range contextIDs {
		if _, ok := t.orig[tableKey{p, string(id)}]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	found, err := t.under.GetMany(ctx, p, missing)
	if err != nil {
		return err
	}
	for _, id := range missing {
		data, ok := found[string(id)]
		t.orig[tableKey{p, string(id)}] = tableValue{data: data, present: ok}
	}
	return nil
}

func (t *batchTable) Get(ctx context.Context, p peer.ID, contextID []byte) ([]byte, error) {
	v, err := t.current(ctx, tableKey{p, string(contextID)})
	if err != nil {
		return nil, err
	}
	if !v.present {
		return nil, NewErrNotFound(datastore.ErrNotFound)
	}
	return v.data, nil
}

func (t *batchTable) Put(ctx context.Context, p peer.ID, contextID []byte, data []byte) error {
	return t.write(ctx, tableKey{p, string(contextID)}, tableValue{data: data, present: true})
}

func (t *batchTable) Delete(ctx context.Context, p peer.ID, contextID []byte) error {
	return t.write(ctx, tableKey{p, string(contextID)}, tableValue{})
}

// current is the mapping as the batch sees it: its own write, or what the
// store held.
func (t *batchTable) current(ctx context.Context, k tableKey) (tableValue, error) {
	if v, ok := t.pending[k]; ok {
		return v, nil
	}
	return t.original(ctx, k)
}

func (t *batchTable) original(ctx context.Context, k tableKey) (tableValue, error) {
	if v, ok := t.orig[k]; ok {
		return v, nil
	}
	data, err := t.under.Get(ctx, k.provider, []byte(k.contextID))
	switch {
	case err == nil:
		t.orig[k] = tableValue{data: data, present: true}
	case IsNotFound(err):
		t.orig[k] = tableValue{}
	default:
		return tableValue{}, err
	}
	return t.orig[k], nil
}

func (t *batchTable) write(ctx context.Context, k tableKey, v tableValue) error {
	// What the store held is needed to restore it.
	if _, err := t.original(ctx, k); err != nil {
		return err
	}
	if _, ok := t.pending[k]; !ok {
		t.order = append(t.order, k)
	}
	t.pending[k] = v
	return nil
}

// flush writes every mapping the batch changed.
func (t *batchTable) flush(ctx context.Context) error {
	var changed []tableKey
	for _, k := range t.order {
		if !t.pending[k].equal(t.orig[k]) {
			changed = append(changed, k)
		}
	}
	t.flushed = changed
	err := t.writeAll(ctx, changed, t.pending)
	t.pending = map[tableKey]tableValue{}
	t.order = nil
	return err
}

// restore writes back what the store held for every mapping the batch
// flushed.
func (t *batchTable) restore(ctx context.Context) error {
	flushed := t.flushed
	t.flushed = nil
	return t.writeAll(ctx, flushed, t.orig)
}

// writeAll writes the values of the given keys with one PutMany and one
// DeleteMany per provider.
func (t *batchTable) writeAll(ctx context.Context, keys []tableKey, values map[tableKey]tableValue) error {
	puts := map[peer.ID][]ContextEntry{}
	deletes := map[peer.ID][][]byte{}
	var providers []peer.ID
	seen := map[peer.ID]bool{}
	for _, k := range keys {
		if !seen[k.provider] {
			seen[k.provider] = true
			providers = append(providers, k.provider)
		}
		v := values[k]
		if v.present {
			puts[k.provider] = append(puts[k.provider], ContextEntry{ContextID: []byte(k.contextID), Data: v.data})
		} else {
			deletes[k.provider] = append(deletes[k.provider], []byte(k.contextID))
		}
	}
	for _, p := range providers {
		if len(puts[p]) > 0 {
			if err := t.under.PutMany(ctx, p, puts[p]); err != nil {
				return fmt.Errorf("writing %d mappings: %w", len(puts[p]), err)
			}
		}
		if len(deletes[p]) > 0 {
			if err := t.under.DeleteMany(ctx, p, deletes[p]); err != nil {
				return fmt.Errorf("deleting %d mappings: %w", len(deletes[p]), err)
			}
		}
	}
	return nil
}

func (v tableValue) equal(o tableValue) bool {
	return v.present == o.present && bytes.Equal(v.data, o.data)
}
