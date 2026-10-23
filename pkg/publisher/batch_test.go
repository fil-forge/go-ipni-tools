package publisher_test

import (
	"context"
	"sync"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipni/go-libipni/dagsync/ipnisync/head"
	"github.com/ipni/go-libipni/ingest/schema"
	ipnimeta "github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/go-ipni-tools/pkg/publisher"
	"github.com/fil-forge/go-ipni-tools/pkg/store"
)

func TestPublishBatchWithBatchStore(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(nil)
	require.NoError(t, err)
	pid, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)
	provInfo := peer.AddrInfo{ID: pid}
	ctx := context.Background()

	newPublisher := func(t *testing.T, bs *memBatchStore) *publisher.IPNIPublisher {
		t.Helper()
		p, err := publisher.New(priv, bs.PublisherStore, publisher.WithBatchStore(bs))
		require.NoError(t, err)
		return p
	}

	t.Run("one call each to load, write adverts and commit", func(t *testing.T) {
		bs := newMemBatchStore()
		_, err := newPublisher(t, bs).PublishBatch(ctx, provInfo, batchSpecs(t, 10))
		require.NoError(t, err)
		require.Equal(t, batchCalls{LoadContexts: 1, PutAdverts: 1, CommitBatch: 1}, bs.calls)
	})

	t.Run("publishes the chain the default store does", func(t *testing.T) {
		specs := batchSpecs(t, 3)
		batched, err := newPublisher(t, newMemBatchStore()).PublishBatch(ctx, provInfo, specs)
		require.NoError(t, err)
		perItem, err := publisher.New(priv, batchStore())
		require.NoError(t, err)
		perItemHead, err := perItem.PublishBatch(ctx, provInfo, specs)
		require.NoError(t, err)
		require.Equal(t, perItemHead, batched)
	})

	t.Run("skips content already advertised", func(t *testing.T) {
		bs := newMemBatchStore()
		p := newPublisher(t, bs)
		specs := batchSpecs(t, 2)
		first, err := p.PublishBatch(ctx, provInfo, specs)
		require.NoError(t, err)
		again, err := p.PublishBatch(ctx, provInfo, specs)
		require.NoError(t, err)
		require.Equal(t, first, again)
	})

	t.Run("a commit after the head moved fails and maps nothing", func(t *testing.T) {
		bs := newMemBatchStore()
		p := newPublisher(t, bs)
		specs := batchSpecs(t, 2)
		// Another publisher moves the head after this batch has read it.
		bs.beforeCommit = func() {
			bs.beforeCommit = nil
			_, err := p.PublishBatch(ctx, provInfo, specs[1:])
			require.NoError(t, err)
		}
		_, err := p.PublishBatch(ctx, provInfo, specs[:1])
		require.ErrorIs(t, err, store.ErrPreconditionFailed)
		requireUnmapped(t, ctx, bs, pid, specs[0].ContextID, "the spec whose commit failed")
	})

	t.Run("a failed advert write commits nothing", func(t *testing.T) {
		bs := newMemBatchStore()
		bs.failPutAdverts = true
		_, err := newPublisher(t, bs).PublishBatch(ctx, provInfo, batchSpecs(t, 2))
		require.ErrorIs(t, err, errInjected)
		require.Zero(t, bs.calls.CommitBatch)
	})
}

// batchCalls counts the calls a batch makes to a memBatchStore.
type batchCalls struct {
	LoadContexts, PutAdverts, CommitBatch int
}

// memBatchStore is a batch store whose commit is atomic: it moves the head
// first and writes the records only once that succeeded. Records live in
// memory; adverts and the head live in the publisher store it embeds, whose
// own mappings it serves from the records.
type memBatchStore struct {
	store.PublisherStore
	mu             sync.Mutex
	records        map[string]store.ContextRecord
	calls          batchCalls
	failPutAdverts bool
	beforeCommit   func()
}

var _ store.BatchStore = (*memBatchStore)(nil)

func newMemBatchStore() *memBatchStore {
	return &memBatchStore{PublisherStore: batchStore(), records: map[string]store.ContextRecord{}}
}

func (m *memBatchStore) LoadContexts(_ context.Context, p peer.ID, contextIDs [][]byte) (map[string]store.ContextRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls.LoadContexts++
	found := map[string]store.ContextRecord{}
	for _, id := range contextIDs {
		if r, ok := m.records[string(p)+"/"+string(id)]; ok {
			found[string(id)] = r
		}
	}
	return found, nil
}

func (m *memBatchStore) PutAdverts(ctx context.Context, advs []schema.Advertisement) error {
	m.mu.Lock()
	m.calls.PutAdverts++
	fail := m.failPutAdverts
	m.mu.Unlock()
	if fail {
		return errInjected
	}
	for _, adv := range advs {
		if _, err := m.PutAdvert(ctx, adv); err != nil {
			return err
		}
	}
	return nil
}

func (m *memBatchStore) CommitBatch(ctx context.Context, p peer.ID, changes []store.ContextChange, oldHead, newHead *head.SignedHead) error {
	m.mu.Lock()
	m.calls.CommitBatch++
	beforeCommit := m.beforeCommit
	m.mu.Unlock()
	if beforeCommit != nil {
		beforeCommit()
	}
	if _, err := m.ReplaceHead(ctx, oldHead, newHead); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range changes {
		m.records[string(p)+"/"+string(c.ContextID)] = c.New
	}
	return nil
}

func (m *memBatchStore) ChunkLinkForProviderAndContextID(_ context.Context, p peer.ID, contextID []byte) (ipld.Link, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.records[string(p)+"/"+string(contextID)]
	if r.ChunkLink == nil {
		return nil, store.NewErrNotFound(datastore.ErrNotFound)
	}
	return r.ChunkLink, nil
}

func (m *memBatchStore) MetadataForProviderAndContextID(_ context.Context, p peer.ID, contextID []byte) (ipnimeta.Metadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.records[string(p)+"/"+string(contextID)]
	if r.Metadata == nil {
		return ipnimeta.Metadata{}, store.NewErrNotFound(datastore.ErrNotFound)
	}
	return *r.Metadata, nil
}
