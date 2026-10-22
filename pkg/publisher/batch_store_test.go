package publisher_test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/go-ipni-tools/pkg/metadata"
	"github.com/fil-forge/go-ipni-tools/pkg/publisher"
	"github.com/fil-forge/go-ipni-tools/pkg/store"
)

func TestPublishBatchOnBatchStore(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(nil)
	require.NoError(t, err)
	pid, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)
	provInfo := peer.AddrInfo{ID: pid}
	ctx := context.Background()

	publishTen := func(t *testing.T) *batchingStore {
		t.Helper()
		bs := newBatchingStore()
		p, err := publisher.New(priv, bs.publisherStore())
		require.NoError(t, err)
		_, err = p.PublishBatch(ctx, provInfo, batchSpecs(t, 10))
		require.NoError(t, err)
		return bs
	}

	t.Run("reads each table with one call", func(t *testing.T) {
		bs := publishTen(t)
		require.Equal(t, calls{GetMany: 1, PutMany: 1}, bs.chunkLinks.calls, "entries mappings")
		require.Equal(t, calls{GetMany: 1, PutMany: 1}, bs.metadata.calls, "metadata mappings")
	})

	t.Run("writes the adverts with one call", func(t *testing.T) {
		bs := publishTen(t)
		require.Equal(t, calls{Get: 1, PutMany: 1, Replace: 1}, bs.blocks.calls, "the Get reads the head")
	})

	t.Run("publishes the chain the per-item path does", func(t *testing.T) {
		specs := batchSpecs(t, 3)
		batched, err := publisher.New(priv, newBatchingStore().publisherStore())
		require.NoError(t, err)
		batchedHead, err := batched.PublishBatch(ctx, provInfo, specs)
		require.NoError(t, err)
		perItem, err := publisher.New(priv, store.FromDatastore(dssync.MutexWrap(datastore.NewMapDatastore()),
			store.WithMetadataContext(metadata.MetadataContext), store.WithIdentityEntries()))
		require.NoError(t, err)
		perItemHead, err := perItem.PublishBatch(ctx, provInfo, specs)
		require.NoError(t, err)
		require.Equal(t, perItemHead, batchedHead)
	})

	t.Run("skips content already advertised", func(t *testing.T) {
		bs := newBatchingStore()
		p, err := publisher.New(priv, bs.publisherStore())
		require.NoError(t, err)
		specs := batchSpecs(t, 2)
		first, err := p.PublishBatch(ctx, provInfo, specs)
		require.NoError(t, err)
		again, err := p.PublishBatch(ctx, provInfo, specs)
		require.NoError(t, err)
		require.Equal(t, first, again)
	})

	t.Run("a failed head move puts the mappings back", func(t *testing.T) {
		bs := newBatchingStore()
		bs.blocks.failReplace = true
		st := bs.publisherStore()
		p, err := publisher.New(priv, st)
		require.NoError(t, err)
		spec := batchSpecs(t, 1)[0]
		_, err = p.PublishBatch(ctx, provInfo, []publisher.AdvertSpec{spec})
		require.ErrorIs(t, err, errInjected)
		requireUnmapped(t, ctx, st, pid, spec.ContextID, "the spec whose head did not move")
	})

	t.Run("a failed head move restores a replaced mapping", func(t *testing.T) {
		bs := newBatchingStore()
		st := bs.publisherStore()
		p, err := publisher.New(priv, st)
		require.NoError(t, err)
		spec := batchSpecs(t, 1)[0]
		_, err = p.PublishBatch(ctx, provInfo, []publisher.AdvertSpec{spec})
		require.NoError(t, err)

		readvertised := spec
		readvertised.Metadata = batchSpecs(t, 1)[0].Metadata
		bs.blocks.failReplace = true
		_, err = p.PublishBatch(ctx, provInfo, []publisher.AdvertSpec{readvertised})
		require.ErrorIs(t, err, errInjected)
		got, err := st.MetadataForProviderAndContextID(ctx, pid, []byte(spec.ContextID))
		require.NoError(t, err)
		require.True(t, spec.Metadata.Equal(got), "the metadata advertised before is back")
	})

	t.Run("a failed block write leaves the mappings unwritten", func(t *testing.T) {
		bs := newBatchingStore()
		bs.blocks.failPutMany = true
		p, err := publisher.New(priv, bs.publisherStore())
		require.NoError(t, err)
		_, err = p.PublishBatch(ctx, provInfo, batchSpecs(t, 2))
		require.ErrorIs(t, err, errInjected)
		require.Zero(t, bs.chunkLinks.calls.PutMany)
	})
}

// calls counts the store calls a batch makes.
type calls struct {
	Get, Put, Delete, Replace    int
	GetMany, PutMany, DeleteMany int
}

// batchingStore is a publisher store whose blocks and tables work in batches.
type batchingStore struct {
	blocks     *memBlocks
	chunkLinks *memTable
	metadata   *memTable
}

func newBatchingStore() *batchingStore {
	return &batchingStore{
		blocks:     &memBlocks{data: map[string][]byte{}},
		chunkLinks: &memTable{data: map[string][]byte{}},
		metadata:   &memTable{data: map[string][]byte{}},
	}
}

func (b *batchingStore) publisherStore() *store.AdStore {
	return store.NewPublisherStore(b.blocks, b.chunkLinks, b.metadata,
		store.WithMetadataContext(metadata.MetadataContext), store.WithIdentityEntries())
}

type memBlocks struct {
	mu          sync.Mutex
	data        map[string][]byte
	calls       calls
	failReplace bool
	failPutMany bool
}

var _ store.BatchStore = (*memBlocks)(nil)

func (m *memBlocks) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls.Get++
	data, ok := m.data[key]
	if !ok {
		return nil, store.NewErrNotFound(datastore.ErrNotFound)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memBlocks) Put(_ context.Context, key string, _ uint64, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls.Put++
	m.data[key] = data
	return nil
}

func (m *memBlocks) PutMany(_ context.Context, blocks []store.Block) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls.PutMany++
	if m.failPutMany {
		return errInjected
	}
	for _, b := range blocks {
		m.data[b.Key] = b.Data
	}
	return nil
}

func (m *memBlocks) Replace(_ context.Context, key string, old io.Reader, _ uint64, new io.Reader) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls.Replace++
	if m.failReplace {
		return errInjected
	}
	cur, exists := m.data[key]
	if old == nil {
		if exists {
			return store.ErrPreconditionFailed
		}
	} else {
		want, err := io.ReadAll(old)
		if err != nil {
			return err
		}
		if !exists || !bytes.Equal(cur, want) {
			return store.ErrPreconditionFailed
		}
	}
	data, err := io.ReadAll(new)
	if err != nil {
		return err
	}
	m.data[key] = data
	return nil
}

type memTable struct {
	mu    sync.Mutex
	data  map[string][]byte
	calls calls
}

var _ store.BatchProviderContextTable = (*memTable)(nil)

func tableKey(p peer.ID, contextID []byte) string {
	return string(p) + "/" + string(contextID)
}

func (m *memTable) Get(_ context.Context, p peer.ID, contextID []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls.Get++
	data, ok := m.data[tableKey(p, contextID)]
	if !ok {
		return nil, store.NewErrNotFound(datastore.ErrNotFound)
	}
	return data, nil
}

func (m *memTable) Put(_ context.Context, p peer.ID, contextID []byte, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls.Put++
	m.data[tableKey(p, contextID)] = data
	return nil
}

func (m *memTable) Delete(_ context.Context, p peer.ID, contextID []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls.Delete++
	delete(m.data, tableKey(p, contextID))
	return nil
}

func (m *memTable) GetMany(_ context.Context, p peer.ID, contextIDs [][]byte) (map[string][]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls.GetMany++
	found := map[string][]byte{}
	for _, id := range contextIDs {
		if data, ok := m.data[tableKey(p, id)]; ok {
			found[string(id)] = data
		}
	}
	return found, nil
}

func (m *memTable) PutMany(_ context.Context, p peer.ID, entries []store.ContextEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls.PutMany++
	for _, e := range entries {
		m.data[tableKey(p, e.ContextID)] = e.Data
	}
	return nil
}

func (m *memTable) DeleteMany(_ context.Context, p peer.ID, contextIDs [][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls.DeleteMany++
	for _, id := range contextIDs {
		delete(m.data, tableKey(p, id))
	}
	return nil
}
