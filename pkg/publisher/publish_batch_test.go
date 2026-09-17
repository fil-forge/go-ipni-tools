package publisher_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipni/go-libipni/dagsync/ipnisync/head"
	ipnimeta "github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/libforge/testutil"

	intrnl_testutil "github.com/fil-forge/go-ipni-tools/internal/testutil"
	"github.com/fil-forge/go-ipni-tools/pkg/metadata"
	"github.com/fil-forge/go-ipni-tools/pkg/publisher"
	"github.com/fil-forge/go-ipni-tools/pkg/store"
)

// batchSpecs builds n single-multihash specs, each under its own context ID
// and carrying its own location commitment metadata, which is the shape one
// accepted blob produces.
func batchSpecs(t *testing.T, n int) []publisher.AdvertSpec {
	t.Helper()
	digests := intrnl_testutil.RandomMultihashes(t, n)
	specs := make([]publisher.AdvertSpec, n)
	for i := range specs {
		specs[i] = publisher.AdvertSpec{
			ContextID: testutil.RandomCID(t).String(),
			Digests:   slices.Values(digests[i : i+1]),
			Metadata: metadata.MetadataContext.New(&metadata.LocationCommitmentMetadata{
				Claim: testutil.RandomCID(t),
			}),
		}
	}
	return specs
}

// batchStore is a publisher store that can read location commitment metadata
// back, which GenerateAd does to decide whether content is already advertised.
func batchStore() store.PublisherStore {
	return store.FromDatastore(dssync.MutexWrap(datastore.NewMapDatastore()),
		store.WithMetadataContext(metadata.MetadataContext))
}

// chainFrom walks the advertisement chain back from head and returns every
// advertisement's entries root, newest first.
func chainFrom(t *testing.T, ctx context.Context, st store.PublisherStore, head ipld.Link) []ipld.Link {
	t.Helper()
	var entries []ipld.Link
	for lnk := head; lnk != nil; {
		ad, err := st.Advert(ctx, lnk)
		require.NoError(t, err)
		entries = append(entries, ad.Entries)
		lnk = ad.PreviousID
	}
	return entries
}

func TestPublishBatch(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(nil)
	require.NoError(t, err)
	pid, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)
	provInfo := peer.AddrInfo{ID: pid}
	ctx := context.Background()

	t.Run("one commit and one announce for many adverts", func(t *testing.T) {
		var announces atomic.Int32
		indexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			announces.Add(1)
			w.WriteHeader(http.StatusNoContent)
		}))
		t.Cleanup(indexer.Close)

		st := batchStore()
		p, err := publisher.New(priv, st,
			publisher.WithDirectAnnounce(indexer.URL),
			publisher.WithAnnounceAddrs("/dns/publisher.example/tcp/443/https"))
		require.NoError(t, err)

		const n = 25
		specs := batchSpecs(t, n)
		head, err := p.PublishBatch(ctx, provInfo, specs)
		require.NoError(t, err)
		require.NotNil(t, head)

		// Every spec became an advertisement on one chain, in order.
		chain := chainFrom(t, ctx, st, head)
		require.Len(t, chain, n)
		for i, spec := range specs {
			var got []multihash.Multihash
			for e, err := range st.Entries(ctx, chain[n-1-i]) {
				require.NoError(t, err)
				got = append(got, e)
			}
			require.Equal(t, slices.Collect(spec.Digests), got, "advert %d carries its own spec's content", i)
		}

		// The head moved once and the indexer heard about it once.
		stored, err := st.Head(ctx)
		require.NoError(t, err)
		require.Equal(t, head, stored.Head)
		require.EqualValues(t, 1, announces.Load(), "a batch announces once, however many adverts it carries")
	})

	t.Run("one mapping read per spec", func(t *testing.T) {
		counting := &countingStore{PublisherStore: batchStore()}
		p, err := publisher.New(priv, counting)
		require.NoError(t, err)

		specs := batchSpecs(t, 7)
		_, err = p.PublishBatch(ctx, provInfo, specs)
		require.NoError(t, err)
		require.Equal(t, len(specs), counting.chunkReads, "the prior state is read once, by generation itself")
	})

	t.Run("already advertised specs are skipped", func(t *testing.T) {
		st := batchStore()
		p, err := publisher.New(priv, st)
		require.NoError(t, err)

		specs := batchSpecs(t, 5)
		first, err := p.PublishBatch(ctx, provInfo, specs)
		require.NoError(t, err)

		// The same content again, plus one new spec: only the new one publishes.
		again, err := p.PublishBatch(ctx, provInfo, append(slices.Clone(specs), batchSpecs(t, 1)...))
		require.NoError(t, err)
		require.NotEqual(t, first, again)
		require.Len(t, chainFrom(t, ctx, st, again), 6)

		// Nothing new: the head stays where it is.
		same, err := p.PublishBatch(ctx, provInfo, specs)
		require.NoError(t, err)
		require.Equal(t, again, same)
	})

	t.Run("a failed batch leaves nothing behind", func(t *testing.T) {
		st := batchStore()
		failing := &failingMetadataStore{PublisherStore: st, failOn: 3}
		p, err := publisher.New(priv, failing)
		require.NoError(t, err)

		specs := batchSpecs(t, 5)
		_, err = p.PublishBatch(ctx, provInfo, specs)
		// GenerateAd wraps store errors with %s, so the chain is not walkable.
		require.ErrorContains(t, err, errInjected.Error())
		stored, err := st.Head(ctx)
		require.True(t, err != nil || stored == nil, "a failed batch must not move the head")
		// No mapping survives, the spec that was being generated included:
		// its entries mapping was written before its metadata write failed.
		for i, spec := range specs {
			requireUnmapped(t, ctx, st, pid, spec.ContextID, fmt.Sprintf("spec %d", i))
		}

		// A fresh publisher over the same store, as after a restart: the
		// adverts generated before the failure lived only in memory, so the
		// store must not still call their content advertised.
		p2, err := publisher.New(priv, st)
		require.NoError(t, err)
		head, err := p2.PublishBatch(ctx, provInfo, specs)
		require.NoError(t, err)
		require.Len(t, chainFrom(t, ctx, st, head), 5, "every spec of the failed batch publishes on retry")
	})
}

var errInjected = errors.New("injected store failure")

// failingMetadataStore fails the nth metadata write, which is the last store
// write GenerateAd makes for an advertisement. onFail, if set, runs at that
// moment; failDeletes makes every mapping delete fail too, which is what a
// rollback needs to do.
type failingMetadataStore struct {
	store.PublisherStore
	calls       int
	failOn      int
	onFail      func()
	failDeletes bool
}

func (f *failingMetadataStore) PutMetadataForProviderAndContextID(ctx context.Context, p peer.ID, contextID []byte, md ipnimeta.Metadata) error {
	f.calls++
	if f.calls == f.failOn {
		if f.onFail != nil {
			f.onFail()
		}
		return fmt.Errorf("metadata write %d: %w", f.calls, errInjected)
	}
	return f.PublisherStore.PutMetadataForProviderAndContextID(ctx, p, contextID, md)
}

var errRollback = errors.New("injected rollback failure")

func (f *failingMetadataStore) DeleteChunkLinkForProviderAndContextID(ctx context.Context, p peer.ID, contextID []byte) error {
	// A real store refuses work on an ended context; the map store does not.
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.failDeletes {
		return errRollback
	}
	return f.PublisherStore.DeleteChunkLinkForProviderAndContextID(ctx, p, contextID)
}

// countingStore counts the entries-mapping reads a batch makes.
type countingStore struct {
	store.PublisherStore
	chunkReads int
}

func (c *countingStore) ChunkLinkForProviderAndContextID(ctx context.Context, p peer.ID, contextID []byte) (ipld.Link, error) {
	c.chunkReads++
	return c.PublisherStore.ChunkLinkForProviderAndContextID(ctx, p, contextID)
}

// requireUnmapped asserts the store maps nothing to the provider and context
// ID: neither entries nor metadata.
func requireUnmapped(t *testing.T, ctx context.Context, st store.PublisherStore, pid peer.ID, contextID, what string) {
	t.Helper()
	_, err := st.ChunkLinkForProviderAndContextID(ctx, pid, []byte(contextID))
	require.True(t, store.IsNotFound(err), "%s: entries mapping left behind (err=%v)", what, err)
	_, err = st.MetadataForProviderAndContextID(ctx, pid, []byte(contextID))
	require.True(t, store.IsNotFound(err), "%s: metadata mapping left behind (err=%v)", what, err)
}

func TestPublishBatchRollback(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(nil)
	require.NoError(t, err)
	pid, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)
	provInfo := peer.AddrInfo{ID: pid}
	ctx := context.Background()

	t.Run("a failed re-advertisement keeps the existing mapping", func(t *testing.T) {
		st := batchStore()
		failing := &failingMetadataStore{PublisherStore: st}
		p, err := publisher.New(priv, failing)
		require.NoError(t, err)

		// Content already advertised under metadata M1.
		specs := batchSpecs(t, 2)
		first, err := p.PublishBatch(ctx, provInfo, specs[:1])
		require.NoError(t, err)
		chunkBefore, err := st.ChunkLinkForProviderAndContextID(ctx, pid, []byte(specs[0].ContextID))
		require.NoError(t, err)
		metaBefore := specs[0].Metadata

		// The same content again under new metadata, beside a spec whose
		// metadata write fails: the first metadata write of this batch
		// re-advertises, the second (the new spec's) fails.
		readvertised := specs[0]
		readvertised.Metadata = metadata.MetadataContext.New(&metadata.LocationCommitmentMetadata{Claim: testutil.RandomCID(t)})
		failing.failOn = failing.calls + 2
		_, err = p.PublishBatch(ctx, provInfo, []publisher.AdvertSpec{readvertised, specs[1]})
		require.ErrorContains(t, err, errInjected.Error())

		// The pre-existing mapping is back as it was: same entries, metadata M1.
		chunkAfter, err := st.ChunkLinkForProviderAndContextID(ctx, pid, []byte(specs[0].ContextID))
		require.NoError(t, err)
		require.Equal(t, chunkBefore, chunkAfter, "a failed re-advertisement must not drop the existing entries mapping")
		metaAfter, err := st.MetadataForProviderAndContextID(ctx, pid, []byte(specs[0].ContextID))
		require.NoError(t, err)
		require.True(t, metaBefore.Equal(metaAfter), "a failed re-advertisement must restore the previous metadata")
		requireUnmapped(t, ctx, st, pid, specs[1].ContextID, "the new spec")
		stored, err := st.Head(ctx)
		require.NoError(t, err)
		require.Equal(t, first, stored.Head, "the head is where the first batch left it")

		// A retry publishes the re-advertisement and the new spec: three
		// adverts on the chain.
		failing.failOn = 0
		head, err := p.PublishBatch(ctx, provInfo, []publisher.AdvertSpec{readvertised, specs[1]})
		require.NoError(t, err)
		require.Len(t, chainFrom(t, ctx, st, head), 3)
	})

	t.Run("a rollback that fails is reported beside the cause", func(t *testing.T) {
		st := batchStore()
		failing := &failingMetadataStore{PublisherStore: st, failOn: 3, failDeletes: true}
		p, err := publisher.New(priv, failing)
		require.NoError(t, err)

		_, err = p.PublishBatch(ctx, provInfo, batchSpecs(t, 4))
		require.ErrorContains(t, err, errInjected.Error(), "the cause is reported")
		require.ErrorIs(t, err, errRollback, "so is every mapping the rollback could not undo")
	})

	t.Run("rollback completes after the caller cancels", func(t *testing.T) {
		st := batchStore()
		callerCtx, cancel := context.WithCancel(ctx)
		// The caller gives up at the moment the store fails, as a request
		// timeout would.
		failing := &failingMetadataStore{PublisherStore: st, failOn: 3, onFail: cancel}
		p, err := publisher.New(priv, failing)
		require.NoError(t, err)

		specs := batchSpecs(t, 4)
		_, err = p.PublishBatch(callerCtx, provInfo, specs)
		require.ErrorContains(t, err, errInjected.Error())
		require.NotErrorIs(t, err, context.Canceled, "the rollback does not run on the cancelled context")
		for i, spec := range specs {
			requireUnmapped(t, ctx, st, pid, spec.ContextID, fmt.Sprintf("spec %d", i))
		}
	})

	t.Run("a failed commit rolls the batch back", func(t *testing.T) {
		st := batchStore()
		failing := &failingHeadStore{PublisherStore: st, failNext: true}
		p, err := publisher.New(priv, failing)
		require.NoError(t, err)

		specs := batchSpecs(t, 4)
		_, err = p.PublishBatch(ctx, provInfo, specs)
		require.ErrorContains(t, err, errInjected.Error())
		for i, spec := range specs {
			requireUnmapped(t, ctx, st, pid, spec.ContextID, fmt.Sprintf("spec %d", i))
		}

		head, err := p.PublishBatch(ctx, provInfo, specs)
		require.NoError(t, err)
		require.Len(t, chainFrom(t, ctx, st, head), 4, "every spec of the failed batch publishes on retry")
	})

	t.Run("a failed commit whose rollback fails reports both", func(t *testing.T) {
		st := batchStore()
		// The commit fails, and so does every mapping delete the rollback then
		// attempts; nothing here fails during generation, so the only report
		// of the rollback comes from the commit's undo of the pending batch.
		failing := &failingHeadStore{
			PublisherStore: &failingMetadataStore{PublisherStore: st, failDeletes: true},
			failNext:       true,
		}
		p, err := publisher.New(priv, failing)
		require.NoError(t, err)

		_, err = p.PublishBatch(ctx, provInfo, batchSpecs(t, 3))
		require.ErrorContains(t, err, errInjected.Error(), "the commit failure is reported")
		require.ErrorIs(t, err, errRollback, "so is every pending advert the rollback could not undo")
	})
}

// failingHeadStore fails the next head replacement, which is the commit's
// last store write before the announce.
type failingHeadStore struct {
	store.PublisherStore
	failNext bool
}

func (f *failingHeadStore) ReplaceHead(ctx context.Context, oldHead, newHead *head.SignedHead) (ipld.Link, error) {
	if f.failNext {
		f.failNext = false
		return nil, fmt.Errorf("replace head: %w", errInjected)
	}
	return f.PublisherStore.ReplaceHead(ctx, oldHead, newHead)
}
