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
// write GenerateAd makes for an advertisement.
type failingMetadataStore struct {
	store.PublisherStore
	calls  int
	failOn int
}

func (f *failingMetadataStore) PutMetadataForProviderAndContextID(ctx context.Context, p peer.ID, contextID []byte, md ipnimeta.Metadata) error {
	f.calls++
	if f.calls == f.failOn {
		return fmt.Errorf("metadata write %d: %w", f.calls, errInjected)
	}
	return f.PublisherStore.PutMetadataForProviderAndContextID(ctx, p, contextID, md)
}
