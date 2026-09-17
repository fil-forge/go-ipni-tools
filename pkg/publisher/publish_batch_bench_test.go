package publisher_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"slices"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/go-ipni-tools/pkg/metadata"
	"github.com/fil-forge/go-ipni-tools/pkg/publisher"
	"github.com/fil-forge/go-ipni-tools/pkg/store"
)

// BenchmarkPublishBatch compares publishing N advertisements one commit at a
// time, one advert per accepted blob as a storage node does through Publish,
// against PublishBatch committing them once.
//
// The per-advert work (entries, chunk link and metadata writes, plus the
// advertisement signature and write) is the same either way: each blob needs
// its own advertisement, because the context ID is derived per blob and the
// metadata names that blob's claim. What batching removes is the per-commit
// work: reading the head, signing a new head, replacing it, and announcing.
func BenchmarkPublishBatch(b *testing.B) {
	for _, n := range []int{100, 400} {
		b.Run(fmt.Sprintf("adverts=%d/Publish", n), func(b *testing.B) {
			benchAdverts(b, n, false)
		})
		b.Run(fmt.Sprintf("adverts=%d/PublishBatch", n), func(b *testing.B) {
			benchAdverts(b, n, true)
		})
	}
}

func benchAdverts(b *testing.B, n int, batched bool) {
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		b.Fatal(err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		b.Fatal(err)
	}
	prov := peer.AddrInfo{ID: pid}
	ctx := context.Background()

	// One multihash per advert under its own context ID, as a location
	// commitment carries.
	specs := make([]publisher.AdvertSpec, n)
	for i := range specs {
		buf := make([]byte, 32)
		if _, err := rand.Read(buf); err != nil {
			b.Fatal(err)
		}
		digest, err := multihash.Sum(buf, multihash.SHA2_256, -1)
		if err != nil {
			b.Fatal(err)
		}
		specs[i] = publisher.AdvertSpec{
			ContextID: fmt.Sprintf("ctx-%d", i),
			Digests:   slices.Values([]multihash.Multihash{digest}),
			Metadata: metadata.MetadataContext.New(&metadata.LocationCommitmentMetadata{
				Claim: cid.NewCidV1(cid.Raw, digest),
			}),
		}
	}

	for b.Loop() {
		b.StopTimer()
		st := store.FromDatastore(dssync.MutexWrap(datastore.NewMapDatastore()),
			store.WithMetadataContext(metadata.MetadataContext))
		p, err := publisher.New(priv, st)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		if batched {
			if _, err := p.PublishBatch(ctx, prov, specs); err != nil {
				b.Fatal(err)
			}
			continue
		}
		for _, spec := range specs {
			if _, err := p.Publish(ctx, prov, spec.ContextID, spec.Digests, spec.Metadata); err != nil {
				b.Fatal(err)
			}
		}
	}
}
