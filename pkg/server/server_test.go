package server_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"slices"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/libforge/testutil"

	intrnl_testutil "github.com/fil-forge/go-ipni-tools/internal/testutil"
	"github.com/fil-forge/go-ipni-tools/pkg/publisher"
	"github.com/fil-forge/go-ipni-tools/pkg/server"
	"github.com/fil-forge/go-ipni-tools/pkg/store"
)

// An indexer syncs an advertisement whose entries link is an identity CID by
// fetching that CID from the server like any other block.
func TestSyncAdvertWithIdentityEntries(t *testing.T) {
	ctx := context.Background()
	priv, _, err := crypto.GenerateEd25519Key(nil)
	require.NoError(t, err)
	pid, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)

	st := store.FromDatastore(dssync.MutexWrap(datastore.NewMapDatastore()), store.WithIdentityEntries())
	pub, err := publisher.New(priv, st)
	require.NoError(t, err)
	digests := intrnl_testutil.RandomMultihashes(t, 1)
	adLink, err := pub.Publish(ctx, peer.AddrInfo{ID: pid}, testutil.RandomCID(t).String(), slices.Values(digests), metadata.Default.New())
	require.NoError(t, err)
	ad, err := st.Advert(ctx, adLink)
	require.NoError(t, err)

	srv, err := server.NewServer(st)
	require.NoError(t, err)
	httpServer := httptest.NewServer(srv)
	t.Cleanup(httpServer.Close)
	u, err := url.Parse(httpServer.URL)
	require.NoError(t, err)
	addr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/dns4/%s/tcp/%s/http", u.Hostname(), u.Port()))
	require.NoError(t, err)

	lsys := cidlink.DefaultLinkSystem()
	mem := &memstore.Store{}
	lsys.SetReadStorage(mem)
	lsys.SetWriteStorage(mem)
	var synced []cid.Cid
	client := ipnisync.NewSync(lsys, func(_ peer.ID, c cid.Cid) { synced = append(synced, c) })
	syncer, err := client.NewSyncer(peer.AddrInfo{ID: pid, Addrs: []multiaddr.Multiaddr{addr}})
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx, asCid(adLink), selectorparse.CommonSelector_ExploreAllRecursively))

	require.Equal(t, []cid.Cid{asCid(adLink), asCid(ad.Entries)}, synced)
}

func asCid(lnk ipld.Link) cid.Cid {
	return lnk.(cidlink.Link).Cid
}
