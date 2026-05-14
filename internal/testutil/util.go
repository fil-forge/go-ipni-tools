package testutil

import (
	"math/rand"
	"testing"

	"github.com/fil-forge/libforge/testutil"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func RandomMultihashes(t *testing.T, count int) []multihash.Multihash {
	require.Greater(t, count, 0, "count must be greater than 0")
	mhs := make([]multihash.Multihash, 0, count)
	for range count {
		mhs = append(mhs, testutil.RandomMultihash(t))
	}
	return mhs
}

var seedSeq int64

func RandomPeer(t *testing.T) peer.ID {
	src := rand.NewSource(seedSeq)
	seedSeq++
	r := rand.New(src)
	_, publicKey := testutil.Must2(crypto.GenerateEd25519Key(r))(t)
	return testutil.Must(peer.IDFromPublicKey(publicKey))(t)
}
