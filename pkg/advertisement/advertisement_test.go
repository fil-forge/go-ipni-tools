package advertisement_test

import (
	"net/url"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/maurl"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/fil-forge/libforge/capabilities"
	"github.com/fil-forge/libforge/capabilities/assert"
	"github.com/fil-forge/libforge/digestutil"
	"github.com/fil-forge/libforge/testutil"

	intrnl_testutil "github.com/fil-forge/go-ipni-tools/internal/testutil"
	"github.com/fil-forge/go-ipni-tools/pkg/advertisement"
)

func TestShardCID(t *testing.T) {
	curioPath := testutil.Must(multiaddr.NewMultiaddr("/http-path/" + url.PathEscape("piece/{blobCID}")))(t)
	blobPath := testutil.Must(multiaddr.NewMultiaddr("/http-path/" + url.PathEscape("blob/{blob}/{blob}")))(t)
	mixedPath := testutil.Must(multiaddr.NewMultiaddr("/http-path/" + url.PathEscape("blob/{blobCID}/{blob}")))(t)
	baseUrl := testutil.Must(url.Parse("https://node.com"))(t)
	base := testutil.Must(maurl.FromURL(baseUrl))(t)
	testCid := testutil.RandomCID(t)
	testMhs := intrnl_testutil.RandomMultihashes(t, 3)

	testPieceLink := testutil.RandomCID(t)

	tests := []struct {
		name      string
		provider  peer.AddrInfo
		caveats   assert.LocationArguments
		expected  *cid.Cid
		expectErr bool
	}{
		{
			name: "Valid shard CID from location url",
			provider: peer.AddrInfo{
				ID: intrnl_testutil.RandomPeer(t),
				Addrs: []multiaddr.Multiaddr{
					multiaddr.Join(base, curioPath),
				},
			},
			caveats: assert.LocationArguments{
				Content: testMhs[0],
				Location: []capabilities.CborURL{
					capabilities.CborURL(*baseUrl.JoinPath("piece", testPieceLink.String())),
				},
			},
			expected: func() *cid.Cid {
				c := testPieceLink
				return &c
			}(),
			expectErr: false,
		},
		{
			name: "Valid shard multihash from location url",
			provider: peer.AddrInfo{
				ID: intrnl_testutil.RandomPeer(t),
				Addrs: []multiaddr.Multiaddr{
					multiaddr.Join(base, blobPath),
				},
			},
			caveats: assert.LocationArguments{
				Content: testMhs[0],
				Location: []capabilities.CborURL{
					capabilities.CborURL(*baseUrl.JoinPath("blob", digestutil.Format(testMhs[1]), digestutil.Format(testMhs[1]))),
				},
			},
			expected: func() *cid.Cid {
				cid := cid.NewCidV1(cid.Raw, testMhs[1])
				return &cid
			}(),
			expectErr: false,
		},
		{
			name: "Valid CID in mixed location url",
			provider: peer.AddrInfo{
				ID: intrnl_testutil.RandomPeer(t),
				Addrs: []multiaddr.Multiaddr{
					multiaddr.Join(base, mixedPath),
				},
			},
			caveats: assert.LocationArguments{
				Content: testMhs[0],
				Location: []capabilities.CborURL{
					capabilities.CborURL(*baseUrl.JoinPath("blob", testCid.String(), digestutil.Format(testCid.Hash()))),
				},
			},
			expected: func() *cid.Cid {
				c := testCid
				return &c
			}(),
			expectErr: false,
		},
		// TODO: test for error on blob & cid not matching
		{
			name: "No matching location",
			provider: peer.AddrInfo{
				Addrs: []multiaddr.Multiaddr{
					multiaddr.Join(base, curioPath),
				},
			},
			caveats: assert.LocationArguments{
				Location: []capabilities.CborURL{
					capabilities.CborURL(*baseUrl.JoinPath("blob", digestutil.Format(testMhs[1]), digestutil.Format(testMhs[1]))),
				},
				Content: testMhs[0],
			},
			expected:  nil,
			expectErr: false,
		},
		{
			name: "Invalid CID in location",
			provider: peer.AddrInfo{
				Addrs: []multiaddr.Multiaddr{
					multiaddr.Join(base, curioPath),
				},
			},
			caveats: assert.LocationArguments{
				Location: []capabilities.CborURL{
					capabilities.CborURL(*baseUrl.JoinPath("piece", "applesauce")),
				},
				Content: testMhs[0],
			},
			expected:  nil,
			expectErr: true,
		},
		// TODO: invalid multihash in location test
		// TODO: multiaddrs with blobs test

	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := advertisement.ShardCID(tt.provider, tt.caveats)
			if (err != nil) != tt.expectErr {
				t.Fatalf("expected error: %v, got: %v", tt.expectErr, err)
			}
			if result != nil && tt.expected != nil && !result.Equals(*tt.expected) {
				t.Errorf("expected: %v, got: %v", tt.expected, result)
			}
			if result == nil && tt.expected != nil {
				t.Errorf("expected: %v, got: nil", tt.expected)
			}
		})
	}
}
