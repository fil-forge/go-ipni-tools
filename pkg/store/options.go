package store

import (
	"fmt"

	"github.com/ipni/go-libipni/metadata"
)

// Option is an option configuring a store.
type Option func(cfg *options)

type options struct {
	metadataContext metadata.MetadataContext
	identityEntries int
}

// WithMetadataContext configues the IPNI metadata context, allowing custom
// metadata types to be stored. If not configured, the default context is used.
func WithMetadataContext(context metadata.MetadataContext) Option {
	return func(o *options) {
		o.metadataContext = context
	}
}

// WithIdentityEntries makes the store inline the entries of an advertisement
// with at most maxEntries multihashes into its link: the link is an identity
// CID carrying the encoded chunk, and nothing is written. The store serves the
// chunk by decoding the CID, so an indexer fetches and verifies it as it
// would any other block. Entries with more multihashes, or that span more
// than one chunk, are stored as usual. It panics when maxEntries is less
// than 1.
//
// The identity digest grows by about 36 bytes per sha2-256 multihash. boxo's
// verifcid rejects identity digests over 128 bytes by default, so tools built
// on boxo reject the CID of more than three such multihashes.
func WithIdentityEntries(maxEntries int) Option {
	if maxEntries < 1 {
		panic(fmt.Sprintf("store.WithIdentityEntries: maxEntries must be at least 1, got %d", maxEntries))
	}
	return func(o *options) {
		o.identityEntries = maxEntries
	}
}
