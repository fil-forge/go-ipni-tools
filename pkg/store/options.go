package store

import (
	"github.com/ipni/go-libipni/metadata"
)

// Option is an option configuring a store.
type Option func(cfg *options)

type options struct {
	metadataContext metadata.MetadataContext
	identityEntries bool
}

// WithMetadataContext configues the IPNI metadata context, allowing custom
// metadata types to be stored. If not configured, the default context is used.
func WithMetadataContext(context metadata.MetadataContext) Option {
	return func(o *options) {
		o.metadataContext = context
	}
}

// WithIdentityEntries makes the store inline an entries chain of a single
// multihash into its link: the link is an identity CID carrying the encoded
// chunk, and nothing is written. The store serves the chunk by decoding the
// CID, so an indexer fetches and verifies it as it would any other block.
// Longer chains are stored as usual.
func WithIdentityEntries() Option {
	return func(o *options) {
		o.identityEntries = true
	}
}
