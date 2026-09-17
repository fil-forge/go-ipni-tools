package publisher

import (
	"context"
	"fmt"
	"iter"

	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/go-ipni-tools/pkg/store"
)

// generateAd generates an advertisement the way GenerateAd does and returns,
// beside it, the undo of the store writes generating it made: the mappings
// from provider and context ID to entries and to metadata go back to what
// they were before the call. The undo is returned whether or not generation
// succeeded, since GenerateAd writes the entries mapping before the metadata
// one and either can fail. Entries blocks are content addressed and are left
// where they are.
func generateAd(ctx context.Context, publisherStore store.PublisherStore, p peer.ID, addrs []multiaddr.Multiaddr, contextID []byte, md metadata.Metadata, mhs iter.Seq[mh.Multihash]) (schema.Advertisement, func(context.Context), error) {
	undo, err := snapshotMappings(ctx, publisherStore, p, contextID)
	if err != nil {
		return schema.Advertisement{}, nil, err
	}
	adv, err := GenerateAd(ctx, publisherStore, p, addrs, contextID, md, false, mhs)
	if err != nil {
		return schema.Advertisement{}, undo, err
	}
	return adv, undo, nil
}

// snapshotMappings records what the store maps the provider and context ID to
// and returns the function that puts it back.
func snapshotMappings(ctx context.Context, s store.PublisherStore, p peer.ID, contextID []byte) (func(context.Context), error) {
	prevChunk, err := s.ChunkLinkForProviderAndContextID(ctx, p, contextID)
	if err != nil && !store.IsNotFound(err) {
		return nil, fmt.Errorf("reading entries mapping for provider + context id: %w", err)
	}
	var prevMeta metadata.Metadata
	hadMeta := false
	if prevChunk != nil {
		prevMeta, err = s.MetadataForProviderAndContextID(ctx, p, contextID)
		switch {
		case err == nil:
			hadMeta = true
		case !store.IsNotFound(err):
			return nil, fmt.Errorf("reading metadata mapping for provider + context id: %w", err)
		}
	}
	return func(ctx context.Context) {
		if prevChunk == nil {
			// Nothing was mapped before this attempt.
			deleteMappings(ctx, s, p, contextID)
			return
		}
		// The entries mapping is the one that was there: entries are content
		// addressed, so regenerating them yields the same link and GenerateAd
		// does not rewrite it. Only the metadata can have moved.
		if hadMeta {
			if err := s.PutMetadataForProviderAndContextID(ctx, p, contextID, prevMeta); err != nil {
				log.Warnw("failed to restore metadata mapping after failed publish", "err", err)
			}
			return
		}
		if err := s.DeleteMetadataForProviderAndContextID(ctx, p, contextID); err != nil {
			log.Warnw("failed to remove metadata mapping after failed publish", "err", err)
		}
	}, nil
}

func deleteMappings(ctx context.Context, s store.PublisherStore, p peer.ID, contextID []byte) {
	if err := s.DeleteChunkLinkForProviderAndContextID(ctx, p, contextID); err != nil {
		log.Warnw("failed to remove entries mapping after failed publish", "err", err)
	}
	if err := s.DeleteMetadataForProviderAndContextID(ctx, p, contextID); err != nil {
		log.Warnw("failed to remove metadata mapping after failed publish", "err", err)
	}
}

// forgetChunkLink is the undo for an advertisement queued through AddToBatch,
// whose generation was not observed: it drops the mapping from provider and
// context ID to entries, so generating the advertisement again produces it
// rather than ErrAlreadyAdvertised. A removal advertisement has nothing to
// undo.
func forgetChunkLink(s store.PublisherStore, adv schema.Advertisement) func(context.Context) {
	return func(ctx context.Context) {
		if adv.IsRm {
			return
		}
		p, err := peer.Decode(adv.Provider)
		if err != nil {
			return
		}
		if err := s.DeleteChunkLinkForProviderAndContextID(ctx, p, adv.ContextID); err != nil {
			log.Warnw("failed to remove entries mapping after failed publish", "err", err)
		}
	}
}
