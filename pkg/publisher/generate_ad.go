package publisher

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"iter"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/go-ipni-tools/pkg/store"
)

// GenerateAd generates an advertisement for the given parameters. Generation
// writes the store's mappings for the provider and context ID before it
// returns; should it fail partway, those writes are undone, so the store is
// as it was and the call can be retried. Any failure to undo is reported
// beside the cause.
func GenerateAd(ctx context.Context, publisherStore store.PublisherStore, peer peer.ID, addrs []multiaddr.Multiaddr, contextID []byte, md metadata.Metadata, isRm bool, mhs iter.Seq[mh.Multihash]) (schema.Advertisement, error) {
	adv, undo, err := generateAd(ctx, publisherStore, peer, addrs, contextID, md, isRm, mhs)
	if err == nil || undo == nil || errors.Is(err, ErrAlreadyAdvertised) || errors.Is(err, ErrContextIDNotFound) {
		// Nothing was written on those two: they report the store as found.
		return adv, err
	}
	cctx, cancel := cleanupContext(ctx)
	defer cancel()
	if uerr := undo(cctx); uerr != nil {
		return schema.Advertisement{}, errors.Join(err, fmt.Errorf("rolling back the advertisement being generated: %w", uerr))
	}
	return schema.Advertisement{}, err
}

// generateAd generates an advertisement and returns, beside it, the undo of
// the store writes generating it made: the mappings from provider and context
// ID to entries and to metadata go back to what they were when the call
// began. The undo is built from the reads generation makes anyway, and is
// returned whether or not generation succeeded, since the entries mapping is
// written before the metadata one and either write can fail; it is nil only
// when the prior state could not be read. Entries blocks are content
// addressed and are left where they are.
func generateAd(ctx context.Context, publisherStore store.PublisherStore, peer peer.ID, addrs []multiaddr.Multiaddr, contextID []byte, md metadata.Metadata, isRm bool, mhs iter.Seq[mh.Multihash]) (schema.Advertisement, func(context.Context) error, error) {
	var err error

	log := log.With("providerID", peer).With("contextID", base64.StdEncoding.EncodeToString(contextID))

	chunkLink, err := publisherStore.ChunkLinkForProviderAndContextID(ctx, peer, contextID)
	if err != nil {
		if !store.IsNotFound(err) {
			return schema.Advertisement{}, nil, fmt.Errorf("could not get entries cid by provider + context id: %s", err)
		}
	}

	// What the store mapped before this call, for the undo. The metadata is
	// read whether or not an entries mapping exists: a metadata row can be
	// there on its own, left by a commit that failed under an older release,
	// and the undo must put it back rather than delete it. A removal of
	// unmapped content writes nothing, so it needs no snapshot.
	prevChunk := chunkLink
	var prevMetadata metadata.Metadata
	hadMetadata := false
	if !isRm || prevChunk != nil {
		prevMetadata, err = publisherStore.MetadataForProviderAndContextID(ctx, peer, contextID)
		switch {
		case err == nil:
			hadMetadata = true
		case !store.IsNotFound(err):
			return schema.Advertisement{}, nil, fmt.Errorf("could not get metadata for provider + context id: %s", err)
		}
	}
	undo := restoreMappings(publisherStore, peer, contextID, prevChunk, hadMetadata, prevMetadata)

	// If not removing, then generate the link for the list of CIDs from the
	// contextID using the multihash lister, and store the relationship.
	if !isRm {
		log.Info("Creating advertisement")

		prior := store.ContextRecord{ChunkLink: chunkLink}
		if hadMetadata {
			prior.Metadata = &prevMetadata
		}
		next, err := nextRecord(log, prior, md, func() (ipld.Link, error) {
			return publisherStore.PutEntries(ctx, mhs)
		})
		if err != nil {
			return schema.Advertisement{}, undo, err
		}

		if chunkLink == nil {
			chunkLink = next.ChunkLink
			// Store the relationship between providerID, contextID and CID of the
			// advertised list of Cids.
			err = publisherStore.PutChunkLinkForProviderAndContextID(ctx, peer, contextID, chunkLink)
			if err != nil {
				return schema.Advertisement{}, undo, fmt.Errorf("failed to write provider + context id to entries cid mapping: %s", err)
			}
		}

		if err = publisherStore.PutMetadataForProviderAndContextID(ctx, peer, contextID, md); err != nil {
			return schema.Advertisement{}, undo, fmt.Errorf("failed to write provider + context id to metadata mapping: %s", err)
		}
	} else {
		log.Info("Creating removal advertisement")

		if chunkLink == nil {
			return schema.Advertisement{}, undo, ErrContextIDNotFound
		}

		// If removing by context ID, it means the list of CIDs is not needed
		// anymore, so we can remove the entry from the datastore.
		err = publisherStore.DeleteChunkLinkForProviderAndContextID(ctx, peer, contextID)
		if err != nil {
			return schema.Advertisement{}, undo, fmt.Errorf("failed to delete provider + context id to entries cid mapping: %s", err)
		}
		err = publisherStore.DeleteMetadataForProviderAndContextID(ctx, peer, contextID)
		if err != nil {
			return schema.Advertisement{}, undo, fmt.Errorf("failed to delete provider + context id to metadata mapping: %s", err)
		}

		// Create an advertisement to delete content by contextID by specifying
		// that advertisement has no entries.
		chunkLink = schema.NoEntries

		// The advertisement still requires a valid metadata even though
		// metadata is not used for removal. Create a valid empty metadata.
		md = metadata.Default.New()
	}

	mdBytes, err := md.MarshalBinary()
	if err != nil {
		return schema.Advertisement{}, undo, err
	}

	var stringAddrs []string
	for _, addr := range addrs {
		stringAddrs = append(stringAddrs, addr.String())
	}

	return schema.Advertisement{
		Provider:  peer.String(),
		Addresses: stringAddrs,
		Entries:   chunkLink,
		ContextID: contextID,
		Metadata:  mdBytes,
		IsRm:      isRm,
	}, undo, nil
}

// nextRecord returns the record a context ID holds once md is advertised
// under it, given the record it holds now, or [ErrAlreadyAdvertised] when its
// content is mapped and carries md already. For a context ID with no entries
// mapping it calls putEntries, which writes the entries and returns their
// link. An existing entries mapping is kept: the content is advertised again
// under the new metadata.
func nextRecord(log *zap.SugaredLogger, prior store.ContextRecord, md metadata.Metadata, putEntries func() (ipld.Link, error)) (store.ContextRecord, error) {
	if prior.ChunkLink != nil {
		if prior.Metadata == nil {
			log.Warn("No metadata for existing provider + context ID, generating new advertisement")
		} else if md.Equal(*prior.Metadata) {
			return store.ContextRecord{}, ErrAlreadyAdvertised
		}
		return store.ContextRecord{ChunkLink: prior.ChunkLink, Metadata: &md}, nil
	}

	log.Info("Generating entries linked list for advertisement")
	chunkLink, err := putEntries()
	if err != nil {
		return store.ContextRecord{}, fmt.Errorf("could not generate entries list: %s", err)
	}
	if chunkLink == nil {
		log.Warn("chunking for context ID resulted in no link")
		chunkLink = schema.NoEntries
	}
	return store.ContextRecord{ChunkLink: chunkLink, Metadata: &md}, nil
}
