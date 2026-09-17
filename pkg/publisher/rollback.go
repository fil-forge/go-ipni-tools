package publisher

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/fil-forge/go-ipni-tools/pkg/store"
)

// rollbackTimeout bounds how long undoing a failed publish may take. The
// undo runs even when the caller's context has ended, since a mapping left
// behind would make its content unpublishable on retry, so it needs a bound
// of its own.
const rollbackTimeout = 30 * time.Second

// cleanupContext derives the context an undo runs under: the caller's values,
// not its cancellation, and a deadline of its own.
func cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
}

// restoreMappings returns the undo that puts the store's mappings for the
// provider and context ID back to the given prior state: a mapping that did
// not exist is deleted, one that did is written back.
func restoreMappings(s store.PublisherStore, p peer.ID, contextID []byte, prevChunk ipld.Link, hadMeta bool, prevMeta metadata.Metadata) func(context.Context) error {
	return func(ctx context.Context) error {
		var errs []error
		if prevChunk == nil {
			if err := s.DeleteChunkLinkForProviderAndContextID(ctx, p, contextID); err != nil && !store.IsNotFound(err) {
				errs = append(errs, fmt.Errorf("removing entries mapping: %w", err))
			}
		} else if err := s.PutChunkLinkForProviderAndContextID(ctx, p, contextID, prevChunk); err != nil {
			errs = append(errs, fmt.Errorf("restoring entries mapping: %w", err))
		}
		if !hadMeta {
			if err := s.DeleteMetadataForProviderAndContextID(ctx, p, contextID); err != nil && !store.IsNotFound(err) {
				errs = append(errs, fmt.Errorf("removing metadata mapping: %w", err))
			}
		} else if err := s.PutMetadataForProviderAndContextID(ctx, p, contextID, prevMeta); err != nil {
			errs = append(errs, fmt.Errorf("restoring metadata mapping: %w", err))
		}
		return errors.Join(errs...)
	}
}

// forgetChunkLink is the undo for an advertisement queued through AddToBatch,
// whose generation was not observed: it drops the mapping from provider and
// context ID to entries, so generating the advertisement again produces it
// rather than ErrAlreadyAdvertised. A removal's generation deleted the
// mappings, and the advertisement carries nothing to restore them from, so
// its undo reports that rather than pretending the store is as it was.
func forgetChunkLink(s store.PublisherStore, adv schema.Advertisement) func(context.Context) error {
	return func(ctx context.Context) error {
		if adv.IsRm {
			return fmt.Errorf("removal advertisement: the mappings its generation deleted cannot be restored from the advertisement; retrying reports %w", ErrContextIDNotFound)
		}
		p, err := peer.Decode(adv.Provider)
		if err != nil {
			return fmt.Errorf("decoding advertisement provider: %w", err)
		}
		if err := s.DeleteChunkLinkForProviderAndContextID(ctx, p, adv.ContextID); err != nil && !store.IsNotFound(err) {
			return fmt.Errorf("removing entries mapping: %w", err)
		}
		return nil
	}
}
