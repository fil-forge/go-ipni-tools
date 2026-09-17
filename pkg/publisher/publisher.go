package publisher

import (
	"context"
	"errors"
	"fmt"
	"iter"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/go-ipni-tools/pkg/store"
)

var log = logging.Logger("publisher")

type Publisher interface {
	// Publish creates, signs and publishes an advert. It then announces the new
	// advert to other indexers.
	Publish(ctx context.Context, provider peer.AddrInfo, contextID string, digests iter.Seq[mh.Multihash], meta metadata.Metadata) (ipld.Link, error)
}

type AsyncPublisher interface {
	// Publish creates, signs and publishes an advert but does so asynchronously, so no advert CID is returned.
	Publish(ctx context.Context, provider peer.AddrInfo, contextID string, digests iter.Seq[mh.Multihash], meta metadata.Metadata) error
}

type IPNIPublisher struct {
	batchPublisher *AdvertisementPublisher
	store          store.PublisherStore
}

// Publish creates a new advertisement from the latest head, signs it, and publishes it.
// Publish is not safe for concurrent use and advertisements may be lost if called concurrently. A mutex or any other
// synchronization mechanism must be used around Publish if it will be called from concurrent goroutines.
func (p *IPNIPublisher) Publish(ctx context.Context, providerInfo peer.AddrInfo, contextID string, digests iter.Seq[mh.Multihash], meta metadata.Metadata) (ipld.Link, error) {
	link, err := p.publishAdvForIndex(ctx, providerInfo.ID, providerInfo.Addrs, []byte(contextID), meta, false, digests)
	if err != nil {
		return nil, fmt.Errorf("publishing IPNI advert: %w", err)
	}
	return link, nil
}

var _ Publisher = (*IPNIPublisher)(nil)

// AdvertSpec is what one advertisement is generated from: the content it
// announces, the context ID it is published under and the metadata it carries.
type AdvertSpec struct {
	ContextID string
	Digests   iter.Seq[mh.Multihash]
	Metadata  metadata.Metadata
}

// BatchPublisher publishes many advertisements under one commit.
type BatchPublisher interface {
	// PublishBatch generates one advertisement per spec and publishes them
	// all under a single commit. See [IPNIPublisher.PublishBatch].
	PublishBatch(ctx context.Context, provider peer.AddrInfo, specs []AdvertSpec) (ipld.Link, error)
}

// PublishBatch generates one advertisement per spec and publishes them all
// under a single commit: one signed head and one announce however many specs
// there are, where Publish pays both per advertisement. A spec whose content
// is already advertised with identical metadata is skipped. The returned link
// is the head after the commit, unchanged when every spec was skipped and nil
// when nothing has ever been published.
//
// On failure, every mapping from provider and context ID that this call
// wrote, the one being generated when it failed included, is put back to what
// it was, so calling again with the same specs publishes every one of them.
// Undoing a write can itself fail; the returned error then says which
// mappings could not be restored, and those may differ from what the store
// held before the call. Entries blocks are content addressed and are left in
// place. Like Publish, PublishBatch is not safe for concurrent use.
func (p *IPNIPublisher) PublishBatch(ctx context.Context, provider peer.AddrInfo, specs []AdvertSpec) (ipld.Link, error) {
	for _, spec := range specs {
		adv, undo, err := generateAd(ctx, p.store, provider.ID, provider.Addrs, []byte(spec.ContextID), spec.Metadata, false, spec.Digests)
		if errors.Is(err, ErrAlreadyAdvertised) {
			continue
		}
		if err != nil {
			cctx, cancel := cleanupContext(ctx)
			defer cancel()
			errs := []error{fmt.Errorf("generating IPNI advert: %w", err)}
			if undo != nil {
				if uerr := undo(cctx); uerr != nil {
					errs = append(errs, fmt.Errorf("rolling back the advert being generated: %w", uerr))
				}
			}
			errs = append(errs, p.batchPublisher.Discard(cctx))
			return nil, errors.Join(errs...)
		}
		p.batchPublisher.add(adv, undo)
	}
	link, err := p.batchPublisher.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("committing IPNI adverts: %w", err)
	}
	return link, nil
}

var _ BatchPublisher = (*IPNIPublisher)(nil)

// New creates a new IPNI publisher.
// IPNIPublisher is not safe for concurrent use. There is the risk of losing advertisements if Publish is called
// from concurrent goroutines. If you will be publishing from multiple goroutines concurrently, a synchronization
// mechanism (such as sync.Mutex) must be used to ensure that Publish is called serially.
func New(id crypto.PrivKey, store store.PublisherStore, opts ...Option) (*IPNIPublisher, error) {
	bp, err := NewAdvertisementPublisher(id, store, opts...)
	if err != nil {
		return nil, err
	}
	return &IPNIPublisher{
		batchPublisher: bp,
		store:          store,
	}, nil
}

func (p *IPNIPublisher) publishAdvForIndex(ctx context.Context, peer peer.ID, addrs []multiaddr.Multiaddr, contextID []byte, md metadata.Metadata, isRm bool, mhs iter.Seq[mh.Multihash]) (ipld.Link, error) {
	adv, undo, err := generateAd(ctx, p.store, peer, addrs, contextID, md, isRm, mhs)
	if err != nil {
		// These two report the store as it was found; nothing was written.
		if errors.Is(err, ErrAlreadyAdvertised) || errors.Is(err, ErrContextIDNotFound) || undo == nil {
			return nil, err
		}
		cctx, cancel := cleanupContext(ctx)
		defer cancel()
		if uerr := undo(cctx); uerr != nil {
			return nil, errors.Join(err, fmt.Errorf("rolling back the advert being generated: %w", uerr))
		}
		return nil, err
	}
	p.batchPublisher.add(adv, undo)
	return p.batchPublisher.Commit(ctx)
}

type simpleAsyncPublisher struct {
	publisher Publisher
}

func AsyncFrom(p Publisher) AsyncPublisher {
	return &simpleAsyncPublisher{
		publisher: p,
	}
}

func (s *simpleAsyncPublisher) Publish(ctx context.Context, provider peer.AddrInfo, contextID string, digests iter.Seq[mh.Multihash], meta metadata.Metadata) error {
	_, err := s.publisher.Publish(ctx, provider, contextID, digests, meta)
	return err
}
