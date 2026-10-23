package publisher

import (
	"net/url"

	"github.com/multiformats/go-multiaddr"

	"github.com/fil-forge/go-ipni-tools/pkg/store"
)

// Option is an option configuring a publisher.
type Option func(cfg *options) error

type options struct {
	pubHTTPAnnounceAddrs []multiaddr.Multiaddr
	topic                string
	announceURLs         []*url.URL
	batchStore           store.BatchStore
}

// WithDirectAnnounce sets indexer URLs to send direct HTTP announcements to.
func WithDirectAnnounce(announceURLs ...string) Option {
	return func(o *options) error {
		for _, urlStr := range announceURLs {
			u, err := url.Parse(urlStr)
			if err != nil {
				return err
			}
			o.announceURLs = append(o.announceURLs, u)
		}
		return nil
	}
}

// WithAnnounceAddrs configures the multiaddrs that are put into announce
// messages to tell indexers the addresses to fetch advertisements from.
func WithAnnounceAddrs(addrs ...string) Option {
	return func(opts *options) error {
		for _, addr := range addrs {
			if addr != "" {
				maddr, err := multiaddr.NewMultiaddr(addr)
				if err != nil {
					return err
				}
				opts.pubHTTPAnnounceAddrs = append(opts.pubHTTPAnnounceAddrs, maddr)
			}
		}
		return nil
	}
}

func WithTopic(topic string) Option {
	return func(opts *options) error {
		opts.topic = topic
		return nil
	}
}

// WithBatchStore sets the store a batch is published through. Without it, a
// batch reads and writes its mappings one call at a time through the
// publisher's store, and writes back the mappings it changed when its commit
// fails. A store with transactions, or with calls that read and write many
// mappings at once, publishes a batch with fewer calls.
func WithBatchStore(batchStore store.BatchStore) Option {
	return func(opts *options) error {
		opts.batchStore = batchStore
		return nil
	}
}
