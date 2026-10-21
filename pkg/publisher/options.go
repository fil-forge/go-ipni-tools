package publisher

import (
	"net/url"

	"github.com/multiformats/go-multiaddr"
)

// Option is an option configuring a publisher.
type Option func(cfg *options) error

type options struct {
	pubHTTPAnnounceAddrs []multiaddr.Multiaddr
	topic                string
	announceURLs         []*url.URL
	transactional        bool
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

// WithTransactionalStore tells the publisher that the caller runs each
// publish in a transaction over the store and rolls it back when publishing
// fails. The publisher then leaves undoing its store writes to that rollback:
// an undo issued inside a transaction the database has already aborted would
// fail, and its error would hide the cause.
func WithTransactionalStore() Option {
	return func(opts *options) error {
		opts.transactional = true
		return nil
	}
}

func WithTopic(topic string) Option {
	return func(opts *options) error {
		opts.topic = topic
		return nil
	}
}
