package store_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	ipldschema "github.com/ipld/go-ipld-prime/schema"
	libschema "github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/go-ipni-tools/internal/testutil"
	"github.com/fil-forge/go-ucanto/core/ipld/block"
	ucborcodec "github.com/fil-forge/go-ucanto/core/ipld/codec/cbor"
	ujsoncodec "github.com/fil-forge/go-ucanto/core/ipld/codec/json"
	"github.com/fil-forge/go-ucanto/core/ipld/hash/sha256"

	"github.com/fil-forge/go-ipni-tools/pkg/store"
)

var customMetadataID = multicodec.Code(0x3E0000)

type customMetadata struct {
	Data string
}

func (c *customMetadata) ID() multicodec.Code {
	return customMetadataID
}

func (c *customMetadata) MarshalBinary() (data []byte, err error) {
	buf := bytes.NewBuffer(varint.ToUvarint(uint64(c.ID())))
	nd := bindnode.Wrap(c, customMetadataPrototype().Type())
	if err := dagcbor.Encode(nd, buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *customMetadata) ReadFrom(r io.Reader) (n int64, err error) {
	return readFrom(c, r)
}

func readFrom[T any](val *T, r io.Reader) (int64, error) {
	cr := &countingReader{r: r}
	v, err := varint.ReadUvarint(cr)
	if err != nil {
		return cr.readCount, err
	}
	id := multicodec.Code(v)
	if id != customMetadataID {
		return cr.readCount, fmt.Errorf("transport id does not match %s: %s", customMetadataID, id)
	}

	nb := customMetadataPrototype().NewBuilder()
	err = dagcbor.Decode(nb, cr)
	if err != nil {
		return cr.readCount, err
	}
	nd := nb.Build()
	read := bindnode.Unwrap(nd).(*T)
	*val = *read
	return cr.readCount, nil
}

func (c *customMetadata) UnmarshalBinary(data []byte) error {
	r := bytes.NewReader(data)
	_, err := readFrom(c, r)
	return err
}

var _ metadata.Protocol = (*customMetadata)(nil)

func customMetadataPrototype() ipldschema.TypedPrototype {
	typeSystem, err := ipld.LoadSchemaBytes([]byte(`
	  type CustomMetadata struct {
		  Data String
		}
	`))
	if err != nil {
		panic(fmt.Errorf("failed to load schema: %w", err))
	}
	return bindnode.Prototype((*customMetadata)(nil), typeSystem.TypeByName("CustomMetadata"))
}

type countingReader struct {
	readCount int64
	r         io.Reader
}

func (c *countingReader) ReadByte() (byte, error) {
	b := []byte{0}
	_, err := c.Read(b)
	return b[0], err
}

func (c *countingReader) Read(b []byte) (n int, err error) {
	read, err := c.r.Read(b)
	c.readCount += int64(read)
	return read, err
}

func TestCustomMetadataContext(t *testing.T) {
	mctx := metadata.Default.WithProtocol(customMetadataID, func() metadata.Protocol {
		return &customMetadata{}
	})

	s := store.FromDatastore(datastore.NewMapDatastore(), store.WithMetadataContext(mctx))

	peerID, err := peer.Decode("12D3KooWLpDkh3ZnFARvrQE1n3Ddb9G66YmfKxp3Z6EYMq1MmcoQ")
	require.NoError(t, err)
	contextID := []byte{1, 2, 3}
	md := mctx.New(&customMetadata{Data: "TEST"})

	err = s.PutMetadataForProviderAndContextID(context.Background(), peerID, contextID, md)
	require.NoError(t, err)

	r, err := s.MetadataForProviderAndContextID(context.Background(), peerID, contextID)
	require.NoError(t, err)
	require.Equal(t, md, r)
}

// TestPutAdvertAndEntriesUsesDagCBOR verifies that PutAdvert and PutEntries
// store blocks using the dag-cbor codec (as opposed to dag-json).
func TestPutAdvertAndEntriesUsesDagCBOR(t *testing.T) {
	ctx := context.Background()
	s := store.FromDatastore(datastore.NewMapDatastore())

	mhs := testutil.RandomMultihashes(t, 5)
	entriesLink, err := s.PutEntries(ctx, slices.Values(mhs))
	require.NoError(t, err)
	require.Equal(t, uint64(multicodec.DagCbor), entriesLink.(cidlink.Link).Cid.Prefix().Codec,
		"PutEntries should produce a dag-cbor CID")

	// Verify entries can be read back correctly.
	var gotMhs []multihash.Multihash
	for mh, err := range s.Entries(ctx, entriesLink) {
		require.NoError(t, err)
		gotMhs = append(gotMhs, mh)
	}
	require.Equal(t, mhs, gotMhs)

	peerID, err := peer.Decode("12D3KooWLpDkh3ZnFARvrQE1n3Ddb9G66YmfKxp3Z6EYMq1MmcoQ")
	require.NoError(t, err)
	md := metadata.Default.New()
	mdBytes, err := md.MarshalBinary()
	require.NoError(t, err)

	ad := libschema.Advertisement{
		Provider:  peerID.String(),
		Addresses: []string{},
		Entries:   entriesLink,
		ContextID: []byte{1},
		Metadata:  mdBytes,
	}
	adLink, err := s.PutAdvert(ctx, ad)
	require.NoError(t, err)
	require.Equal(t, uint64(multicodec.DagCbor), adLink.(cidlink.Link).Cid.Prefix().Codec,
		"PutAdvert should produce a dag-cbor CID")

	// Verify the advertisement can be read back correctly.
	gotAd, err := s.Advert(ctx, adLink)
	require.NoError(t, err)
	require.Equal(t, ad.Provider, gotAd.Provider)
	require.Equal(t, ad.ContextID, gotAd.ContextID)
}

// TestEntriesMixedDagJsonCBORChain verifies that a chain containing both
// legacy dag-json entry chunks and new dag-cbor entry chunks can be fully
// iterated via Entries.
func TestEntriesMixedDagJsonCBORChain(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewMapDatastore()

	// Build a legacy dag-json entry chunk and insert it directly into the store.
	jsonMhs := testutil.RandomMultihashes(t, 3)
	jsonChunk := &libschema.EntryChunk{Entries: jsonMhs}
	jsonBlk, err := block.Encode(jsonChunk, libschema.EntryChunkPrototype.Type(), ujsoncodec.Codec, sha256.Hasher)
	require.NoError(t, err)
	require.Equal(t, uint64(multicodec.DagJson), jsonBlk.Link().(cidlink.Link).Cid.Prefix().Codec,
		"manually encoded block should use dag-json codec")

	ss := store.SimpleStoreFromDatastore(ds)
	err = ss.Put(ctx, jsonBlk.Link().String(), uint64(len(jsonBlk.Bytes())), bytes.NewReader(jsonBlk.Bytes()))
	require.NoError(t, err)

	// Build a new dag-cbor entry chunk whose Next pointer refers to the dag-json chunk.
	cborMhs := testutil.RandomMultihashes(t, 3)
	cborChunk := &libschema.EntryChunk{Entries: cborMhs, Next: jsonBlk.Link()}
	cborBlk, err := block.Encode(cborChunk, libschema.EntryChunkPrototype.Type(), ucborcodec.Codec, sha256.Hasher)
	require.NoError(t, err)
	require.Equal(t, uint64(multicodec.DagCbor), cborBlk.Link().(cidlink.Link).Cid.Prefix().Codec,
		"new block should use dag-cbor codec")

	err = ss.Put(ctx, cborBlk.Link().String(), uint64(len(cborBlk.Bytes())), bytes.NewReader(cborBlk.Bytes()))
	require.NoError(t, err)

	// Read the mixed chain via the AdStore.
	s := store.FromDatastore(ds)
	var gotMhs []multihash.Multihash
	for mh, err := range s.Entries(ctx, cborBlk.Link()) {
		require.NoError(t, err)
		gotMhs = append(gotMhs, mh)
	}

	// Expect cbor mhs first, then json mhs (chain order).
	expected := make([]multihash.Multihash, 0, len(cborMhs)+len(jsonMhs))
	expected = append(expected, cborMhs...)
	expected = append(expected, jsonMhs...)
	require.Equal(t, expected, gotMhs)
}
