package store_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	ipldschema "github.com/ipld/go-ipld-prime/schema"
	"github.com/ipni/go-libipni/dagsync/ipnisync/head"
	libschema "github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/go-ipni-tools/internal/testutil"
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
	jsonLink, jsonBytes := encodeBlock(t, jsonChunk, multicodec.DagJson, dagjson.Encode)
	require.Equal(t, uint64(multicodec.DagJson), jsonLink.(cidlink.Link).Cid.Prefix().Codec,
		"manually encoded block should use dag-json codec")

	ss := store.SimpleStoreFromDatastore(ds)
	err := ss.Put(ctx, jsonLink.String(), uint64(len(jsonBytes)), bytes.NewReader(jsonBytes))
	require.NoError(t, err)

	// Build a new dag-cbor entry chunk whose Next pointer refers to the dag-json chunk.
	cborMhs := testutil.RandomMultihashes(t, 3)
	cborChunk := &libschema.EntryChunk{Entries: cborMhs, Next: jsonLink}
	cborLink, cborBytes := encodeBlock(t, cborChunk, multicodec.DagCbor, dagcbor.Encode)
	require.Equal(t, uint64(multicodec.DagCbor), cborLink.(cidlink.Link).Cid.Prefix().Codec,
		"new block should use dag-cbor codec")

	err = ss.Put(ctx, cborLink.String(), uint64(len(cborBytes)), bytes.NewReader(cborBytes))
	require.NoError(t, err)

	// Read the mixed chain via the AdStore.
	s := store.FromDatastore(ds)
	var gotMhs []multihash.Multihash
	for mh, err := range s.Entries(ctx, cborLink) {
		require.NoError(t, err)
		gotMhs = append(gotMhs, mh)
	}

	// Expect cbor mhs first, then json mhs (chain order).
	expected := make([]multihash.Multihash, 0, len(cborMhs)+len(jsonMhs))
	expected = append(expected, cborMhs...)
	expected = append(expected, jsonMhs...)
	require.Equal(t, expected, gotMhs)
}

// TestStoredBlockCIDs verifies that blocks written by PutEntries, PutAdvert
// and ReplaceHead produce CIDs with the expected codec and a sha2-256
// multihash, that each CID is consistent with the stored bytes, and that the
// stored bytes round-trip through the go-libipni decoders.
func TestStoredBlockCIDs(t *testing.T) {
	ctx := context.Background()
	s := store.FromDatastore(datastore.NewMapDatastore())

	// Entries are dag-cbor encoded.
	mhs := testutil.RandomMultihashes(t, 3)
	entriesLink, err := s.PutEntries(ctx, slices.Values(mhs))
	require.NoError(t, err)
	entriesCid := entriesLink.(cidlink.Link).Cid
	requireCidPrefix(t, entriesCid, multicodec.DagCbor)
	entriesBytes := encodedBytes(t, ctx, s, entriesLink)
	requireCidMatchesBytes(t, entriesCid, entriesBytes)
	chunk, err := libschema.BytesToEntryChunk(entriesCid, entriesBytes)
	require.NoError(t, err)
	require.Equal(t, mhs, chunk.Entries)

	// Adverts are dag-cbor encoded.
	md := metadata.Default.New()
	mdBytes, err := md.MarshalBinary()
	require.NoError(t, err)
	ad := libschema.Advertisement{
		Provider:  testutil.RandomPeer(t).String(),
		Addresses: []string{},
		Entries:   entriesLink,
		ContextID: []byte{1},
		Metadata:  mdBytes,
	}
	adLink, err := s.PutAdvert(ctx, ad)
	require.NoError(t, err)
	adCid := adLink.(cidlink.Link).Cid
	requireCidPrefix(t, adCid, multicodec.DagCbor)
	adBytes := encodedBytes(t, ctx, s, adLink)
	requireCidMatchesBytes(t, adCid, adBytes)
	gotAd, err := libschema.BytesToAdvertisement(adCid, adBytes)
	require.NoError(t, err)
	require.Equal(t, ad.Provider, gotAd.Provider)
	require.Equal(t, ad.ContextID, gotAd.ContextID)
	require.Equal(t, ad.Entries, gotAd.Entries)

	// The head is dag-json encoded by convention.
	pk, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	sh, err := head.NewSignedHead(adCid, "/indexer/ingest/mainnet", pk)
	require.NoError(t, err)
	headLink, err := s.ReplaceHead(ctx, nil, sh)
	require.NoError(t, err)
	headCid := headLink.(cidlink.Link).Cid
	requireCidPrefix(t, headCid, multicodec.DagJson)
	var headBytes bytes.Buffer
	require.NoError(t, s.EncodeHead(ctx, &headBytes))
	requireCidMatchesBytes(t, headCid, headBytes.Bytes())
	gotHead, err := head.Decode(bytes.NewReader(headBytes.Bytes()))
	require.NoError(t, err)
	require.Equal(t, sh, gotHead)
}

// encodedBytes returns the raw stored bytes for the given link.
func encodedBytes(t *testing.T, ctx context.Context, s store.FullStore, lnk ipld.Link) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, s.Encode(ctx, lnk, &buf))
	return buf.Bytes()
}

// requireCidPrefix asserts the CID is v1 with the given codec and a sha2-256
// multihash.
func requireCidPrefix(t *testing.T, c cid.Cid, codec multicodec.Code) {
	t.Helper()
	prefix := c.Prefix()
	require.Equal(t, uint64(1), prefix.Version)
	require.Equal(t, uint64(codec), prefix.Codec)
	require.Equal(t, uint64(multihash.SHA2_256), prefix.MhType)
}

// requireCidMatchesBytes asserts the CID's digest is the sha2-256 hash of the
// given data.
func requireCidMatchesBytes(t *testing.T, c cid.Cid, data []byte) {
	t.Helper()
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	require.NoError(t, err)
	require.Equal(t, c, cid.NewCidV1(c.Prefix().Codec, mh))
}

// encodeBlock encodes an entry chunk with the given IPLD codec, mirroring the
// block encoding performed by the store.
func encodeBlock(t *testing.T, chunk *libschema.EntryChunk, codec multicodec.Code, encoder ipld.Encoder) (ipld.Link, []byte) {
	t.Helper()
	data, err := ipld.Marshal(encoder, chunk, libschema.EntryChunkPrototype.Type())
	require.NoError(t, err)
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cidlink.Link{Cid: cid.NewCidV1(uint64(codec), mh)}, data
}
