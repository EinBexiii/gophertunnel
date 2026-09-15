package protocol

const (
	HeightMapDataNone = iota
	HeightMapDataHasData
	HeightMapDataTooHigh
	HeightMapDataTooLow
	HeightMapDataAllCopied
)

const (
	SubChunkResultUndefined = iota
	SubChunkResultSuccess
	SubChunkResultChunkNotFound
	SubChunkResultInvalidDimension
	SubChunkResultPlayerNotFound
	SubChunkResultIndexOutOfBounds
	SubChunkResultSuccessAllAir
)

// SubChunkEntry contains the data of a sub-chunk entry relative to a center sub chunk position, used for the sub-chunk
// requesting system introduced in v1.18.10.
type SubChunkEntry struct {
	// Offset contains the offset between the sub-chunk position and the center position.
	Offset SubChunkOffset
	// Result is always one of the constants defined in the SubChunkResult constants.
	Result byte
	// RawPayload contains the serialized sub-chunk data, if present.
	RawPayload Optional[[]byte]
	// HeightMapType is always one of the constants defined in the HeightMapData constants.
	HeightMapType byte
	// HeightMapData is the data for the height map, if present.
	HeightMapData Optional[[]int8]
	// RenderHeightMapType is always one of the constants defined in the HeightMapData constants.
	RenderHeightMapType byte
	// RenderHeightMapData is the data for the render height map, if present.
	RenderHeightMapData Optional[[]int8]
	// BlobHash is the hash of the blob, if present.
	BlobHash Optional[uint64]
}

// Marshal encodes/decodes a SubChunkEntry.
func (x *SubChunkEntry) Marshal(r IO) {
	Single(r, &x.Offset)
	r.Uint8(&x.Result)
	OptionalFunc(r, &x.RawPayload, r.ByteSlice)
	r.Uint8(&x.HeightMapType)
	OptionalFunc(r, &x.HeightMapData, func(data *[]int8) {
		subChunkHeightMap(r, data)
	})
	r.Uint8(&x.RenderHeightMapType)
	OptionalFunc(r, &x.RenderHeightMapData, func(data *[]int8) {
		subChunkHeightMap(r, data)
	})
	OptionalFunc(r, &x.BlobHash, r.Uint64)
}

// subChunkHeightMapRows and subChunkHeightMapCols are the shape of a sub-chunk
// heightmap: one signed height per column of a 16 by 16 sub chunk.
const (
	subChunkHeightMapRows = 16
	subChunkHeightMapCols = 16
)

// subChunkHeightMap reads/writes a sub-chunk heightmap. It holds 256 heights,
// but is not a flat array on the wire: each of the 16 rows carries its own
// length, which is what Mojang's documentation means by array<array<int8>>.
// That is 16 lengths plus 256 values, so 272 bytes, and mistaking the byte
// count for an element count produces a flat 272-value array that decodes
// into the following fields and makes the client drop the connection.
func subChunkHeightMap(r IO, data *[]int8) {
	if len(*data) != subChunkHeightMapRows*subChunkHeightMapCols {
		*data = make([]int8, subChunkHeightMapRows*subChunkHeightMapCols)
	}
	for row := range subChunkHeightMapRows {
		n := uint32(subChunkHeightMapCols)
		r.Varuint32(&n)
		if n != subChunkHeightMapCols {
			r.InvalidValue(n, "sub-chunk heightmap row", "must hold exactly 16 heights")
			return
		}
		for col := range subChunkHeightMapCols {
			r.Int8(&(*data)[row*subChunkHeightMapCols+col])
		}
	}
}

// SubChunkOffset represents an offset from the base position of another sub chunk.
type SubChunkOffset [3]int8

// Marshal encodes/decodes a SubChunkOffset.
func (x *SubChunkOffset) Marshal(r IO) {
	r.Int8(&x[0])
	r.Int8(&x[1])
	r.Int8(&x[2])
}
