package nbd

// Geometry describes an export's size and block size constraints.
type Geometry struct {
	Size               uint64
	MinimumBlockSize   uint64
	PreferredBlockSize uint64
	MaximumBlockSize   uint64
}
