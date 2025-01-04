package storage

import (
	"encoding/binary"
	"fmt"
)

const (
	// PageSize is the fixed size of each page (4KB - standard for databases)
	PageSize = 4096
	
	// PageHeaderSize is the size of the page header
	PageHeaderSize = 16
	
	// MaxPageID represents the maximum page ID  
	MaxPageID = 0xFFFFFFFE
	
	// InvalidPageID represents an invalid page ID
	InvalidPageID = 0xFFFFFFFF
)

// PageID represents a unique page identifier
type PageID uint32

// PageType represents the type of data stored in a page
type PageType byte

const (
	FreePageType     PageType = 0
	BTreeLeafType    PageType = 1
	BTreeInternalType PageType = 2
	MetaPageType     PageType = 3
)

// PageHeader contains metadata for each page
type PageHeader struct {
	PageType   PageType // 1 byte
	Reserved   byte     // 1 byte - for future use
	DataLength uint16   // 2 bytes - actual data length in page
	NextPage   PageID   // 4 bytes - next page in chain (if applicable)
	PrevPage   PageID   // 4 bytes - previous page in chain (if applicable)
	Checksum   uint32   // 4 bytes - simple checksum
}

// Page represents a fixed-size disk page
type Page struct {
	ID     PageID
	Header PageHeader
	Data   [PageSize - PageHeaderSize]byte
}

// NewPage creates a new empty page
func NewPage(id PageID, pageType PageType) *Page {
	return &Page{
		ID: id,
		Header: PageHeader{
			PageType:   pageType,
			DataLength: 0,
			NextPage:   InvalidPageID,
			PrevPage:   InvalidPageID,
		},
	}
}

// Serialize converts the page to bytes for disk storage
func (p *Page) Serialize() []byte {
	buf := make([]byte, PageSize)
	
	// Write header
	buf[0] = byte(p.Header.PageType)
	buf[1] = p.Header.Reserved
	binary.LittleEndian.PutUint16(buf[2:4], p.Header.DataLength)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(p.Header.NextPage))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(p.Header.PrevPage))
	binary.LittleEndian.PutUint32(buf[12:16], p.Header.Checksum)
	
	// Write data
	copy(buf[PageHeaderSize:], p.Data[:])
	
	return buf
}

// Deserialize loads a page from bytes
func (p *Page) Deserialize(data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("invalid page size: got %d, expected %d", len(data), PageSize)
	}
	
	// Read header
	p.Header.PageType = PageType(data[0])
	p.Header.Reserved = data[1]
	p.Header.DataLength = binary.LittleEndian.Uint16(data[2:4])
	p.Header.NextPage = PageID(binary.LittleEndian.Uint32(data[4:8]))
	p.Header.PrevPage = PageID(binary.LittleEndian.Uint32(data[8:12]))
	p.Header.Checksum = binary.LittleEndian.Uint32(data[12:16])
	
	// Read data
	copy(p.Data[:], data[PageHeaderSize:])
	
	return nil
}

// SetData sets the page data and updates the data length
func (p *Page) SetData(data []byte) error {
	if len(data) > len(p.Data) {
		return fmt.Errorf("data too large for page: %d > %d", len(data), len(p.Data))
	}
	
	copy(p.Data[:], data)
	p.Header.DataLength = uint16(len(data))
	
	// Clear remaining bytes
	for i := len(data); i < len(p.Data); i++ {
		p.Data[i] = 0
	}
	
	return nil
}

// GetData returns the actual data in the page (up to DataLength)
func (p *Page) GetData() []byte {
	return p.Data[:p.Header.DataLength]
}

// AvailableSpace returns how much space is left in the page
func (p *Page) AvailableSpace() int {
	return len(p.Data) - int(p.Header.DataLength)
}

// updateChecksum calculates and sets a simple checksum
func (p *Page) updateChecksum() {
	// Simple checksum: sum of all data bytes
	var sum uint32
	for i := 0; i < int(p.Header.DataLength); i++ {
		sum += uint32(p.Data[i])
	}
	p.Header.Checksum = sum
}