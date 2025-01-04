package storage

import (
	"os"
	"testing"
	
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageBasicOperations(t *testing.T) {
	page := NewPage(1, BTreeLeafType)
	
	assert.Equal(t, PageID(1), page.ID)
	assert.Equal(t, BTreeLeafType, page.Header.PageType)
	assert.Equal(t, uint16(0), page.Header.DataLength)
	assert.Equal(t, PageSize-PageHeaderSize, page.AvailableSpace())
}

func TestPageSetGetData(t *testing.T) {
	page := NewPage(1, BTreeLeafType)
	testData := []byte("Hello, Database World!")
	
	err := page.SetData(testData)
	require.NoError(t, err)
	
	assert.Equal(t, uint16(len(testData)), page.Header.DataLength)
	assert.Equal(t, testData, page.GetData())
	assert.Equal(t, PageSize-PageHeaderSize-len(testData), page.AvailableSpace())
}

func TestPageSerialization(t *testing.T) {
	page := NewPage(42, BTreeInternalType)
	testData := []byte("Serialization test data")
	
	err := page.SetData(testData)
	require.NoError(t, err)
	
	// Serialize
	serialized := page.Serialize()
	assert.Equal(t, PageSize, len(serialized))
	
	// Deserialize into new page
	newPage := &Page{ID: 42}
	err = newPage.Deserialize(serialized)
	require.NoError(t, err)
	
	assert.Equal(t, page.ID, newPage.ID)
	assert.Equal(t, page.Header.PageType, newPage.Header.PageType)
	assert.Equal(t, page.Header.DataLength, newPage.Header.DataLength)
	assert.Equal(t, testData, newPage.GetData())
}

func TestPageDataTooLarge(t *testing.T) {
	page := NewPage(1, BTreeLeafType)
	
	// Try to set data larger than page capacity
	largeData := make([]byte, PageSize)
	err := page.SetData(largeData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "data too large")
}

func TestPageManager(t *testing.T) {
	// Create temporary database file
	tempFile := "test_db.dat"
	defer os.Remove(tempFile)
	
	pm, err := NewPageManager(tempFile)
	require.NoError(t, err)
	defer pm.Close()
	
	// Allocate a new page
	pageID, err := pm.AllocatePage(BTreeLeafType)
	require.NoError(t, err)
	assert.Equal(t, PageID(1), pageID) // First allocated page should be ID 1
	
	// Read the allocated page
	page, err := pm.ReadPage(pageID)
	require.NoError(t, err)
	assert.Equal(t, pageID, page.ID)
	assert.Equal(t, BTreeLeafType, page.Header.PageType)
}

func TestPageManagerWriteRead(t *testing.T) {
	tempFile := "test_db_write.dat"
	defer os.Remove(tempFile)
	
	pm, err := NewPageManager(tempFile)
	require.NoError(t, err)
	defer pm.Close()
	
	// Allocate and modify a page
	pageID, err := pm.AllocatePage(BTreeLeafType)
	require.NoError(t, err)
	
	page, err := pm.ReadPage(pageID)
	require.NoError(t, err)
	
	testData := []byte("Test data for page manager")
	err = page.SetData(testData)
	require.NoError(t, err)
	
	// Write back to disk
	err = pm.WritePage(page)
	require.NoError(t, err)
	
	// Read again and verify
	page2, err := pm.ReadPage(pageID)
	require.NoError(t, err)
	
	assert.Equal(t, testData, page2.GetData())
	assert.Equal(t, uint16(len(testData)), page2.Header.DataLength)
}

func TestPageManagerFreeList(t *testing.T) {
	tempFile := "test_db_freelist.dat"
	defer os.Remove(tempFile)
	
	pm, err := NewPageManager(tempFile)
	require.NoError(t, err)
	defer pm.Close()
	
	// Allocate multiple pages
	page1, err := pm.AllocatePage(BTreeLeafType)
	require.NoError(t, err)
	
	page2, err := pm.AllocatePage(BTreeLeafType)
	require.NoError(t, err)
	
	page3, err := pm.AllocatePage(BTreeLeafType)
	require.NoError(t, err)
	
	assert.Equal(t, PageID(1), page1)
	assert.Equal(t, PageID(2), page2)
	assert.Equal(t, PageID(3), page3)
	
	// Deallocate middle page
	err = pm.DeallocatePage(page2)
	require.NoError(t, err)
	
	// Allocate new page should reuse the freed page
	page4, err := pm.AllocatePage(BTreeInternalType)
	require.NoError(t, err)
	assert.Equal(t, page2, page4) // Should reuse page2's ID
	
	// Verify the page was actually marked as the new type
	reusedPage, err := pm.ReadPage(page4)
	require.NoError(t, err)
	assert.Equal(t, BTreeInternalType, reusedPage.Header.PageType)
}