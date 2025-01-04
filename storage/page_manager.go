package storage

import (
	"fmt"
	"os"
	"sync"
)

// PageManager manages disk pages for the database
type PageManager struct {
	file     *os.File
	mu       sync.RWMutex
	nextPage PageID
	freeList []PageID // Simple free list for deallocated pages
}

// NewPageManager creates a new page manager for the given database file
func NewPageManager(filename string) (*PageManager, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}
	
	pm := &PageManager{
		file:     file,
		nextPage: 1, // Page 0 is reserved for metadata
		freeList: make([]PageID, 0),
	}
	
	// Initialize database if it's new (empty file)
	if err := pm.initializeIfEmpty(); err != nil {
		file.Close()
		return nil, err
	}
	
	return pm, nil
}

// Close closes the page manager and underlying file
func (pm *PageManager) Close() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	if pm.file != nil {
		err := pm.file.Close()
		pm.file = nil
		return err
	}
	return nil
}

// AllocatePage allocates a new page and returns its ID
func (pm *PageManager) AllocatePage(pageType PageType) (PageID, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	var pageID PageID
	
	// Try to reuse a page from the free list first
	if len(pm.freeList) > 0 {
		pageID = pm.freeList[len(pm.freeList)-1]
		pm.freeList = pm.freeList[:len(pm.freeList)-1]
	} else {
		// Allocate a new page at the end of file
		pageID = pm.nextPage
		pm.nextPage++
	}
	
	// Create and write an empty page
	page := NewPage(pageID, pageType)
	return pageID, pm.writePageLocked(page)
}

// DeallocatePage marks a page as free for reuse
func (pm *PageManager) DeallocatePage(pageID PageID) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	// Mark page as free
	page := NewPage(pageID, FreePageType)
	if err := pm.writePageLocked(page); err != nil {
		return err
	}
	
	// Add to free list
	pm.freeList = append(pm.freeList, pageID)
	return nil
}

// ReadPage reads a page from disk
func (pm *PageManager) ReadPage(pageID PageID) (*Page, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	return pm.readPageLocked(pageID)
}

// WritePage writes a page to disk
func (pm *PageManager) WritePage(page *Page) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	return pm.writePageLocked(page)
}

// readPageLocked reads a page while holding the lock
func (pm *PageManager) readPageLocked(pageID PageID) (*Page, error) {
	if pageID == InvalidPageID {
		return nil, fmt.Errorf("invalid page ID for read: %d", pageID)
	}
	
	offset := int64(pageID) * PageSize
	
	buf := make([]byte, PageSize)
	n, err := pm.file.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}
	if n != PageSize {
		return nil, fmt.Errorf("incomplete page read: got %d bytes, expected %d", n, PageSize)
	}
	
	page := &Page{ID: pageID}
	if err := page.Deserialize(buf); err != nil {
		return nil, fmt.Errorf("failed to deserialize page %d: %w", pageID, err)
	}
	
	return page, nil
}

// writePageLocked writes a page while holding the lock
func (pm *PageManager) writePageLocked(page *Page) error {
	if page.ID == InvalidPageID {
		return fmt.Errorf("invalid page ID for write: %d", page.ID)
	}
	
	// Update checksum before writing
	page.updateChecksum()
	
	offset := int64(page.ID) * PageSize
	buf := page.Serialize()
	
	n, err := pm.file.WriteAt(buf, offset)
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", page.ID, err)
	}
	if n != PageSize {
		return fmt.Errorf("incomplete page write: wrote %d bytes, expected %d", n, PageSize)
	}
	
	// Ensure data is written to disk
	return pm.file.Sync()
}

// initializeIfEmpty initializes an empty database file with metadata
func (pm *PageManager) initializeIfEmpty() error {
	stat, err := pm.file.Stat()
	if err != nil {
		return err
	}
	
	// If file is empty, initialize with metadata page
	if stat.Size() == 0 {
		metaPage := NewPage(0, MetaPageType)
		metaData := []byte("SIMPLEDB_V1") // Simple magic header
		if err := metaPage.SetData(metaData); err != nil {
			return err
		}
		
		return pm.writePageLocked(metaPage)
	}
	
	return nil
}

// Sync forces any pending writes to disk
func (pm *PageManager) Sync() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	return pm.file.Sync()
}