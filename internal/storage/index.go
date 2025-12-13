package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"nano-elastic/internal/types"
)

// IndexManager manages the storage for an index
type IndexManager struct {
	Name      string
	BasePath  string
	Schema    *types.Schema
	segments  []*Segment
	wal       *WAL
	mu        sync.RWMutex
	nextSegID int
}

// NewIndexManager creates a new index manager
func NewIndexManager(name string, basePath string, schema *types.Schema) (*IndexManager, error) {
	indexPath := filepath.Join(basePath, name)
	
	// Create index directory if it doesn't exist
	if err := os.MkdirAll(indexPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create index directory: %w", err)
	}
	
	// Create WAL
	wal, err := NewWAL(indexPath)
	if err != nil {
		return nil, err
	}
	
	if err := wal.Open(); err != nil {
		return nil, err
	}
	
	im := &IndexManager{
		Name:     name,
		BasePath: indexPath,
		Schema:   schema,
		segments: make([]*Segment, 0),
		wal:      wal,
	}
	
	// Load existing segments
	if err := im.loadSegments(); err != nil {
		return nil, err
	}
	
	// If no segments exist, create the first one
	if len(im.segments) == 0 {
		seg, err := im.createSegment()
		if err != nil {
			return nil, err
		}
		im.segments = append(im.segments, seg)
	}
	
	return im, nil
}

// loadSegments loads existing segments from disk
func (im *IndexManager) loadSegments() error {
	entries, err := os.ReadDir(im.BasePath)
	if err != nil {
		return err
	}
	
	for _, entry := range entries {
		filename := entry.Name()
		if !entry.IsDir() && filepath.Ext(filename) == ".dat" && len(filename) > 8 && filename[:8] == "segment_" {
			// This is a segment file (segment_<id>.dat)
			// Extract segment ID from filename
			segID := filename[8 : len(filename)-4] // Remove "segment_" prefix and ".dat" suffix
			
			seg, err := NewSegment(segID, im.BasePath)
			if err != nil {
				continue
			}
			
			if err := seg.Open(); err != nil {
				continue
			}
			
			im.segments = append(im.segments, seg)
		}
	}
	
	return nil
}

// createSegment creates a new segment
func (im *IndexManager) createSegment() (*Segment, error) {
	im.nextSegID++
	segID := fmt.Sprintf("seg%d", im.nextSegID)
	
	seg, err := NewSegment(segID, im.BasePath)
	if err != nil {
		return nil, err
	}
	
	seg.Created = time.Now().Unix()
	
	if err := seg.Open(); err != nil {
		return nil, err
	}
	
	return seg, nil
}

// WriteDocument writes a document to the index
func (im *IndexManager) WriteDocument(doc *types.Document) error {
	im.mu.Lock()
	defer im.mu.Unlock()
	
	// Validate document against schema
	if err := im.Schema.ValidateDocument(doc); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}
	
	// Write to WAL first (for durability)
	if err := im.wal.WriteEntry(WALEntryWrite, im.Name, doc.ID, doc); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}
	
	// Write to current segment
	if len(im.segments) == 0 {
		return fmt.Errorf("no segments available")
	}
	currentSeg := im.segments[len(im.segments)-1]
	if err := currentSeg.WriteDocument(doc); err != nil {
		return fmt.Errorf("failed to write to segment: %w", err)
	}
	
	// Flush index periodically (for now, flush after each write for Phase 1)
	// In production, we'd batch this
	if err := currentSeg.Flush(); err != nil {
		return fmt.Errorf("failed to flush segment: %w", err)
	}
	
	return nil
}

// ReadDocument reads a document from the index by ID
func (im *IndexManager) ReadDocument(id string) (*types.Document, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()
	
	// Search through segments (newest first)
	for i := len(im.segments) - 1; i >= 0; i-- {
		seg := im.segments[i]
		doc, err := seg.ReadDocument(id)
		if err == nil {
			return doc, nil
		}
		// Continue to next segment if document not found in this one
	}
	
	return nil, fmt.Errorf("document not found: %s", id)
}

// GetDocumentCount returns the total number of documents in the index
func (im *IndexManager) GetDocumentCount() int {
	im.mu.RLock()
	defer im.mu.RUnlock()
	
	total := 0
	for _, seg := range im.segments {
		total += seg.GetDocCount()
	}
	
	return total
}

// Close closes the index manager and all its resources
func (im *IndexManager) Close() error {
	im.mu.Lock()
	defer im.mu.Unlock()
	
	// Close all segments
	for _, seg := range im.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	
	// Close WAL
	if err := im.wal.Close(); err != nil {
		return err
	}
	
	return nil
}

