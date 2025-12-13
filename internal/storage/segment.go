package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"nano-elastic/internal/types"
)

// Segment represents a storage segment containing documents
type Segment struct {
	ID          string
	Path        string
	DocCount    int
	Size        int64
	Created     int64
	Version     int
	mu          sync.RWMutex
	file        *os.File
	docIndex    map[string]int64 // Document ID -> file offset
	initialized bool
}

// SegmentHeader is written at the beginning of each segment file
type SegmentHeader struct {
	Magic        [4]byte // Magic number: "NSEG"
	Version      uint16
	DocCount     uint32
	Created      int64
	IndexOffset  int64  // Offset where the index starts (at end of file)
	Reserved     [8]byte // Reserved for future use
}

const (
	SegmentMagic = "NSEG"
	SegmentVersion = 1
)

// NewSegment creates a new segment
func NewSegment(id string, basePath string) (*Segment, error) {
	segmentPath := filepath.Join(basePath, fmt.Sprintf("segment_%s.dat", id))
	
	seg := &Segment{
		ID:       id,
		Path:     segmentPath,
		DocCount: 0,
		Version:  SegmentVersion,
		docIndex: make(map[string]int64),
		Created:  time.Now().Unix(),
	}
	
	return seg, nil
}

// Open opens an existing segment file
func (s *Segment) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.initialized {
		return nil
	}
	
	var err error
	s.file, err = os.OpenFile(s.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open segment file: %w", err)
	}
	
	// Check if file is empty (new segment)
	stat, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat segment file: %w", err)
	}
	
	if stat.Size() == 0 {
		// Write header for new segment
		if err := s.writeHeader(); err != nil {
			return err
		}
		s.initialized = true
		return nil
	}
	
	// Read header from existing segment
	header, err := s.readHeader()
	if err != nil {
		return err
	}
	
	// Read document index (it's at the end, position stored in header)
	if header.IndexOffset > 0 {
		if err := s.readIndexAt(header.IndexOffset); err != nil {
			return err
		}
	}
	
	s.initialized = true
	return nil
}

// writeHeader writes the segment header
func (s *Segment) writeHeader() error {
	header := SegmentHeader{
		Version:  SegmentVersion,
		DocCount: uint32(s.DocCount),
		Created:  s.Created,
	}
	copy(header.Magic[:], SegmentMagic)
	
	if err := binary.Write(s.file, binary.LittleEndian, header); err != nil {
		return fmt.Errorf("failed to write segment header: %w", err)
	}
	
	return nil
}

// readHeader reads the segment header
func (s *Segment) readHeader() (*SegmentHeader, error) {
	var header SegmentHeader
	if err := binary.Read(s.file, binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("failed to read segment header: %w", err)
	}
	
	// Validate magic number
	if string(header.Magic[:]) != SegmentMagic {
		return nil, fmt.Errorf("invalid segment magic number")
	}
	
	s.Version = int(header.Version)
	s.DocCount = int(header.DocCount)
	s.Created = header.Created
	
	return &header, nil
}

// readIndexAt reads the index from a specific offset
func (s *Segment) readIndexAt(offset int64) error {
	if offset <= 0 {
		// No index yet
		s.docIndex = make(map[string]int64)
		return nil
	}
	
	if _, err := s.file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to index at offset %d: %w", offset, err)
	}
	
	var count uint32
	if err := binary.Read(s.file, binary.LittleEndian, &count); err != nil {
		if err == io.EOF {
			s.docIndex = make(map[string]int64)
			return nil // Empty index
		}
		return fmt.Errorf("failed to read index count: %w", err)
	}
	
	s.docIndex = make(map[string]int64, count)
	
	for i := uint32(0); i < count; i++ {
		// Read document ID length
		var idLen uint16
		if err := binary.Read(s.file, binary.LittleEndian, &idLen); err != nil {
			return fmt.Errorf("failed to read ID length: %w", err)
		}
		
		// Read document ID
		idBytes := make([]byte, idLen)
		if _, err := io.ReadFull(s.file, idBytes); err != nil {
			return fmt.Errorf("failed to read ID: %w", err)
		}
		
		// Read offset
		var docOffset int64
		if err := binary.Read(s.file, binary.LittleEndian, &docOffset); err != nil {
			return fmt.Errorf("failed to read offset: %w", err)
		}
		
		s.docIndex[string(idBytes)] = docOffset
	}
	
	return nil
}


// writeIndex writes the document index to the end of the segment file
func (s *Segment) writeIndex() error {
	// Seek to end of file (where index will be written)
	indexOffset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end: %w", err)
	}
	
	// Write index count
	count := uint32(len(s.docIndex))
	if err := binary.Write(s.file, binary.LittleEndian, count); err != nil {
		return fmt.Errorf("failed to write index count: %w", err)
	}
	
	// Write index entries
	for id, offset := range s.docIndex {
		idBytes := []byte(id)
		idLen := uint16(len(idBytes))
		
		// Write ID length
		if err := binary.Write(s.file, binary.LittleEndian, idLen); err != nil {
			return fmt.Errorf("failed to write ID length: %w", err)
		}
		
		// Write ID
		if _, err := s.file.Write(idBytes); err != nil {
			return fmt.Errorf("failed to write ID: %w", err)
		}
		
		// Write offset
		if err := binary.Write(s.file, binary.LittleEndian, offset); err != nil {
			return fmt.Errorf("failed to write offset: %w", err)
		}
	}
	
	// Update header with index offset
	currentPos, _ := s.file.Seek(0, io.SeekCurrent)
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	
	var header SegmentHeader
	if err := binary.Read(s.file, binary.LittleEndian, &header); err != nil {
		return err
	}
	
	header.IndexOffset = indexOffset
	header.DocCount = uint32(s.DocCount)
	
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	
	if err := binary.Write(s.file, binary.LittleEndian, header); err != nil {
		return err
	}
	
	// Restore position
	if _, err := s.file.Seek(currentPos, io.SeekStart); err != nil {
		return err
	}
	
	return nil
}

// WriteDocument writes a document to the segment
func (s *Segment) WriteDocument(doc *types.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if !s.initialized {
		if err := s.Open(); err != nil {
			return err
		}
	}
	
	// Serialize document to JSON
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}
	
	// Get current file size to determine where to write
	stat, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	
	headerSize := int64(binary.Size(SegmentHeader{}))
	
	// If file only has header, start writing documents after header
	// Otherwise, if there's an index at the end, we need to remove it first
	// For simplicity, we'll append documents and rewrite index at end
	var writeOffset int64
	
	if stat.Size() == headerSize {
		// New segment, write after header
		writeOffset = headerSize
		if _, err := s.file.Seek(writeOffset, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek: %w", err)
		}
	} else {
		// Existing segment, check if there's an index at the end
		// Read header to get index offset
		if _, err := s.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		var header SegmentHeader
		if err := binary.Read(s.file, binary.LittleEndian, &header); err != nil {
			return err
		}
		
		if header.IndexOffset > 0 {
			// Truncate file to remove old index
			if err := s.file.Truncate(header.IndexOffset); err != nil {
				return err
			}
			writeOffset = header.IndexOffset
		} else {
			// No index yet, append
			writeOffset = stat.Size()
		}
		
		if _, err := s.file.Seek(writeOffset, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek: %w", err)
		}
	}
	
	// Write document length
	docLen := uint32(len(docBytes))
	if err := binary.Write(s.file, binary.LittleEndian, docLen); err != nil {
		return fmt.Errorf("failed to write document length: %w", err)
	}
	
	// Write document data
	if _, err := s.file.Write(docBytes); err != nil {
		return fmt.Errorf("failed to write document: %w", err)
	}
	
	// Update index in memory FIRST (before writing to disk)
	if s.docIndex == nil {
		s.docIndex = make(map[string]int64)
	}
	s.docIndex[doc.ID] = writeOffset
	
	// Debug: verify index was updated
	if _, exists := s.docIndex[doc.ID]; !exists {
		return fmt.Errorf("failed to update index for document %s", doc.ID)
	}
	
	// Update document count
	s.DocCount++
	
	// Update header (but don't write index yet - keep it in memory for now)
	if err := s.updateHeader(); err != nil {
		return err
	}
	
	// Sync to disk (document is written, index stays in memory)
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync segment: %w", err)
	}
	
	return nil
}


// ReadDocument reads a document from the segment by ID
func (s *Segment) ReadDocument(id string) (*types.Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if !s.initialized {
		if err := s.Open(); err != nil {
			return nil, err
		}
	}
	
	// Ensure index is initialized
	if s.docIndex == nil {
		s.docIndex = make(map[string]int64)
	}
	
	offset, ok := s.docIndex[id]
	if !ok {
		// Debug: show what IDs we have
		ids := make([]string, 0, len(s.docIndex))
		for k := range s.docIndex {
			ids = append(ids, k)
		}
		if len(ids) == 0 {
			return nil, fmt.Errorf("document not found: %s (index is empty, segment: %s, initialized: %v)", id, s.ID, s.initialized)
		}
		return nil, fmt.Errorf("document not found: %s (available in segment %s: %v)", id, s.ID, ids)
	}
	
	// Seek to document position
	if _, err := s.file.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to document: %w", err)
	}
	
	// Read document length
	var docLen uint32
	if err := binary.Read(s.file, binary.LittleEndian, &docLen); err != nil {
		return nil, fmt.Errorf("failed to read document length: %w", err)
	}
	
	// Read document data
	docBytes := make([]byte, docLen)
	if _, err := io.ReadFull(s.file, docBytes); err != nil {
		return nil, fmt.Errorf("failed to read document: %w", err)
	}
	
	// Deserialize document
	var doc types.Document
	if err := json.Unmarshal(docBytes, &doc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal document: %w", err)
	}
	
	return &doc, nil
}

// updateHeader updates the segment header with current values
func (s *Segment) updateHeader() error {
	currentPos, err := s.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	
	// Seek to beginning
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	
	// Update created time if not set
	if s.Created == 0 {
		s.Created = time.Now().Unix()
	}
	
	// Write updated header
	if err := s.writeHeader(); err != nil {
		return err
	}
	
	// Restore position
	if _, err := s.file.Seek(currentPos, io.SeekStart); err != nil {
		return err
	}
	
	return nil
}

// Flush writes the index to disk
func (s *Segment) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if !s.initialized || s.file == nil {
		return nil
	}
	
	// Write index at end
	return s.writeIndex()
}

// Close closes the segment file
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Flush index before closing
	if s.initialized && s.file != nil {
		if err := s.writeIndex(); err != nil {
			// Log error but continue with close
		}
	}
	
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			return err
		}
		s.file = nil
	}
	
	s.initialized = false
	return nil
}

// GetDocCount returns the number of documents in the segment
func (s *Segment) GetDocCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.DocCount
}

// GetAllDocIDs returns all document IDs in the segment
func (s *Segment) GetAllDocIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	ids := make([]string, 0, len(s.docIndex))
	for id := range s.docIndex {
		ids = append(ids, id)
	}
	return ids
}

