package inverted

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// IndexSegment represents a persisted segment of the inverted index
// Segments allow us to write indexes to disk and read them back
type IndexSegment struct {
	ID       string
	Path     string
	termDict map[string]*PostingList
	mu       sync.RWMutex
	file     *os.File
}

// SegmentHeader for inverted index segment
type SegmentHeader struct {
	Magic    [4]byte // "NINV"
	Version  uint16
	TermCount uint32
	Reserved [8]byte
}

const (
	IndexSegmentMagic   = "NINV"
	IndexSegmentVersion = 1
)

// NewIndexSegment creates a new index segment
func NewIndexSegment(id string, basePath string) (*IndexSegment, error) {
	segmentPath := filepath.Join(basePath, fmt.Sprintf("index_segment_%s.dat", id))
	
	return &IndexSegment{
		ID:       id,
		Path:     segmentPath,
		termDict: make(map[string]*PostingList),
	}, nil
}

// Write writes the index segment to disk
func (seg *IndexSegment) Write(index *InvertedIndex) error {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	
	var err error
	seg.file, err = os.OpenFile(seg.Path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open segment file: %w", err)
	}
	defer seg.file.Close()
	
	// Write header
	header := SegmentHeader{
		Version:   IndexSegmentVersion,
		TermCount: uint32(len(index.termDict)),
	}
	copy(header.Magic[:], IndexSegmentMagic)
	
	if err := binary.Write(seg.file, binary.LittleEndian, header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	
	// Write term dictionary
	index.mu.RLock()
	defer index.mu.RUnlock()
	
	for term, postingList := range index.termDict {
		// Write term length and term
		termBytes := []byte(term)
		termLen := uint16(len(termBytes))
		if err := binary.Write(seg.file, binary.LittleEndian, termLen); err != nil {
			return err
		}
		if _, err := seg.file.Write(termBytes); err != nil {
			return err
		}
		
		// Write posting list
		if err := seg.writePostingList(postingList); err != nil {
			return err
		}
	}
	
	return seg.file.Sync()
}

// writePostingList writes a posting list to the file
func (seg *IndexSegment) writePostingList(pl *PostingList) error {
	// Write document frequency
	if err := binary.Write(seg.file, binary.LittleEndian, uint32(pl.DocFreq)); err != nil {
		return err
	}
	
	// Write each posting
	for _, posting := range pl.Postings {
		// Write docID
		docIDBytes := []byte(posting.DocID)
		docIDLen := uint16(len(docIDBytes))
		if err := binary.Write(seg.file, binary.LittleEndian, docIDLen); err != nil {
			return err
		}
		if _, err := seg.file.Write(docIDBytes); err != nil {
			return err
		}
		
		// Write term frequency
		if err := binary.Write(seg.file, binary.LittleEndian, uint32(posting.TermFreq)); err != nil {
			return err
		}
		
		// Write positions count and positions
		posCount := uint32(len(posting.Positions))
		if err := binary.Write(seg.file, binary.LittleEndian, posCount); err != nil {
			return err
		}
		if posCount > 0 {
			if err := binary.Write(seg.file, binary.LittleEndian, posting.Positions); err != nil {
				return err
			}
		}
	}
	
	return nil
}

// Read reads an index segment from disk
func (seg *IndexSegment) Read() (*InvertedIndex, error) {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	
	var err error
	seg.file, err = os.Open(seg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file: %w", err)
	}
	defer seg.file.Close()
	
	// Read header
	var header SegmentHeader
	if err := binary.Read(seg.file, binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	
	// Validate magic
	if string(header.Magic[:]) != IndexSegmentMagic {
		return nil, fmt.Errorf("invalid segment magic")
	}
	
	// Create index
	index := NewInvertedIndex()
	index.termDict = make(map[string]*PostingList, header.TermCount)
	
	// Read term dictionary
	for i := uint32(0); i < header.TermCount; i++ {
		// Read term
		var termLen uint16
		if err := binary.Read(seg.file, binary.LittleEndian, &termLen); err != nil {
			return nil, err
		}
		
		termBytes := make([]byte, termLen)
		if _, err := io.ReadFull(seg.file, termBytes); err != nil {
			return nil, err
		}
		term := string(termBytes)
		
		// Read posting list
		postingList, err := seg.readPostingList()
		if err != nil {
			return nil, err
		}
		
		index.termDict[term] = postingList
	}
	
	return index, nil
}

// readPostingList reads a posting list from the file
func (seg *IndexSegment) readPostingList() (*PostingList, error) {
	pl := NewPostingList()
	
	// Read document frequency
	var docFreq uint32
	if err := binary.Read(seg.file, binary.LittleEndian, &docFreq); err != nil {
		return nil, err
	}
	pl.DocFreq = int(docFreq)
	
	// Read postings
	pl.Postings = make([]Posting, 0, docFreq)
	for i := uint32(0); i < docFreq; i++ {
		// Read docID
		var docIDLen uint16
		if err := binary.Read(seg.file, binary.LittleEndian, &docIDLen); err != nil {
			return nil, err
		}
		
		docIDBytes := make([]byte, docIDLen)
		if _, err := io.ReadFull(seg.file, docIDBytes); err != nil {
			return nil, err
		}
		
		// Read term frequency
		var termFreq uint32
		if err := binary.Read(seg.file, binary.LittleEndian, &termFreq); err != nil {
			return nil, err
		}
		
		// Read positions
		var posCount uint32
		if err := binary.Read(seg.file, binary.LittleEndian, &posCount); err != nil {
			return nil, err
		}
		
		positions := make([]int, posCount)
		if posCount > 0 {
			if err := binary.Read(seg.file, binary.LittleEndian, positions); err != nil {
				return nil, err
			}
		}
		
		pl.Postings = append(pl.Postings, Posting{
			DocID:     string(docIDBytes),
			TermFreq:  int(termFreq),
			Positions: positions,
		})
	}
	
	return pl, nil
}

