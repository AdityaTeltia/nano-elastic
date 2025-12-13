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

// WALEntryType represents the type of WAL entry
type WALEntryType uint8

const (
	WALEntryWrite WALEntryType = iota + 1
	WALEntryDelete
	WALEntryUpdate
)

// WALEntry represents a single entry in the write-ahead log
type WALEntry struct {
	Type      WALEntryType
	Index     string
	DocID     string
	Document  *types.Document
	Timestamp int64
	Sequence  uint64
}

// WAL (Write-Ahead Log) provides durability guarantees
type WAL struct {
	Path       string
	file       *os.File
	sequence   uint64
	mu         sync.Mutex
	initialized bool
}

// WALHeader is written at the beginning of the WAL file
type WALHeader struct {
	Magic    [4]byte // "NWAL"
	Version  uint16
	Sequence uint64
	Reserved [8]byte
}

const (
	WALMagic   = "NWAL"
	WALVersion = 1
)

// NewWAL creates a new write-ahead log
func NewWAL(basePath string) (*WAL, error) {
	walPath := filepath.Join(basePath, "wal.dat")
	
	wal := &WAL{
		Path: walPath,
	}
	
	return wal, nil
}

// Open opens the WAL file
func (w *WAL) Open() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.initialized {
		return nil
	}
	
	var err error
	w.file, err = os.OpenFile(w.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}
	
	// Check if file is empty
	stat, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat WAL file: %w", err)
	}
	
	if stat.Size() == 0 {
		// Write header for new WAL
		if err := w.writeHeader(); err != nil {
			return err
		}
		w.initialized = true
		return nil
	}
	
	// Read header from existing WAL
	if err := w.readHeader(); err != nil {
		return err
	}
	
	// Recover sequence number
	if err := w.recoverSequence(); err != nil {
		return err
	}
	
	w.initialized = true
	return nil
}

// writeHeader writes the WAL header
func (w *WAL) writeHeader() error {
	header := WALHeader{
		Version:  WALVersion,
		Sequence: w.sequence,
	}
	copy(header.Magic[:], WALMagic)
	
	if err := binary.Write(w.file, binary.LittleEndian, header); err != nil {
		return fmt.Errorf("failed to write WAL header: %w", err)
	}
	
	return nil
}

// readHeader reads the WAL header
func (w *WAL) readHeader() error {
	var header WALHeader
	if err := binary.Read(w.file, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("failed to read WAL header: %w", err)
	}
	
	// Validate magic number
	if string(header.Magic[:]) != WALMagic {
		return fmt.Errorf("invalid WAL magic number")
	}
	
	w.sequence = header.Sequence
	return nil
}

// recoverSequence reads through the WAL to find the highest sequence number
func (w *WAL) recoverSequence() error {
	// Seek to after header
	if _, err := w.file.Seek(int64(binary.Size(WALHeader{})), io.SeekStart); err != nil {
		return err
	}
	
	maxSeq := w.sequence
	
	for {
		entry, err := w.readEntry()
		if err != nil {
			if err == io.EOF {
				break
			}
			// If we hit a corrupted entry, stop here
			break
		}
		
		if entry.Sequence > maxSeq {
			maxSeq = entry.Sequence
		}
	}
	
	w.sequence = maxSeq
	return nil
}

// WriteEntry writes an entry to the WAL
func (w *WAL) WriteEntry(entryType WALEntryType, index string, docID string, doc *types.Document) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if !w.initialized {
		if err := w.Open(); err != nil {
			return err
		}
	}
	
	// Increment sequence
	w.sequence++
	
	entry := WALEntry{
		Type:      entryType,
		Index:     index,
		DocID:     docID,
		Document:  doc,
		Timestamp: time.Now().UnixNano(),
		Sequence:  w.sequence,
	}
	
	// Serialize entry
	entryBytes, err := w.serializeEntry(&entry)
	if err != nil {
		return fmt.Errorf("failed to serialize WAL entry: %w", err)
	}
	
	// Write entry length
	entryLen := uint32(len(entryBytes))
	if err := binary.Write(w.file, binary.LittleEndian, entryLen); err != nil {
		return fmt.Errorf("failed to write entry length: %w", err)
	}
	
	// Write entry data
	if _, err := w.file.Write(entryBytes); err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}
	
	// Sync to disk for durability
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}
	
	// Update header with new sequence
	if err := w.updateHeader(); err != nil {
		return err
	}
	
	return nil
}

// serializeEntry serializes a WAL entry
func (w *WAL) serializeEntry(entry *WALEntry) ([]byte, error) {
	// Format: [type:uint8][seq:uint64][ts:int64][indexLen:uint16][index:bytes][docIDLen:uint16][docID:bytes][docLen:uint32][doc:json]
	
	indexBytes := []byte(entry.Index)
	docIDBytes := []byte(entry.DocID)
	
	var docBytes []byte
	var err error
	if entry.Document != nil {
		docBytes, err = json.Marshal(entry.Document)
		if err != nil {
			return nil, err
		}
	}
	
	// Calculate total size
	totalSize := 1 + // type
		8 + // sequence
		8 + // timestamp
		2 + len(indexBytes) + // index
		2 + len(docIDBytes) + // docID
		4 + len(docBytes) // document
	
	result := make([]byte, 0, totalSize)
	
	// Write type
	result = append(result, byte(entry.Type))
	
	// Write sequence
	seqBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(seqBytes, entry.Sequence)
	result = append(result, seqBytes...)
	
	// Write timestamp
	tsBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(tsBytes, uint64(entry.Timestamp))
	result = append(result, tsBytes...)
	
	// Write index
	indexLen := uint16(len(indexBytes))
	indexLenBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(indexLenBytes, indexLen)
	result = append(result, indexLenBytes...)
	result = append(result, indexBytes...)
	
	// Write docID
	docIDLen := uint16(len(docIDBytes))
	docIDLenBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(docIDLenBytes, docIDLen)
	result = append(result, docIDLenBytes...)
	result = append(result, docIDBytes...)
	
	// Write document
	docLen := uint32(len(docBytes))
	docLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(docLenBytes, docLen)
	result = append(result, docLenBytes...)
	result = append(result, docBytes...)
	
	return result, nil
}

// readEntry reads a single entry from the WAL
func (w *WAL) readEntry() (*WALEntry, error) {
	// Read entry length
	var entryLen uint32
	if err := binary.Read(w.file, binary.LittleEndian, &entryLen); err != nil {
		return nil, err
	}
	
	// Read entry data
	entryBytes := make([]byte, entryLen)
	if _, err := io.ReadFull(w.file, entryBytes); err != nil {
		return nil, err
	}
	
	// Deserialize entry
	entry, err := w.deserializeEntry(entryBytes)
	if err != nil {
		return nil, err
	}
	
	return entry, nil
}

// deserializeEntry deserializes a WAL entry
func (w *WAL) deserializeEntry(data []byte) (*WALEntry, error) {
	entry := &WALEntry{}
	offset := 0
	
	// Read type
	entry.Type = WALEntryType(data[offset])
	offset++
	
	// Read sequence
	entry.Sequence = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	
	// Read timestamp
	entry.Timestamp = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	
	// Read index
	indexLen := binary.LittleEndian.Uint16(data[offset:])
	offset += 2
	entry.Index = string(data[offset : offset+int(indexLen)])
	offset += int(indexLen)
	
	// Read docID
	docIDLen := binary.LittleEndian.Uint16(data[offset:])
	offset += 2
	entry.DocID = string(data[offset : offset+int(docIDLen)])
	offset += int(docIDLen)
	
	// Read document
	docLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	if docLen > 0 {
		var doc types.Document
		if err := json.Unmarshal(data[offset:offset+int(docLen)], &doc); err != nil {
			return nil, err
		}
		entry.Document = &doc
	}
	
	return entry, nil
}

// updateHeader updates the WAL header
func (w *WAL) updateHeader() error {
	currentPos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	
	// Seek to beginning
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	
	// Write updated header
	if err := w.writeHeader(); err != nil {
		return err
	}
	
	// Restore position
	if _, err := w.file.Seek(currentPos, io.SeekStart); err != nil {
		return err
	}
	
	return nil
}

// Replay replays all entries from the WAL, calling the provided function for each entry
func (w *WAL) Replay(fn func(*WALEntry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if !w.initialized {
		if err := w.Open(); err != nil {
			return err
		}
	}
	
	// Seek to after header
	if _, err := w.file.Seek(int64(binary.Size(WALHeader{})), io.SeekStart); err != nil {
		return err
	}
	
	for {
		entry, err := w.readEntry()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		
		if err := fn(entry); err != nil {
			return err
		}
	}
	
	return nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
		w.file = nil
	}
	
	w.initialized = false
	return nil
}

// Flush forces a sync to disk
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.file != nil {
		return w.file.Sync()
	}
	
	return nil
}

