package inverted

import (
	"sync"

	"nano-elastic/internal/analyzer"
)

// InvertedIndex is the main inverted index structure
// It maps terms (words) to posting lists (documents containing those terms)
type InvertedIndex struct {
	// termDict maps term -> PostingList
	// In Go, maps are reference types and are not thread-safe
	termDict map[string]*PostingList
	
	// Mutex for thread-safe operations
	// sync.RWMutex allows multiple readers or one writer
	mu sync.RWMutex
	
	// Analyzer for processing text
	analyzer *analyzer.Analyzer
	
	// Statistics
	totalTerms int // Total number of terms indexed
	totalDocs  int // Total number of documents indexed
}

// NewInvertedIndex creates a new inverted index
func NewInvertedIndex() *InvertedIndex {
	return &InvertedIndex{
		termDict: make(map[string]*PostingList),
		analyzer: analyzer.NewAnalyzer(),
	}
}

// NewInvertedIndexWithAnalyzer creates an index with a custom analyzer
func NewInvertedIndexWithAnalyzer(analyzer *analyzer.Analyzer) *InvertedIndex {
	return &InvertedIndex{
		termDict: make(map[string]*PostingList),
		analyzer: analyzer,
	}
}

// IndexDocument indexes a document's text field
// docID: unique document identifier
// fieldName: name of the text field
// text: the text content to index
func (idx *InvertedIndex) IndexDocument(docID string, fieldName string, text string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	
	// Analyze the text to get tokens with positions
	tokens, positions := idx.analyzer.AnalyzeWithPositions(text)
	
	// Index each token
	for i, token := range tokens {
		// Create a unique term key: "fieldName:token"
		// This allows same word in different fields to be separate
		termKey := fieldName + ":" + token
		
		// Get or create posting list for this term
		postingList, exists := idx.termDict[termKey]
		if !exists {
			postingList = NewPostingList()
			idx.termDict[termKey] = postingList
		}
		
		// Add posting with position
		postingList.AddPosting(docID, positions[i])
		idx.totalTerms++
	}
	
	idx.totalDocs++
}

// Search finds documents containing a term
// Returns a posting list for the term, or nil if not found
func (idx *InvertedIndex) Search(term string) *PostingList {
	idx.mu.RLock() // Read lock (allows multiple concurrent readers)
	defer idx.mu.RUnlock()
	
	// Analyze the search term (normalize it)
	tokens := idx.analyzer.Analyze(term)
	if len(tokens) == 0 {
		return nil
	}
	
	// For now, search in all fields (we can improve this later)
	// Try to find the term in any field
	for fieldName := range idx.getAllFieldNames() {
		termKey := fieldName + ":" + tokens[0]
		if postingList, exists := idx.termDict[termKey]; exists {
			return postingList
		}
	}
	
	return nil
}

// SearchInField searches for a term in a specific field
func (idx *InvertedIndex) SearchInField(fieldName string, term string) *PostingList {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	
	// Analyze the search term
	tokens := idx.analyzer.Analyze(term)
	if len(tokens) == 0 {
		return nil
	}
	
	termKey := fieldName + ":" + tokens[0]
	postingList, exists := idx.termDict[termKey]
	if !exists {
		return nil
	}
	
	return postingList
}

// SearchMultipleTerms finds documents containing all terms (AND query)
// Returns intersection of all posting lists
func (idx *InvertedIndex) SearchMultipleTerms(terms []string) *PostingList {
	if len(terms) == 0 {
		return nil
	}
	
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	
	// Analyze all terms
	var postingLists []*PostingList
	for _, term := range terms {
		tokens := idx.analyzer.Analyze(term)
		if len(tokens) == 0 {
			continue
		}
		
		// Try to find in any field
		found := false
		for fieldName := range idx.getAllFieldNames() {
			termKey := fieldName + ":" + tokens[0]
			if pl, exists := idx.termDict[termKey]; exists {
				postingLists = append(postingLists, pl)
				found = true
				break
			}
		}
		
		if !found {
			// Term not found, AND query returns empty
			return NewPostingList()
		}
	}
	
	if len(postingLists) == 0 {
		return NewPostingList()
	}
	
	// Intersect all posting lists
	return idx.intersectPostingLists(postingLists)
}

// intersectPostingLists finds documents that appear in ALL posting lists
// This implements AND query logic
func (idx *InvertedIndex) intersectPostingLists(lists []*PostingList) *PostingList {
	if len(lists) == 0 {
		return NewPostingList()
	}
	
	if len(lists) == 1 {
		return lists[0]
	}
	
	// Start with first list
	result := NewPostingList()
	
	// For each document in first list, check if it's in all other lists
	for _, posting := range lists[0].Postings {
		inAll := true
		
		// Check if this docID exists in all other lists
		for i := 1; i < len(lists); i++ {
			if _, exists := lists[i].GetPosting(posting.DocID); !exists {
				inAll = false
				break
			}
		}
		
		if inAll {
			// Document is in all lists, add to result
			result.Postings = append(result.Postings, posting)
			result.DocFreq++
		}
	}
	
	return result
}

// getAllFieldNames extracts all unique field names from term dictionary
// Helper function to get field names from "fieldName:term" keys
func (idx *InvertedIndex) getAllFieldNames() map[string]bool {
	fields := make(map[string]bool)
	for termKey := range idx.termDict {
		// Split "fieldName:term" to get field name
		// In Go, we can use strings.Split or strings.Index
		// For simplicity, we'll extract field name
		// This is a simplified version - in production, we'd track fields separately
		if idx := indexOf(termKey, ':'); idx > 0 {
			fieldName := termKey[:idx]
			fields[fieldName] = true
		}
	}
	return fields
}

// indexOf finds the first occurrence of a character in a string
// Go doesn't have a built-in indexOf for characters, so we implement it
func indexOf(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// GetStats returns index statistics
func (idx *InvertedIndex) GetStats() (totalTerms int, totalDocs int, uniqueTerms int) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	
	return idx.totalTerms, idx.totalDocs, len(idx.termDict)
}

// Clear removes all indexed data
func (idx *InvertedIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	
	idx.termDict = make(map[string]*PostingList)
	idx.totalTerms = 0
	idx.totalDocs = 0
}

