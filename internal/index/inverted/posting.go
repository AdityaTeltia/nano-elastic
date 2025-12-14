package inverted

// Posting represents a single entry in a posting list
// A posting list contains all documents that contain a specific term
type Posting struct {
	DocID     string  // Document ID
	TermFreq  int     // Term frequency (how many times term appears in document)
	Positions []int   // Positions where term appears (for phrase queries)
}

// PostingList represents a list of postings for a term
// This is the core data structure of an inverted index
type PostingList struct {
	Postings []Posting // All documents containing this term
	DocFreq  int       // Document frequency (how many documents contain this term)
}

// NewPostingList creates a new empty posting list
func NewPostingList() *PostingList {
	return &PostingList{
		Postings: make([]Posting, 0),
		DocFreq:  0,
	}
}

// AddPosting adds a posting to the list
// If document already exists, it updates the term frequency
func (pl *PostingList) AddPosting(docID string, position int) {
	// Check if document already exists in posting list
	for i := range pl.Postings {
		if pl.Postings[i].DocID == docID {
			// Document already exists, update it
			pl.Postings[i].TermFreq++
			pl.Postings[i].Positions = append(pl.Postings[i].Positions, position)
			return
		}
	}
	
	// New document, add it
	pl.Postings = append(pl.Postings, Posting{
		DocID:     docID,
		TermFreq:  1,
		Positions: []int{position},
	})
	pl.DocFreq++
}

// GetPosting finds a posting for a specific document ID
// Returns the posting and true if found, nil and false otherwise
func (pl *PostingList) GetPosting(docID string) (*Posting, bool) {
	for i := range pl.Postings {
		if pl.Postings[i].DocID == docID {
			return &pl.Postings[i], true
		}
	}
	return nil, false
}

// GetDocIDs returns all document IDs in this posting list
func (pl *PostingList) GetDocIDs() []string {
	docIDs := make([]string, len(pl.Postings))
	for i, posting := range pl.Postings {
		docIDs[i] = posting.DocID
	}
	return docIDs
}

// Size returns the number of postings in the list
func (pl *PostingList) Size() int {
	return len(pl.Postings)
}

