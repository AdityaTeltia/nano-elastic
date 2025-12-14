package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"nano-elastic/internal/analyzer"
	"nano-elastic/internal/index/inverted"
	"nano-elastic/internal/storage"
	"nano-elastic/internal/types"
)

func main() {
	fmt.Println("=== Nano-Elastic Phase 2 Demo: Inverted Index ===")
	fmt.Println()

	// Create a temporary directory for the demo
	demoDir := filepath.Join(os.TempDir(), "nano-elastic-demo-phase2")
	os.RemoveAll(demoDir)
	os.MkdirAll(demoDir, 0755)
	defer os.RemoveAll(demoDir)

	fmt.Printf("Using storage directory: %s\n", demoDir)
	fmt.Println()

	// Create schema
	fmt.Println("1. Creating index schema...")
	schema := types.NewSchema("demo-index")
	schema.AddField("title", types.FieldTypeText, types.WithAnalyzed(true))
	schema.AddField("description", types.FieldTypeText, types.WithAnalyzed(true))
	schema.AddField("author", types.FieldTypeKeyword)
	fmt.Printf("   ✓ Schema created with %d fields\n", len(schema.Fields))
	fmt.Println()

	// Create index manager for document storage
	fmt.Println("2. Creating index manager...")
	indexManager, err := storage.NewIndexManager("demo-index", demoDir, schema)
	if err != nil {
		log.Fatalf("Failed to create index manager: %v", err)
	}
	defer indexManager.Close()
	fmt.Println("   ✓ Index manager created")
	fmt.Println()

	// Create inverted index
	fmt.Println("3. Creating inverted index...")
	invertedIndex := inverted.NewInvertedIndex()
	fmt.Println("   ✓ Inverted index created")
	fmt.Println()

	// Index documents
	fmt.Println("4. Indexing documents with inverted index...")
	documents := []struct {
		id          string
		title       string
		description string
		author      string
	}{
		{"1", "The Great Gatsby", "A classic American novel about the Jazz Age and the American Dream", "F. Scott Fitzgerald"},
		{"2", "1984", "A dystopian social science fiction novel about totalitarian control", "George Orwell"},
		{"3", "To Kill a Mockingbird", "A novel about racial inequality and loss of innocence in the American South", "Harper Lee"},
		{"4", "Pride and Prejudice", "A romantic novel of manners about Elizabeth Bennet and Mr. Darcy", "Jane Austen"},
		{"5", "The Catcher in the Rye", "A controversial novel about teenage rebellion and alienation", "J.D. Salinger"},
	}

	startTime := time.Now()
	for _, doc := range documents {
		// Create document
		document := types.NewDocument(doc.id)
		document.SetField("title", types.TextValue{Value: doc.title})
		document.SetField("description", types.TextValue{Value: doc.description})
		document.SetField("author", types.KeywordValue{Value: doc.author})

		// Store document
		if err := indexManager.WriteDocument(document); err != nil {
			log.Fatalf("Failed to write document %s: %v", doc.id, err)
		}

		// Index text fields in inverted index
		invertedIndex.IndexDocument(doc.id, "title", doc.title)
		invertedIndex.IndexDocument(doc.id, "description", doc.description)

		fmt.Printf("   ✓ Indexed: %s\n", doc.title)
	}
	elapsed := time.Since(startTime)
	fmt.Printf("   ✓ Indexed %d documents in %v (%.2f docs/sec)\n", len(documents), elapsed, float64(len(documents))/elapsed.Seconds())
	fmt.Println()

	// Show index statistics
	fmt.Println("5. Index statistics...")
	totalTerms, totalDocs, uniqueTerms := invertedIndex.GetStats()
	fmt.Printf("   ✓ Total terms indexed: %d\n", totalTerms)
	fmt.Printf("   ✓ Unique terms: %d\n", uniqueTerms)
	fmt.Printf("   ✓ Documents indexed: %d\n", totalDocs)
	fmt.Println()

	// Demonstrate tokenization
	fmt.Println("6. Tokenization example...")
	tokenizer := analyzer.NewTokenizer()
	text := "The Great Gatsby is a classic novel"
	tokens := tokenizer.Tokenize(text)
	fmt.Printf("   Text: \"%s\"\n", text)
	fmt.Printf("   Tokens: %v\n", tokens)
	fmt.Println()

	// Demonstrate analyzer (with stop word removal)
	fmt.Println("7. Analysis example (with stop word removal)...")
	analyzer := analyzer.NewAnalyzer()
	analyzed := analyzer.Analyze(text)
	fmt.Printf("   Text: \"%s\"\n", text)
	fmt.Printf("   After analysis: %v\n", analyzed)
	fmt.Println()

	// Search examples
	fmt.Println("8. Search examples...")
	
	// Search for "novel"
	fmt.Println("   Searching for 'novel':")
	results := invertedIndex.Search("novel")
	if results != nil {
		fmt.Printf("   ✓ Found in %d documents:\n", results.DocFreq)
		for _, posting := range results.Postings {
			doc, _ := indexManager.ReadDocument(posting.DocID)
			fmt.Printf("      - %s (appears %d times)\n", doc.GetFieldAsText("title"), posting.TermFreq)
		}
	} else {
		fmt.Println("   ✗ Not found")
	}
	fmt.Println()

	// Search for "american"
	fmt.Println("   Searching for 'american':")
	results = invertedIndex.Search("american")
	if results != nil {
		fmt.Printf("   ✓ Found in %d documents:\n", results.DocFreq)
		for _, posting := range results.Postings {
			doc, _ := indexManager.ReadDocument(posting.DocID)
			fmt.Printf("      - %s\n", doc.GetFieldAsText("title"))
		}
	} else {
		fmt.Println("   ✗ Not found")
	}
	fmt.Println()

	// Multi-term search (AND query)
	fmt.Println("   Searching for 'novel' AND 'classic':")
	results = invertedIndex.SearchMultipleTerms([]string{"novel", "classic"})
	if results != nil && results.DocFreq > 0 {
		fmt.Printf("   ✓ Found in %d documents:\n", results.DocFreq)
		for _, posting := range results.Postings {
			doc, _ := indexManager.ReadDocument(posting.DocID)
			fmt.Printf("      - %s\n", doc.GetFieldAsText("title"))
		}
	} else {
		fmt.Println("   ✗ Not found")
	}
	fmt.Println()

	// Field-specific search
	fmt.Println("   Searching for 'gatsby' in 'title' field:")
	results = invertedIndex.SearchInField("title", "gatsby")
	if results != nil {
		fmt.Printf("   ✓ Found in %d documents:\n", results.DocFreq)
		for _, posting := range results.Postings {
			doc, _ := indexManager.ReadDocument(posting.DocID)
			fmt.Printf("      - %s\n", doc.GetFieldAsText("title"))
		}
	} else {
		fmt.Println("   ✗ Not found")
	}
	fmt.Println()

	// Performance summary
	fmt.Println("=== Performance Summary ===")
	fmt.Printf("Documents indexed: %d\n", totalDocs)
	fmt.Printf("Unique terms: %d\n", uniqueTerms)
	fmt.Printf("Total term occurrences: %d\n", totalTerms)
	fmt.Printf("Indexing speed: %.2f docs/sec\n", float64(len(documents))/elapsed.Seconds())
	fmt.Println()

	fmt.Println("=== Phase 2 Complete! ===")
	fmt.Println("✓ Inverted index working")
	fmt.Println("✓ Tokenization working")
	fmt.Println("✓ Text analysis working")
	fmt.Println("✓ Search functionality working")
	fmt.Println()
	fmt.Println("Next: Phase 3 - BM25 Scoring Algorithm")
}

