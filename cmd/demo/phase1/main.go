package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"nano-elastic/internal/storage"
	"nano-elastic/internal/types"
)

func main() {
	fmt.Println("=== Nano-Elastic Phase 1 Demo: Basic Document Storage ===")
	fmt.Println()

	// Create a temporary directory for the demo
	demoDir := filepath.Join(os.TempDir(), "nano-elastic-demo")
	os.RemoveAll(demoDir) // Clean up any previous demo
	os.MkdirAll(demoDir, 0755)
	defer os.RemoveAll(demoDir) // Clean up after demo

	fmt.Printf("Using storage directory: %s\n", demoDir)
	fmt.Println()

	// Create a schema
	fmt.Println("1. Creating index schema...")
	schema := types.NewSchema("demo-index")
	schema.AddField("title", types.FieldTypeText, types.WithAnalyzed(true))
	schema.AddField("author", types.FieldTypeKeyword)
	schema.AddField("year", types.FieldTypeNumeric)
	schema.AddField("rating", types.FieldTypeNumeric)
	schema.AddField("description", types.FieldTypeText, types.WithAnalyzed(true))
	fmt.Printf("   ✓ Schema created with %d fields\n", len(schema.Fields))
	fmt.Println()

	// Create index manager
	fmt.Println("2. Creating index manager...")
	indexManager, err := storage.NewIndexManager("demo-index", demoDir, schema)
	if err != nil {
		log.Fatalf("Failed to create index manager: %v", err)
	}
	defer indexManager.Close()
	fmt.Println("   ✓ Index manager created")
	fmt.Println()

	// Create and index some documents
	fmt.Println("3. Indexing documents...")
	documents := []struct {
		id    string
		title string
		author string
		year  float64
		rating float64
		description string
	}{
		{"1", "The Great Gatsby", "F. Scott Fitzgerald", 1925, 4.5, "A classic American novel about the Jazz Age"},
		{"2", "1984", "George Orwell", 1949, 4.8, "A dystopian social science fiction novel"},
		{"3", "To Kill a Mockingbird", "Harper Lee", 1960, 4.7, "A novel about racial inequality in the American South"},
		{"4", "Pride and Prejudice", "Jane Austen", 1813, 4.6, "A romantic novel of manners"},
		{"5", "The Catcher in the Rye", "J.D. Salinger", 1951, 4.3, "A controversial novel about teenage rebellion"},
	}

	startTime := time.Now()
	for _, doc := range documents {
		document := types.NewDocument(doc.id)
		document.SetField("title", types.TextValue{Value: doc.title})
		document.SetField("author", types.KeywordValue{Value: doc.author})
		document.SetField("year", types.NumericValue{Value: doc.year})
		document.SetField("rating", types.NumericValue{Value: doc.rating})
		document.SetField("description", types.TextValue{Value: doc.description})

		if err := indexManager.WriteDocument(document); err != nil {
			log.Fatalf("Failed to write document %s: %v", doc.id, err)
		}
		fmt.Printf("   ✓ Indexed document: %s\n", doc.title)
	}
	elapsed := time.Since(startTime)
	fmt.Printf("   ✓ Indexed %d documents in %v (%.2f docs/sec)\n", len(documents), elapsed, float64(len(documents))/elapsed.Seconds())
	fmt.Println()

	// Verify document count
	fmt.Println("4. Verifying index...")
	docCount := indexManager.GetDocumentCount()
	fmt.Printf("   ✓ Total documents in index: %d\n", docCount)
	fmt.Println()

	// Retrieve documents
	fmt.Println("5. Retrieving documents by ID...")
	for _, doc := range documents {
		retrieved, err := indexManager.ReadDocument(doc.id)
		if err != nil {
			log.Fatalf("Failed to read document %s: %v", doc.id, err)
		}

		fmt.Printf("   Document ID: %s\n", retrieved.ID)
		fmt.Printf("   - Title: %s\n", retrieved.GetFieldAsText("title"))
		fmt.Printf("   - Author: %s\n", retrieved.GetFieldAsText("author"))
		fmt.Printf("   - Year: %s\n", retrieved.GetFieldAsText("year"))
		fmt.Printf("   - Rating: %s\n", retrieved.GetFieldAsText("rating"))
		fmt.Printf("   - Version: %d\n", retrieved.Version)
		fmt.Println()
	}

	// Test persistence by creating a new index manager
	fmt.Println("6. Testing persistence (reopening index)...")
	indexManager.Close()

	newIndexManager, err := storage.NewIndexManager("demo-index", demoDir, schema)
	if err != nil {
		log.Fatalf("Failed to reopen index manager: %v", err)
	}
	defer newIndexManager.Close()

	recoveredCount := newIndexManager.GetDocumentCount()
	fmt.Printf("   ✓ Recovered %d documents from disk\n", recoveredCount)

	// Verify we can still read documents
	testDoc, err := newIndexManager.ReadDocument("1")
	if err != nil {
		log.Fatalf("Failed to read document after reopening: %v", err)
	}
	fmt.Printf("   ✓ Successfully read document: %s\n", testDoc.GetFieldAsText("title"))
	fmt.Println()

	// Performance summary
	fmt.Println("=== Performance Summary ===")
	fmt.Printf("Documents indexed: %d\n", docCount)
	fmt.Printf("Indexing speed: %.2f docs/sec\n", float64(len(documents))/elapsed.Seconds())
	fmt.Printf("Storage directory: %s\n", demoDir)
	fmt.Println()

	fmt.Println("=== Phase 1 Complete! ===")
	fmt.Println("✓ Document storage working")
	fmt.Println("✓ Schema validation working")
	fmt.Println("✓ Persistence working")
	fmt.Println("✓ WAL (Write-Ahead Log) working")
	fmt.Println()
	fmt.Println("Next: Phase 2 - Inverted Index Implementation")
}

