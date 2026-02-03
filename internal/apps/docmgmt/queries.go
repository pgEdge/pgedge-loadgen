//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

package docmgmt

import (
	"context"
	"fmt"
	"time"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen/embeddings"
)

// Query weights for document management workload
var queryWeights = map[string]int{
	"semantic_search":   35,
	"find_similar":      15,
	"browse_folder":     15,
	"document_retrieve": 15,
	"version_history":   5,
	"upload_update":     10,
	"permission_check":  5,
}

// QueryExecutor executes docmgmt queries.
type QueryExecutor struct {
	faker        *datagen.Faker
	embedder     embeddings.Embedder
	numDocuments int
	numUsers     int
	numFolders   int
	numChunks    int
}

// NewQueryExecutor creates a new query executor.
func NewQueryExecutor(embedder embeddings.Embedder, numDocuments, numUsers, numFolders, numChunks int) *QueryExecutor {
	return &QueryExecutor{
		faker:        datagen.NewFaker(),
		embedder:     embedder,
		numDocuments: max(1, numDocuments),
		numUsers:     max(1, numUsers),
		numFolders:   max(1, numFolders),
		numChunks:    max(1, numChunks),
	}
}

// ExecuteRandomQuery executes a random query based on weights.
func (e *QueryExecutor) ExecuteRandomQuery(ctx context.Context, db apps.DB) apps.QueryResult {
	queryType := e.selectQueryType()

	start := time.Now()
	var err error
	var rowsAffected int64

	switch queryType {
	case "semantic_search":
		rowsAffected, err = e.executeSemanticSearch(ctx, db)
	case "find_similar":
		rowsAffected, err = e.executeFindSimilar(ctx, db)
	case "browse_folder":
		rowsAffected, err = e.executeBrowseFolder(ctx, db)
	case "document_retrieve":
		rowsAffected, err = e.executeDocumentRetrieve(ctx, db)
	case "version_history":
		rowsAffected, err = e.executeVersionHistory(ctx, db)
	case "upload_update":
		rowsAffected, err = e.executeUploadUpdate(ctx, db)
	case "permission_check":
		rowsAffected, err = e.executePermissionCheck(ctx, db)
	}

	return apps.QueryResult{
		QueryName:    queryType,
		Duration:     time.Since(start).Nanoseconds(),
		RowsAffected: rowsAffected,
		Error:        err,
	}
}

func (e *QueryExecutor) selectQueryType() string {
	types := make([]string, 0, len(queryWeights))
	weights := make([]int, 0, len(queryWeights))
	for k, v := range queryWeights {
		types = append(types, k)
		weights = append(weights, v)
	}
	return datagen.ChooseWeighted(e.faker, types, weights)
}

// Semantic Search - Vector similarity search for documents
func (e *QueryExecutor) executeSemanticSearch(ctx context.Context, db apps.DB) (int64, error) {
	searchQueries := []string{
		"quarterly financial report",
		"employee onboarding checklist",
		"project proposal template",
		"contract agreement terms",
		"marketing campaign strategy",
		"technical architecture document",
		"security compliance audit",
		"budget planning spreadsheet",
		"meeting notes from last week",
		"product roadmap presentation",
		"customer feedback analysis",
		"vendor agreement contract",
		"training materials for new hires",
		"risk assessment report",
		"performance review guidelines",
		"policy update announcement",
		"invoice for services rendered",
		"purchase order approval",
		"legal compliance documentation",
		"data privacy policy",
	}

	query := datagen.Choose(e.faker, searchQueries)
	queryEmbedding := e.embedder.Embed(query)
	userID := e.faker.Int(1, e.numUsers)

	// Search documents
	rows, err := db.Query(ctx, `
        SELECT d.id, d.title, d.description, d.file_type,
               f.name AS folder_name, u.full_name AS owner,
               1 - (d.embedding <=> $1::vector) AS similarity
        FROM document d
        JOIN folder f ON d.folder_id = f.id
        JOIN doc_user u ON d.owner_id = u.id
        WHERE d.status = 'active'
        ORDER BY d.embedding <=> $1::vector
        LIMIT 20
    `, formatEmbedding(queryEmbedding))
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		count++
	}

	// Also search document chunks for more granular results
	chunkRows, err := db.Query(ctx, `
        SELECT c.id, c.document_id, d.title,
               c.content, c.start_page, c.end_page,
               1 - (c.embedding <=> $1::vector) AS similarity
        FROM document_chunk c
        JOIN document d ON c.document_id = d.id
        WHERE d.status = 'active'
        ORDER BY c.embedding <=> $1::vector
        LIMIT 10
    `, formatEmbedding(queryEmbedding))
	if err == nil {
		defer chunkRows.Close()
		for chunkRows.Next() {
			count++
		}
	}

	// Log audit entry (fire and forget)
	_, _ = db.Exec(ctx, `
        INSERT INTO audit_log (user_id, action, details, ip_address)
        VALUES ($1, 'search', $2::jsonb, $3)
    `, userID, fmt.Sprintf(`{"query": "%s", "results": %d}`, query, count),
		fmt.Sprintf("192.168.%d.%d", e.faker.Int(1, 255), e.faker.Int(1, 255)))

	return count, rows.Err()
}

// Find Similar - Find documents similar to a given document
func (e *QueryExecutor) executeFindSimilar(ctx context.Context, db apps.DB) (int64, error) {
	documentID := e.faker.Int(1, e.numDocuments)

	rows, err := db.Query(ctx, `
        SELECT d2.id, d2.title, d2.file_type, d2.description,
               1 - (d2.embedding <=> d1.embedding) AS similarity
        FROM document d1, document d2
        WHERE d1.id = $1
            AND d2.id != d1.id
            AND d2.status = 'active'
        ORDER BY d2.embedding <=> d1.embedding
        LIMIT 10
    `, documentID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		count++
	}
	return count, rows.Err()
}

// Browse Folder - Traditional folder-based browsing
func (e *QueryExecutor) executeBrowseFolder(ctx context.Context, db apps.DB) (int64, error) {
	folderID := e.faker.Int(1, e.numFolders)

	// Get subfolders
	subfolderRows, err := db.Query(ctx, `
        SELECT f.id, f.name, f.path, u.full_name AS owner,
               COUNT(d.id) AS document_count
        FROM folder f
        LEFT JOIN doc_user u ON f.owner_id = u.id
        LEFT JOIN document d ON f.id = d.folder_id AND d.status = 'active'
        WHERE f.parent_id = $1
        GROUP BY f.id, f.name, f.path, u.full_name
        ORDER BY f.name
    `, folderID)
	if err != nil {
		return 0, err
	}
	defer subfolderRows.Close()

	var count int64
	for subfolderRows.Next() {
		count++
	}

	// Get documents in folder
	docRows, err := db.Query(ctx, `
        SELECT d.id, d.title, d.file_type, d.file_size, d.updated_at,
               u.full_name AS owner,
               array_agg(t.name) AS tags
        FROM document d
        LEFT JOIN doc_user u ON d.owner_id = u.id
        LEFT JOIN document_tag dt ON d.id = dt.document_id
        LEFT JOIN doc_tag t ON dt.tag_id = t.id
        WHERE d.folder_id = $1 AND d.status = 'active'
        GROUP BY d.id, d.title, d.file_type, d.file_size, d.updated_at, u.full_name
        ORDER BY d.updated_at DESC
        LIMIT 50
    `, folderID)
	if err != nil {
		return count, err
	}
	defer docRows.Close()

	for docRows.Next() {
		count++
	}
	return count, docRows.Err()
}

// Document Retrieve - Full document fetch with metadata
func (e *QueryExecutor) executeDocumentRetrieve(ctx context.Context, db apps.DB) (int64, error) {
	documentID := e.faker.Int(1, e.numDocuments)
	userID := e.faker.Int(1, e.numUsers)

	// Fetch document with metadata
	rows, err := db.Query(ctx, `
        SELECT d.id, d.title, d.description, d.file_type, d.file_size,
               d.mime_type, d.version, d.checksum, d.created_at, d.updated_at,
               f.name AS folder_name, f.path AS folder_path,
               u.full_name AS owner, u.email AS owner_email,
               array_agg(DISTINCT t.name) AS tags
        FROM document d
        JOIN folder f ON d.folder_id = f.id
        JOIN doc_user u ON d.owner_id = u.id
        LEFT JOIN document_tag dt ON d.id = dt.document_id
        LEFT JOIN doc_tag t ON dt.tag_id = t.id
        WHERE d.id = $1
        GROUP BY d.id, d.title, d.description, d.file_type, d.file_size,
                 d.mime_type, d.version, d.checksum, d.created_at, d.updated_at,
                 f.name, f.path, u.full_name, u.email
    `, documentID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		count++
	}

	// Log view action
	_, _ = db.Exec(ctx, `
        INSERT INTO audit_log (user_id, document_id, action, ip_address)
        VALUES ($1, $2, 'view', $3)
    `, userID, documentID,
		fmt.Sprintf("192.168.%d.%d", e.faker.Int(1, 255), e.faker.Int(1, 255)))

	return count, rows.Err()
}

// Version History - Get document version history
func (e *QueryExecutor) executeVersionHistory(ctx context.Context, db apps.DB) (int64, error) {
	documentID := e.faker.Int(1, e.numDocuments)

	rows, err := db.Query(ctx, `
        SELECT v.id, v.version_number, v.file_size, v.checksum,
               v.change_summary, v.created_at,
               u.full_name AS created_by
        FROM document_version v
        LEFT JOIN doc_user u ON v.created_by = u.id
        WHERE v.document_id = $1
        ORDER BY v.version_number DESC
    `, documentID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		count++
	}
	return count, rows.Err()
}

// Upload/Update - Upload new document or update existing
func (e *QueryExecutor) executeUploadUpdate(ctx context.Context, db apps.DB) (int64, error) {
	userID := e.faker.Int(1, e.numUsers)

	// 70% update existing, 30% new upload
	if e.faker.Float64(0, 1) < 0.7 {
		// Update existing document
		documentID := e.faker.Int(1, e.numDocuments)

		// Create new version
		changeSummary := datagen.Choose(e.faker, []string{
			"Updated content", "Fixed formatting", "Added section",
			"Revised based on feedback", "Minor corrections",
		})
		content := changeSummary + " " + e.faker.Sentence(10)
		embedding := e.embedder.Embed(content)

		// Insert version
		_, err := db.Exec(ctx, `
            INSERT INTO document_version (document_id, version_number, file_size,
                                         checksum, change_summary, created_by, embedding)
            SELECT $1, COALESCE(MAX(version_number), 0) + 1, $2, $3, $4, $5, $6::vector
            FROM document_version WHERE document_id = $1
        `, documentID,
			e.faker.Int(1024, 50*1024*1024),
			e.faker.UUID(),
			changeSummary,
			userID,
			formatEmbedding(embedding))
		if err != nil {
			return 0, err
		}

		// Update document
		newContent := e.faker.Paragraph(2, 4, 12, "\n\n")
		newEmbedding := e.embedder.Embed(newContent)
		_, err = db.Exec(ctx, `
            UPDATE document
            SET version = version + 1, updated_at = NOW(), embedding = $1::vector
            WHERE id = $2
        `, formatEmbedding(newEmbedding), documentID)
		if err != nil {
			return 1, err
		}

		// Log action
		_, _ = db.Exec(ctx, `
            INSERT INTO audit_log (user_id, document_id, action, ip_address)
            VALUES ($1, $2, 'edit', $3)
        `, userID, documentID,
			fmt.Sprintf("192.168.%d.%d", e.faker.Int(1, 255), e.faker.Int(1, 255)))

		return 1, nil
	}

	// New document upload
	title := datagen.Choose(e.faker, []string{
		"New Report", "Draft Document", "Updated Policy", "Meeting Notes",
		"Project Plan", "Analysis Results", "Review Document",
	}) + fmt.Sprintf(" %d", e.faker.Int(1, 1000))

	fileTypes := []string{"pdf", "docx", "xlsx", "pptx", "txt"}
	fileType := datagen.Choose(e.faker, fileTypes)

	content := title + " " + e.faker.Paragraph(2, 4, 12, "\n\n")
	embedding := e.embedder.Embed(content)

	var docID int
	err := db.QueryRow(ctx, `
        INSERT INTO document (title, description, file_type, file_size, folder_id,
                             owner_id, status, embedding)
        VALUES ($1, $2, $3, $4, $5, $6, 'active', $7::vector)
        RETURNING id
    `, title, e.faker.Sentence(10), fileType,
		e.faker.Int(1024, 50*1024*1024),
		e.faker.Int(1, e.numFolders),
		userID,
		formatEmbedding(embedding)).Scan(&docID)
	if err != nil {
		return 0, err
	}

	// Log action
	_, _ = db.Exec(ctx, `
        INSERT INTO audit_log (user_id, document_id, action, ip_address)
        VALUES ($1, $2, 'upload', $3)
    `, userID, docID,
		fmt.Sprintf("192.168.%d.%d", e.faker.Int(1, 255), e.faker.Int(1, 255)))

	return 1, nil
}

// Permission Check - Check user access to document/folder
func (e *QueryExecutor) executePermissionCheck(ctx context.Context, db apps.DB) (int64, error) {
	userID := e.faker.Int(1, e.numUsers)

	// Check document permission
	if e.faker.Float64(0, 1) > 0.5 {
		documentID := e.faker.Int(1, e.numDocuments)

		rows, err := db.Query(ctx, `
            SELECT p.permission_type, p.granted_at, p.expires_at,
                   g.full_name AS granted_by
            FROM permission p
            LEFT JOIN doc_user g ON p.granted_by = g.id
            WHERE p.document_id = $1 AND p.user_id = $2
                AND (p.expires_at IS NULL OR p.expires_at > NOW())
        `, documentID, userID)
		if err != nil {
			return 0, err
		}
		defer rows.Close()

		var count int64
		for rows.Next() {
			count++
		}
		return count, rows.Err()
	}

	// Check folder permission
	folderID := e.faker.Int(1, e.numFolders)

	rows, err := db.Query(ctx, `
        SELECT p.permission_type, p.granted_at, p.expires_at,
               g.full_name AS granted_by
        FROM permission p
        LEFT JOIN doc_user g ON p.granted_by = g.id
        WHERE p.folder_id = $1 AND p.user_id = $2
            AND (p.expires_at IS NULL OR p.expires_at > NOW())
    `, folderID, userID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		count++
	}
	return count, rows.Err()
}
