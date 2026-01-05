//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Portions copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

package knowledgebase

import (
	"context"
	"time"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen/embeddings"
)

// Query weights for knowledge base workload
var queryWeights = map[string]int{
	"semantic_search":   40,
	"similar_questions": 20,
	"browse_category":   15,
	"view_article":      10,
	"submit_feedback":   10,
	"admin_update":      5,
}

// QueryExecutor executes knowledgebase queries.
type QueryExecutor struct {
	faker         *datagen.Faker
	embedder      embeddings.Embedder
	numArticles   int
	numUsers      int
	numCategories int
	numSearches   int
}

// NewQueryExecutor creates a new query executor.
func NewQueryExecutor(embedder embeddings.Embedder, numArticles, numUsers, numCategories, numSearches int) *QueryExecutor {
	return &QueryExecutor{
		faker:         datagen.NewFaker(),
		embedder:      embedder,
		numArticles:   max(1, numArticles),
		numUsers:      max(1, numUsers),
		numCategories: max(1, numCategories),
		numSearches:   max(1, numSearches),
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
	case "similar_questions":
		rowsAffected, err = e.executeSimilarQuestions(ctx, db)
	case "browse_category":
		rowsAffected, err = e.executeBrowseCategory(ctx, db)
	case "view_article":
		rowsAffected, err = e.executeViewArticle(ctx, db)
	case "submit_feedback":
		rowsAffected, err = e.executeSubmitFeedback(ctx, db)
	case "admin_update":
		rowsAffected, err = e.executeAdminUpdate(ctx, db)
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

// Semantic Search - Vector similarity search for articles
func (e *QueryExecutor) executeSemanticSearch(ctx context.Context, db apps.DB) (int64, error) {
	searchQueries := []string{
		"how to reset my password",
		"export data to csv format",
		"api authentication setup",
		"billing payment methods",
		"enable two factor authentication",
		"add team member permissions",
		"dashboard not loading properly",
		"integrate with slack notifications",
		"configure webhook endpoints",
		"delete my account permanently",
		"upgrade subscription plan",
		"mobile app synchronization",
		"generate custom reports",
		"setup single sign on",
		"backup my data",
		"change notification settings",
		"troubleshoot connection issues",
		"customize dashboard layout",
		"audit log access permissions",
		"migrate data between accounts",
	}

	query := datagen.Choose(e.faker, searchQueries)
	queryEmbedding := e.embedder.Embed(query)

	// Log the search
	var userID interface{}
	if e.faker.Float64(0, 1) > 0.2 {
		userID = e.faker.Int(1, e.numUsers)
	}

	rows, err := db.Query(ctx, `
        WITH search_results AS (
            SELECT a.id, a.title, a.summary, c.name AS category,
                   1 - (a.embedding <=> $1::vector) AS similarity
            FROM article a
            JOIN category c ON a.category_id = c.id
            WHERE a.status = 'published'
            ORDER BY a.embedding <=> $1::vector
            LIMIT 10
        )
        SELECT * FROM search_results
    `, formatEmbedding(queryEmbedding))
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		count++
	}

	// Log the search (fire and forget)
	_, _ = db.Exec(ctx, `
        INSERT INTO search_log (user_id, query_text, results_count, session_id, embedding)
        VALUES ($1, $2, $3, $4, $5::vector)
    `, userID, query, count, "sess_"+e.faker.UUID()[:8], formatEmbedding(queryEmbedding))

	return count, rows.Err()
}

// Similar Questions - Find articles matching previous search queries
func (e *QueryExecutor) executeSimilarQuestions(ctx context.Context, db apps.DB) (int64, error) {
	// Get a random search log entry and find similar searches
	searchID := e.faker.Int(1, e.numSearches)

	rows, err := db.Query(ctx, `
        SELECT s2.id, s2.query_text,
               1 - (s2.embedding <=> s1.embedding) AS similarity,
               s2.clicked_article
        FROM search_log s1, search_log s2
        WHERE s1.id = $1
            AND s2.id != s1.id
            AND s2.results_count > 0
        ORDER BY s2.embedding <=> s1.embedding
        LIMIT 5
    `, searchID)
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

// Browse Category - Traditional category-based browsing
func (e *QueryExecutor) executeBrowseCategory(ctx context.Context, db apps.DB) (int64, error) {
	categoryID := e.faker.Int(1, e.numCategories)

	rows, err := db.Query(ctx, `
        SELECT a.id, a.title, a.summary, a.view_count, a.helpful_count,
               u.username AS author,
               array_agg(t.name) AS tags
        FROM article a
        LEFT JOIN kb_user u ON a.author_id = u.id
        LEFT JOIN article_tag at ON a.id = at.article_id
        LEFT JOIN tag t ON at.tag_id = t.id
        WHERE a.category_id = $1 AND a.status = 'published'
        GROUP BY a.id, a.title, a.summary, a.view_count, a.helpful_count, u.username
        ORDER BY a.helpful_count DESC, a.view_count DESC
        LIMIT 20
    `, categoryID)
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

// View Article - Read full article content with sections
func (e *QueryExecutor) executeViewArticle(ctx context.Context, db apps.DB) (int64, error) {
	articleID := e.faker.Int(1, e.numArticles)

	// Update view count
	_, _ = db.Exec(ctx, `
        UPDATE article SET view_count = view_count + 1 WHERE id = $1
    `, articleID)

	// Fetch article with sections
	rows, err := db.Query(ctx, `
        SELECT a.id, a.title, a.content, a.summary, c.name AS category,
               u.username AS author, a.published_at,
               s.title AS section_title, s.content AS section_content, s.section_order
        FROM article a
        JOIN category c ON a.category_id = c.id
        LEFT JOIN kb_user u ON a.author_id = u.id
        LEFT JOIN article_section s ON a.id = s.article_id
        WHERE a.id = $1
        ORDER BY s.section_order
    `, articleID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		count++
	}

	// Also fetch related articles
	relatedRows, err := db.Query(ctx, `
        SELECT r.related_id, a.title, a.summary, r.similarity
        FROM related_article r
        JOIN article a ON r.related_id = a.id
        WHERE r.article_id = $1 AND a.status = 'published'
        ORDER BY r.similarity DESC
        LIMIT 5
    `, articleID)
	if err == nil {
		defer relatedRows.Close()
		for relatedRows.Next() {
			count++
		}
	}

	return count, rows.Err()
}

// Submit Feedback - Rate article helpfulness
func (e *QueryExecutor) executeSubmitFeedback(ctx context.Context, db apps.DB) (int64, error) {
	articleID := e.faker.Int(1, e.numArticles)
	isHelpful := e.faker.Float64(0, 1) > 0.25 // 75% helpful

	var userID interface{}
	if e.faker.Float64(0, 1) > 0.3 {
		userID = e.faker.Int(1, e.numUsers)
	}

	comments := []string{
		"Very helpful, thanks!",
		"Solved my problem",
		"Could be more detailed",
		"Outdated information",
		"",
	}
	var comment interface{}
	if e.faker.Float64(0, 1) > 0.5 {
		c := datagen.Choose(e.faker, comments)
		if c != "" {
			comment = c
		}
	}

	_, err := db.Exec(ctx, `
        INSERT INTO feedback (article_id, user_id, is_helpful, comment, session_id)
        VALUES ($1, $2, $3, $4, $5)
    `, articleID, userID, isHelpful, comment, "sess_"+e.faker.UUID()[:8])
	if err != nil {
		return 0, err
	}

	// Update article counts
	if isHelpful {
		_, _ = db.Exec(ctx, `
            UPDATE article SET helpful_count = helpful_count + 1 WHERE id = $1
        `, articleID)
	} else {
		_, _ = db.Exec(ctx, `
            UPDATE article SET unhelpful_count = unhelpful_count + 1 WHERE id = $1
        `, articleID)
	}

	return 1, nil
}

// Admin Update - Update article content
func (e *QueryExecutor) executeAdminUpdate(ctx context.Context, db apps.DB) (int64, error) {
	articleID := e.faker.Int(1, e.numArticles)

	// Simulate different types of updates
	updateType := e.faker.Int(1, 3)

	switch updateType {
	case 1:
		// Update content and regenerate embedding
		newContent := e.faker.Paragraph(3, 4, 12, "\n\n")
		embedding := e.embedder.Embed(newContent)
		_, err := db.Exec(ctx, `
            UPDATE article
            SET content = $1, embedding = $2::vector, updated_at = NOW()
            WHERE id = $3
        `, newContent, formatEmbedding(embedding), articleID)
		return 1, err

	case 2:
		// Update status
		statuses := []string{"draft", "published", "archived"}
		newStatus := datagen.Choose(e.faker, statuses)
		_, err := db.Exec(ctx, `
            UPDATE article SET status = $1, updated_at = NOW() WHERE id = $2
        `, newStatus, articleID)
		return 1, err

	case 3:
		// Add a new section
		titles := []string{"Update", "Additional Information", "Note", "Appendix"}
		content := e.faker.Paragraph(1, 3, 10, "\n\n")
		embedding := e.embedder.Embed(content)
		_, err := db.Exec(ctx, `
            INSERT INTO article_section (article_id, title, content, section_order, embedding)
            SELECT $1, $2, $3, COALESCE(MAX(section_order), 0) + 1, $4::vector
            FROM article_section WHERE article_id = $1
        `, articleID, datagen.Choose(e.faker, titles), content, formatEmbedding(embedding))
		return 1, err
	}

	return 0, nil
}
