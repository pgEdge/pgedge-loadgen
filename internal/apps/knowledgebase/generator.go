package knowledgebase

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/datagen"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen/embeddings"
	"github.com/pgEdge/pgedge-loadgen/internal/logging"
)

// Table sizes for size calculation
var tableSizes = []datagen.TableSizeInfo{
	{Name: "category", BaseRowSize: 200, ScaleRatio: 20, IndexFactor: 1.1},
	{Name: "tag", BaseRowSize: 100, ScaleRatio: 50, IndexFactor: 1.1},
	{Name: "kb_user", BaseRowSize: 200, ScaleRatio: 200, IndexFactor: 1.2},
	{Name: "article", BaseRowSize: 3000, ScaleRatio: 500, IndexFactor: 1.5}, // Includes embedding
	{Name: "article_section", BaseRowSize: 2000, ScaleRatio: 2000, IndexFactor: 1.5},
	{Name: "article_tag", BaseRowSize: 20, ScaleRatio: 1500, IndexFactor: 1.1},
	{Name: "search_log", BaseRowSize: 2000, ScaleRatio: 1000, IndexFactor: 1.5},
	{Name: "feedback", BaseRowSize: 150, ScaleRatio: 500, IndexFactor: 1.2},
	{Name: "related_article", BaseRowSize: 30, ScaleRatio: 2000, IndexFactor: 1.1},
}

// Generator generates knowledgebase test data.
type Generator struct {
	faker      *datagen.Faker
	cfg        datagen.BatchInsertConfig
	embedder   embeddings.Embedder
	dimensions int
}

// NewGenerator creates a new data generator.
func NewGenerator(embedder embeddings.Embedder, dimensions int) *Generator {
	return &Generator{
		faker:      datagen.NewFaker(),
		cfg:        datagen.DefaultBatchConfig(),
		embedder:   embedder,
		dimensions: dimensions,
	}
}

// GenerateData generates test data for the target size.
func (g *Generator) GenerateData(ctx context.Context, pool *pgxpool.Pool, targetSize int64) error {
	// Adjust table sizes based on embedding dimensions
	adjustedSizes := make([]datagen.TableSizeInfo, len(tableSizes))
	copy(adjustedSizes, tableSizes)
	for i := range adjustedSizes {
		if adjustedSizes[i].Name == "article" || adjustedSizes[i].Name == "article_section" ||
			adjustedSizes[i].Name == "search_log" {
			adjustedSizes[i].BaseRowSize += int64(g.dimensions * 4)
		}
	}

	calc := datagen.NewSizeCalculator(adjustedSizes)
	rowCounts := calc.CalculateRowCounts(targetSize)

	scaleFactor := max(1, int(rowCounts["article"]/500))

	logging.Info().
		Int("scale_factor", scaleFactor).
		Int("dimensions", g.dimensions).
		Str("estimated_size", datagen.FormatSize(calc.EstimatedSize(rowCounts))).
		Msg("Generating knowledge base data")

	numCategories := scaleFactor * 20
	numTags := scaleFactor * 50
	numUsers := scaleFactor * 200
	numArticles := scaleFactor * 500
	numSearches := scaleFactor * 1000
	numFeedback := scaleFactor * 500

	if err := g.generateCategories(ctx, pool, numCategories); err != nil {
		return fmt.Errorf("generating categories: %w", err)
	}

	if err := g.generateTags(ctx, pool, numTags); err != nil {
		return fmt.Errorf("generating tags: %w", err)
	}

	if err := g.generateUsers(ctx, pool, numUsers); err != nil {
		return fmt.Errorf("generating users: %w", err)
	}

	if err := g.generateArticles(ctx, pool, numArticles, numCategories, numUsers); err != nil {
		return fmt.Errorf("generating articles: %w", err)
	}

	if err := g.generateArticleSections(ctx, pool, numArticles); err != nil {
		return fmt.Errorf("generating article sections: %w", err)
	}

	if err := g.generateArticleTags(ctx, pool, numArticles, numTags); err != nil {
		return fmt.Errorf("generating article tags: %w", err)
	}

	if err := g.generateSearchLogs(ctx, pool, numSearches, numUsers, numArticles); err != nil {
		return fmt.Errorf("generating search logs: %w", err)
	}

	if err := g.generateFeedback(ctx, pool, numFeedback, numArticles, numUsers); err != nil {
		return fmt.Errorf("generating feedback: %w", err)
	}

	if err := g.generateRelatedArticles(ctx, pool, numArticles); err != nil {
		return fmt.Errorf("generating related articles: %w", err)
	}

	return nil
}

func (g *Generator) generateCategories(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating categories")

	categoryNames := []string{
		"Getting Started", "Account Management", "Billing & Payments",
		"Technical Support", "Product Features", "Integrations",
		"Security & Privacy", "API Documentation", "Troubleshooting",
		"Best Practices", "Release Notes", "FAQs", "Tutorials",
		"Mobile Apps", "Desktop Apps", "Web Application",
		"Data Management", "Reports & Analytics", "User Guides",
		"Developer Resources",
	}

	batch := make([]string, 0, g.cfg.BatchSize)
	for i := 1; i <= count; i++ {
		name := categoryNames[(i-1)%len(categoryNames)]
		if i > len(categoryNames) {
			name = fmt.Sprintf("%s %d", name, i/len(categoryNames))
		}
		slug := slugify(name)
		description := g.faker.Sentence(10)

		parentID := "NULL"
		if i > 5 && g.faker.Float64(0, 1) < 0.3 {
			parentID = fmt.Sprintf("%d", g.faker.Int(1, min(i-1, 5)))
		}

		batch = append(batch, fmt.Sprintf("('%s', '%s', '%s', %s)",
			escapeSingleQuote(name),
			escapeSingleQuote(slug),
			escapeSingleQuote(description),
			parentID))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "category", "(name, slug, description, parent_id)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "category", "(name, slug, description, parent_id)", batch)
	}
	return nil
}

func (g *Generator) generateTags(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating tags")

	tagNames := []string{
		"how-to", "setup", "configuration", "error", "bug",
		"feature-request", "billing", "account", "security", "api",
		"integration", "performance", "mobile", "desktop", "web",
		"database", "authentication", "authorization", "export", "import",
		"backup", "restore", "upgrade", "migration", "deployment",
		"monitoring", "alerts", "notifications", "email", "sms",
		"webhook", "oauth", "sso", "two-factor", "password",
		"user-management", "team", "organization", "permissions", "roles",
		"dashboard", "reports", "analytics", "charts", "metrics",
		"automation", "workflow", "templates", "customization", "themes",
	}

	batch := make([]string, 0, g.cfg.BatchSize)
	for i := 1; i <= count; i++ {
		name := tagNames[(i-1)%len(tagNames)]
		if i > len(tagNames) {
			name = fmt.Sprintf("%s-%d", name, i/len(tagNames))
		}
		slug := slugify(name)

		batch = append(batch, fmt.Sprintf("('%s', '%s')",
			escapeSingleQuote(name),
			escapeSingleQuote(slug)))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "tag", "(name, slug)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "tag", "(name, slug)", batch)
	}
	return nil
}

func (g *Generator) generateUsers(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating users")

	roles := []string{"customer", "agent", "admin"}
	roleWeights := []int{70, 25, 5}

	batch := make([]string, 0, g.cfg.BatchSize)
	for i := 1; i <= count; i++ {
		firstName := g.faker.FirstName()
		lastName := g.faker.LastName()
		email := fmt.Sprintf("%s.%s%d@example.com",
			slugify(firstName), slugify(lastName), i)
		username := fmt.Sprintf("%s%s%d",
			slugify(firstName)[:min(3, len(firstName))],
			slugify(lastName)[:min(3, len(lastName))], i)
		role := datagen.ChooseWeighted(g.faker, roles, roleWeights)
		isActive := g.faker.Float64(0, 1) > 0.05

		batch = append(batch, fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', %t)",
			escapeSingleQuote(email),
			escapeSingleQuote(username),
			escapeSingleQuote(role),
			escapeSingleQuote(firstName),
			escapeSingleQuote(lastName),
			isActive))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "kb_user",
				"(email, username, role, first_name, last_name, is_active)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "kb_user",
			"(email, username, role, first_name, last_name, is_active)", batch)
	}
	return nil
}

func (g *Generator) generateArticles(ctx context.Context, pool *pgxpool.Pool, count, numCategories, numUsers int) error {
	logging.Info().Int("count", count).Msg("Generating articles")

	statuses := []string{"draft", "published", "archived"}
	statusWeights := []int{10, 85, 5}

	titlePrefixes := []string{
		"How to", "Guide to", "Understanding", "Troubleshooting",
		"Getting Started with", "Best Practices for", "FAQ:",
		"Tips for", "Introduction to", "Advanced",
	}

	topics := []string{
		"User Authentication", "Data Export", "API Integration",
		"Account Settings", "Billing Management", "Security Features",
		"Performance Optimization", "Mobile App Setup", "Team Management",
		"Report Generation", "Dashboard Customization", "Webhook Configuration",
		"SSO Setup", "Two-Factor Authentication", "Password Policies",
		"Data Backup", "System Migration", "Notification Settings",
		"Role Permissions", "Audit Logging",
	}

	batch := make([]string, 0, g.cfg.BatchSize/10)
	progress := datagen.NewProgressReporter("article", int64(count), g.cfg.ProgressInterval)

	for i := 1; i <= count; i++ {
		prefix := datagen.Choose(g.faker, titlePrefixes)
		topic := datagen.Choose(g.faker, topics)
		title := fmt.Sprintf("%s %s", prefix, topic)
		if i > len(titlePrefixes)*len(topics) {
			title = fmt.Sprintf("%s (%d)", title, i)
		}
		slug := fmt.Sprintf("%s-%d", slugify(title), i)
		summary := g.faker.Sentence(20)
		content := g.faker.Paragraph(3, 5, 15, "\n\n")
		status := datagen.ChooseWeighted(g.faker, statuses, statusWeights)

		embeddingText := title + " " + summary + " " + content
		embedding := g.embedder.Embed(embeddingText)
		embeddingStr := formatEmbedding(embedding)

		batch = append(batch, fmt.Sprintf("('%s', '%s', '%s', '%s', %d, %d, '%s', %d, %d, %d, '%s')",
			escapeSingleQuote(title),
			escapeSingleQuote(slug),
			escapeSingleQuote(summary),
			escapeSingleQuote(content),
			g.faker.Int(1, numCategories),
			g.faker.Int(1, numUsers),
			status,
			g.faker.Int(0, 10000),
			g.faker.Int(0, 500),
			g.faker.Int(0, 50),
			embeddingStr))

		if len(batch) >= g.cfg.BatchSize/10 {
			if err := g.executeBatchInsert(ctx, pool, "article",
				"(title, slug, summary, content, category_id, author_id, status, view_count, helpful_count, unhelpful_count, embedding)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "article",
			"(title, slug, summary, content, category_id, author_id, status, view_count, helpful_count, unhelpful_count, embedding)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateArticleSections(ctx context.Context, pool *pgxpool.Pool, numArticles int) error {
	logging.Info().Msg("Generating article sections")

	sectionTitles := []string{
		"Overview", "Prerequisites", "Step-by-Step Guide",
		"Common Issues", "Troubleshooting", "FAQ",
		"Related Topics", "Next Steps", "Additional Resources",
	}

	batch := make([]string, 0, g.cfg.BatchSize/10)

	for articleID := 1; articleID <= numArticles; articleID++ {
		numSections := g.faker.Int(2, 6)
		for order := 1; order <= numSections; order++ {
			title := sectionTitles[(order-1)%len(sectionTitles)]
			content := g.faker.Paragraph(2, 4, 12, "\n\n")
			embedding := g.embedder.Embed(title + " " + content)
			embeddingStr := formatEmbedding(embedding)

			batch = append(batch, fmt.Sprintf("(%d, '%s', '%s', %d, '%s')",
				articleID,
				escapeSingleQuote(title),
				escapeSingleQuote(content),
				order,
				embeddingStr))

			if len(batch) >= g.cfg.BatchSize/10 {
				if err := g.executeBatchInsert(ctx, pool, "article_section",
					"(article_id, title, content, section_order, embedding)", batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "article_section",
			"(article_id, title, content, section_order, embedding)", batch)
	}
	return nil
}

func (g *Generator) generateArticleTags(ctx context.Context, pool *pgxpool.Pool, numArticles, numTags int) error {
	logging.Info().Msg("Generating article-tag relationships")

	batch := make([]string, 0, g.cfg.BatchSize)
	usedPairs := make(map[string]bool)

	for articleID := 1; articleID <= numArticles; articleID++ {
		numTagsForArticle := g.faker.Int(1, 5)
		for j := 0; j < numTagsForArticle; j++ {
			tagID := g.faker.Int(1, numTags)
			key := fmt.Sprintf("%d-%d", articleID, tagID)
			if usedPairs[key] {
				continue
			}
			usedPairs[key] = true

			batch = append(batch, fmt.Sprintf("(%d, %d)", articleID, tagID))

			if len(batch) >= g.cfg.BatchSize {
				if err := g.executeBatchInsert(ctx, pool, "article_tag", "(article_id, tag_id)", batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "article_tag", "(article_id, tag_id)", batch)
	}
	return nil
}

func (g *Generator) generateSearchLogs(ctx context.Context, pool *pgxpool.Pool, count, numUsers, numArticles int) error {
	logging.Info().Int("count", count).Msg("Generating search logs")

	searchQueries := []string{
		"how to reset password",
		"export data to csv",
		"api rate limits",
		"billing questions",
		"two factor authentication setup",
		"team member permissions",
		"dashboard not loading",
		"integration with slack",
		"webhook configuration",
		"account deletion",
		"upgrade subscription",
		"mobile app sync issues",
		"report generation",
		"sso configuration",
		"data backup options",
		"notification settings",
		"api authentication",
		"custom domain setup",
		"performance issues",
		"audit log access",
	}

	batch := make([]string, 0, g.cfg.BatchSize/10)

	for i := 0; i < count; i++ {
		query := datagen.Choose(g.faker, searchQueries)
		if g.faker.Float64(0, 1) < 0.3 {
			query = query + " " + g.faker.Word()
		}

		embedding := g.embedder.Embed(query)
		embeddingStr := formatEmbedding(embedding)

		userID := "NULL"
		if g.faker.Float64(0, 1) > 0.2 {
			userID = fmt.Sprintf("%d", g.faker.Int(1, numUsers))
		}

		clickedArticle := "NULL"
		resultsCount := g.faker.Int(0, 20)
		if resultsCount > 0 && g.faker.Float64(0, 1) > 0.3 {
			clickedArticle = fmt.Sprintf("%d", g.faker.Int(1, numArticles))
		}

		sessionID := fmt.Sprintf("sess_%s", g.faker.UUID()[:8])

		batch = append(batch, fmt.Sprintf("(%s, '%s', %d, %s, '%s', '%s')",
			userID,
			escapeSingleQuote(query),
			resultsCount,
			clickedArticle,
			sessionID,
			embeddingStr))

		if len(batch) >= g.cfg.BatchSize/10 {
			if err := g.executeBatchInsert(ctx, pool, "search_log",
				"(user_id, query_text, results_count, clicked_article, session_id, embedding)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "search_log",
			"(user_id, query_text, results_count, clicked_article, session_id, embedding)", batch)
	}
	return nil
}

func (g *Generator) generateFeedback(ctx context.Context, pool *pgxpool.Pool, count, numArticles, numUsers int) error {
	logging.Info().Int("count", count).Msg("Generating feedback")

	comments := []string{
		"Very helpful, thanks!",
		"This solved my problem",
		"Could use more detail",
		"Outdated information",
		"Clear and concise",
		"Screenshots would help",
		"Exactly what I needed",
		"Confusing instructions",
	}

	batch := make([]string, 0, g.cfg.BatchSize)

	for i := 0; i < count; i++ {
		userID := "NULL"
		if g.faker.Float64(0, 1) > 0.3 {
			userID = fmt.Sprintf("%d", g.faker.Int(1, numUsers))
		}

		comment := "NULL"
		if g.faker.Float64(0, 1) > 0.5 {
			c := datagen.Choose(g.faker, comments)
			comment = fmt.Sprintf("'%s'", escapeSingleQuote(c))
		}

		isHelpful := g.faker.Float64(0, 1) > 0.25
		sessionID := fmt.Sprintf("sess_%s", g.faker.UUID()[:8])

		batch = append(batch, fmt.Sprintf("(%d, %s, %t, %s, '%s')",
			g.faker.Int(1, numArticles),
			userID,
			isHelpful,
			comment,
			sessionID))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "feedback",
				"(article_id, user_id, is_helpful, comment, session_id)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "feedback",
			"(article_id, user_id, is_helpful, comment, session_id)", batch)
	}
	return nil
}

func (g *Generator) generateRelatedArticles(ctx context.Context, pool *pgxpool.Pool, numArticles int) error {
	logging.Info().Msg("Generating related articles")

	batch := make([]string, 0, g.cfg.BatchSize)
	usedPairs := make(map[string]bool)

	for articleID := 1; articleID <= numArticles; articleID++ {
		numRelated := g.faker.Int(2, 5)
		for j := 0; j < numRelated; j++ {
			relatedID := g.faker.Int(1, numArticles)
			if relatedID == articleID {
				continue
			}
			key := fmt.Sprintf("%d-%d", articleID, relatedID)
			if usedPairs[key] {
				continue
			}
			usedPairs[key] = true

			similarity := 0.5 + g.faker.Float64(0, 0.49)

			batch = append(batch, fmt.Sprintf("(%d, %d, %.4f)",
				articleID, relatedID, similarity))

			if len(batch) >= g.cfg.BatchSize {
				if err := g.executeBatchInsert(ctx, pool, "related_article",
					"(article_id, related_id, similarity)", batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "related_article",
			"(article_id, related_id, similarity)", batch)
	}
	return nil
}

func (g *Generator) executeBatchInsert(ctx context.Context, pool *pgxpool.Pool, table, columns string, values []string) error {
	if len(values) == 0 {
		return nil
	}
	sql := fmt.Sprintf("INSERT INTO %s %s VALUES %s", table, columns, strings.Join(values, ", "))
	_, err := pool.Exec(ctx, sql)
	return err
}

func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func slugify(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, " ", "-")
	s = strings.ReplaceAll(s, "'", "")
	s = strings.ReplaceAll(s, "\"", "")
	s = strings.ReplaceAll(s, "&", "and")
	return s
}

func formatEmbedding(embedding []float32) string {
	parts := make([]string, len(embedding))
	for i, v := range embedding {
		parts[i] = fmt.Sprintf("%.6f", v)
	}
	return "[" + strings.Join(parts, ",") + "]"
}
