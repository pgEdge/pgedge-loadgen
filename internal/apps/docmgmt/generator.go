package docmgmt

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
	{Name: "doc_user", BaseRowSize: 250, ScaleRatio: 100, IndexFactor: 1.2},
	{Name: "folder", BaseRowSize: 200, ScaleRatio: 200, IndexFactor: 1.2},
	{Name: "document", BaseRowSize: 3000, ScaleRatio: 1000, IndexFactor: 1.5}, // Includes embedding
	{Name: "document_version", BaseRowSize: 2500, ScaleRatio: 3000, IndexFactor: 1.5},
	{Name: "document_chunk", BaseRowSize: 2000, ScaleRatio: 5000, IndexFactor: 1.5},
	{Name: "doc_tag", BaseRowSize: 50, ScaleRatio: 50, IndexFactor: 1.1},
	{Name: "document_tag", BaseRowSize: 20, ScaleRatio: 3000, IndexFactor: 1.1},
	{Name: "permission", BaseRowSize: 100, ScaleRatio: 2000, IndexFactor: 1.2},
	{Name: "audit_log", BaseRowSize: 300, ScaleRatio: 5000, IndexFactor: 1.3},
	{Name: "share_link", BaseRowSize: 150, ScaleRatio: 200, IndexFactor: 1.2},
}

// Generator generates docmgmt test data.
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
		if adjustedSizes[i].Name == "document" || adjustedSizes[i].Name == "document_version" ||
			adjustedSizes[i].Name == "document_chunk" {
			adjustedSizes[i].BaseRowSize += int64(g.dimensions * 4)
		}
	}

	calc := datagen.NewSizeCalculator(adjustedSizes)
	rowCounts := calc.CalculateRowCounts(targetSize)

	scaleFactor := max(1, int(rowCounts["document"]/1000))

	logging.Info().
		Int("scale_factor", scaleFactor).
		Int("dimensions", g.dimensions).
		Str("estimated_size", datagen.FormatSize(calc.EstimatedSize(rowCounts))).
		Msg("Generating document management data")

	numUsers := scaleFactor * 100
	numFolders := scaleFactor * 200
	numDocuments := scaleFactor * 1000
	numTags := scaleFactor * 50
	numAuditLogs := scaleFactor * 5000

	if err := g.generateUsers(ctx, pool, numUsers); err != nil {
		return fmt.Errorf("generating users: %w", err)
	}

	if err := g.generateFolders(ctx, pool, numFolders, numUsers); err != nil {
		return fmt.Errorf("generating folders: %w", err)
	}

	if err := g.generateTags(ctx, pool, numTags); err != nil {
		return fmt.Errorf("generating tags: %w", err)
	}

	if err := g.generateDocuments(ctx, pool, numDocuments, numFolders, numUsers); err != nil {
		return fmt.Errorf("generating documents: %w", err)
	}

	if err := g.generateDocumentVersions(ctx, pool, numDocuments, numUsers); err != nil {
		return fmt.Errorf("generating document versions: %w", err)
	}

	if err := g.generateDocumentChunks(ctx, pool, numDocuments); err != nil {
		return fmt.Errorf("generating document chunks: %w", err)
	}

	if err := g.generateDocumentTags(ctx, pool, numDocuments, numTags); err != nil {
		return fmt.Errorf("generating document tags: %w", err)
	}

	if err := g.generatePermissions(ctx, pool, numDocuments, numFolders, numUsers); err != nil {
		return fmt.Errorf("generating permissions: %w", err)
	}

	if err := g.generateAuditLogs(ctx, pool, numAuditLogs, numDocuments, numFolders, numUsers); err != nil {
		return fmt.Errorf("generating audit logs: %w", err)
	}

	if err := g.generateShareLinks(ctx, pool, numDocuments, numFolders, numUsers); err != nil {
		return fmt.Errorf("generating share links: %w", err)
	}

	return nil
}

func (g *Generator) generateUsers(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating users")

	departments := []string{
		"Engineering", "Marketing", "Sales", "Finance", "HR",
		"Legal", "Operations", "Product", "Design", "Support",
	}
	roles := []string{"user", "editor", "admin"}
	roleWeights := []int{70, 25, 5}

	batch := make([]string, 0, g.cfg.BatchSize)
	for i := 1; i <= count; i++ {
		firstName := g.faker.FirstName()
		lastName := g.faker.LastName()
		email := fmt.Sprintf("%s.%s%d@company.com",
			slugify(firstName), slugify(lastName), i)
		username := fmt.Sprintf("%s%s%d",
			slugify(firstName)[:min(3, len(firstName))],
			slugify(lastName)[:min(3, len(lastName))], i)
		role := datagen.ChooseWeighted(g.faker, roles, roleWeights)
		isActive := g.faker.Float64(0, 1) > 0.05

		batch = append(batch, fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', %t)",
			escapeSingleQuote(email),
			escapeSingleQuote(username),
			escapeSingleQuote(firstName+" "+lastName),
			escapeSingleQuote(role),
			escapeSingleQuote(datagen.Choose(g.faker, departments)),
			isActive))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "doc_user",
				"(email, username, full_name, role, department, is_active)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "doc_user",
			"(email, username, full_name, role, department, is_active)", batch)
	}
	return nil
}

func (g *Generator) generateFolders(ctx context.Context, pool *pgxpool.Pool, count, numUsers int) error {
	logging.Info().Int("count", count).Msg("Generating folders")

	folderNames := []string{
		"Documents", "Projects", "Reports", "Contracts", "Invoices",
		"Marketing Materials", "HR Documents", "Technical Specs",
		"Meeting Notes", "Presentations", "Templates", "Archives",
		"Shared", "Private", "Drafts", "Final", "Review",
		"Q1", "Q2", "Q3", "Q4", "2024", "2025",
	}

	batch := make([]string, 0, g.cfg.BatchSize)
	for i := 1; i <= count; i++ {
		name := folderNames[(i-1)%len(folderNames)]
		if i > len(folderNames) {
			name = fmt.Sprintf("%s %d", name, i/len(folderNames))
		}

		parentID := "NULL"
		path := "/" + slugify(name)
		if i > 10 && g.faker.Float64(0, 1) < 0.6 {
			pid := g.faker.Int(1, min(i-1, 10))
			parentID = fmt.Sprintf("%d", pid)
			path = fmt.Sprintf("/folder-%d%s", pid, path)
		}

		batch = append(batch, fmt.Sprintf("('%s', %s, %d, '%s')",
			escapeSingleQuote(name),
			parentID,
			g.faker.Int(1, numUsers),
			escapeSingleQuote(path)))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "folder",
				"(name, parent_id, owner_id, path)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "folder",
			"(name, parent_id, owner_id, path)", batch)
	}
	return nil
}

func (g *Generator) generateTags(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating tags")

	tagNames := []string{
		"important", "urgent", "review-needed", "approved", "draft",
		"final", "confidential", "public", "internal", "archived",
		"template", "contract", "invoice", "report", "presentation",
		"meeting", "project", "legal", "hr", "finance",
		"marketing", "sales", "engineering", "product", "design",
		"q1", "q2", "q3", "q4", "2024", "2025",
		"client", "vendor", "partner", "compliance", "policy",
	}

	colors := []string{
		"#FF5733", "#33FF57", "#3357FF", "#FF33F5", "#F5FF33",
		"#33FFF5", "#808080", "#FF8033", "#8033FF", "#33FF80",
	}

	batch := make([]string, 0, g.cfg.BatchSize)
	for i := 1; i <= count; i++ {
		name := tagNames[(i-1)%len(tagNames)]
		if i > len(tagNames) {
			name = fmt.Sprintf("%s-%d", name, i/len(tagNames))
		}
		color := colors[(i-1)%len(colors)]

		batch = append(batch, fmt.Sprintf("('%s', '%s')",
			escapeSingleQuote(name), color))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "doc_tag", "(name, color)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "doc_tag", "(name, color)", batch)
	}
	return nil
}

func (g *Generator) generateDocuments(ctx context.Context, pool *pgxpool.Pool, count, numFolders, numUsers int) error {
	logging.Info().Int("count", count).Msg("Generating documents")

	fileTypes := []string{"pdf", "docx", "xlsx", "pptx", "txt", "md", "csv", "json"}
	fileTypeWeights := []int{30, 25, 15, 10, 8, 5, 4, 3}

	mimeTypes := map[string]string{
		"pdf":  "application/pdf",
		"docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		"pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
		"txt":  "text/plain",
		"md":   "text/markdown",
		"csv":  "text/csv",
		"json": "application/json",
	}

	docTitles := []string{
		"Annual Report", "Project Proposal", "Meeting Notes", "Contract Agreement",
		"Technical Specification", "User Guide", "Policy Document", "Budget Plan",
		"Marketing Strategy", "Sales Report", "Employee Handbook", "Training Manual",
		"Product Roadmap", "Risk Assessment", "Compliance Report", "Security Audit",
		"Performance Review", "Quarterly Update", "Invoice", "Purchase Order",
	}

	statuses := []string{"active", "archived", "deleted"}
	statusWeights := []int{85, 12, 3}

	batch := make([]string, 0, g.cfg.BatchSize/10)
	progress := datagen.NewProgressReporter("document", int64(count), g.cfg.ProgressInterval)

	for i := 1; i <= count; i++ {
		title := docTitles[(i-1)%len(docTitles)]
		if i > len(docTitles) {
			title = fmt.Sprintf("%s %d", title, i/len(docTitles))
		}

		fileType := datagen.ChooseWeighted(g.faker, fileTypes, fileTypeWeights)
		description := g.faker.Sentence(15)
		content := title + " " + description + " " + g.faker.Paragraph(2, 4, 12, "\n\n")
		embedding := g.embedder.Embed(content)
		embeddingStr := formatEmbedding(embedding)

		batch = append(batch, fmt.Sprintf("('%s', '%s', '%s', %d, '%s', %d, %d, '%s', %d, '%s', '%s')",
			escapeSingleQuote(title),
			escapeSingleQuote(description),
			fileType,
			g.faker.Int(1024, 50*1024*1024),
			mimeTypes[fileType],
			g.faker.Int(1, numFolders),
			g.faker.Int(1, numUsers),
			datagen.ChooseWeighted(g.faker, statuses, statusWeights),
			g.faker.Int(1, 10),
			g.faker.UUID(),
			embeddingStr))

		if len(batch) >= g.cfg.BatchSize/10 {
			if err := g.executeBatchInsert(ctx, pool, "document",
				"(title, description, file_type, file_size, mime_type, folder_id, owner_id, status, version, checksum, embedding)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "document",
			"(title, description, file_type, file_size, mime_type, folder_id, owner_id, status, version, checksum, embedding)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateDocumentVersions(ctx context.Context, pool *pgxpool.Pool, numDocuments, numUsers int) error {
	logging.Info().Msg("Generating document versions")

	changeSummaries := []string{
		"Initial version",
		"Updated content",
		"Fixed formatting",
		"Added new section",
		"Revised based on feedback",
		"Minor corrections",
		"Major revision",
		"Final review changes",
		"Updated figures",
		"Grammar fixes",
	}

	batch := make([]string, 0, g.cfg.BatchSize/10)

	for docID := 1; docID <= numDocuments; docID++ {
		numVersions := g.faker.Int(1, 5)
		for v := 1; v <= numVersions; v++ {
			changeSummary := changeSummaries[(v-1)%len(changeSummaries)]
			content := changeSummary + " " + g.faker.Sentence(10)
			embedding := g.embedder.Embed(content)
			embeddingStr := formatEmbedding(embedding)

			batch = append(batch, fmt.Sprintf("(%d, %d, %d, '%s', '%s', %d, '%s')",
				docID, v,
				g.faker.Int(1024, 50*1024*1024),
				g.faker.UUID(),
				escapeSingleQuote(changeSummary),
				g.faker.Int(1, numUsers),
				embeddingStr))

			if len(batch) >= g.cfg.BatchSize/10 {
				if err := g.executeBatchInsert(ctx, pool, "document_version",
					"(document_id, version_number, file_size, checksum, change_summary, created_by, embedding)", batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "document_version",
			"(document_id, version_number, file_size, checksum, change_summary, created_by, embedding)", batch)
	}
	return nil
}

func (g *Generator) generateDocumentChunks(ctx context.Context, pool *pgxpool.Pool, numDocuments int) error {
	logging.Info().Msg("Generating document chunks")

	batch := make([]string, 0, g.cfg.BatchSize/10)

	for docID := 1; docID <= numDocuments; docID++ {
		// Only some documents have chunks (larger docs)
		if g.faker.Float64(0, 1) > 0.3 {
			continue
		}

		numChunks := g.faker.Int(3, 15)
		for idx := 0; idx < numChunks; idx++ {
			content := g.faker.Paragraph(2, 4, 12, "\n\n")
			embedding := g.embedder.Embed(content)
			embeddingStr := formatEmbedding(embedding)

			batch = append(batch, fmt.Sprintf("(%d, %d, '%s', %d, %d, '%s')",
				docID, idx,
				escapeSingleQuote(content),
				idx+1, idx+1,
				embeddingStr))

			if len(batch) >= g.cfg.BatchSize/10 {
				if err := g.executeBatchInsert(ctx, pool, "document_chunk",
					"(document_id, chunk_index, content, start_page, end_page, embedding)", batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "document_chunk",
			"(document_id, chunk_index, content, start_page, end_page, embedding)", batch)
	}
	return nil
}

func (g *Generator) generateDocumentTags(ctx context.Context, pool *pgxpool.Pool, numDocuments, numTags int) error {
	logging.Info().Msg("Generating document-tag relationships")

	batch := make([]string, 0, g.cfg.BatchSize)
	usedPairs := make(map[string]bool)

	for docID := 1; docID <= numDocuments; docID++ {
		numTagsForDoc := g.faker.Int(1, 5)
		for j := 0; j < numTagsForDoc; j++ {
			tagID := g.faker.Int(1, numTags)
			key := fmt.Sprintf("%d-%d", docID, tagID)
			if usedPairs[key] {
				continue
			}
			usedPairs[key] = true

			batch = append(batch, fmt.Sprintf("(%d, %d)", docID, tagID))

			if len(batch) >= g.cfg.BatchSize {
				if err := g.executeBatchInsert(ctx, pool, "document_tag", "(document_id, tag_id)", batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "document_tag", "(document_id, tag_id)", batch)
	}
	return nil
}

func (g *Generator) generatePermissions(ctx context.Context, pool *pgxpool.Pool, numDocuments, numFolders, numUsers int) error {
	logging.Info().Msg("Generating permissions")

	permTypes := []string{"view", "edit", "admin", "download"}

	batch := make([]string, 0, g.cfg.BatchSize)

	// Document permissions
	for i := 0; i < numDocuments/2; i++ {
		docID := g.faker.Int(1, numDocuments)
		userID := g.faker.Int(1, numUsers)
		grantedBy := g.faker.Int(1, numUsers)

		batch = append(batch, fmt.Sprintf("(%d, NULL, %d, '%s', %d)",
			docID, userID, datagen.Choose(g.faker, permTypes), grantedBy))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "permission",
				"(document_id, folder_id, user_id, permission_type, granted_by)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	// Folder permissions
	for i := 0; i < numFolders/2; i++ {
		folderID := g.faker.Int(1, numFolders)
		userID := g.faker.Int(1, numUsers)
		grantedBy := g.faker.Int(1, numUsers)

		batch = append(batch, fmt.Sprintf("(NULL, %d, %d, '%s', %d)",
			folderID, userID, datagen.Choose(g.faker, permTypes), grantedBy))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "permission",
				"(document_id, folder_id, user_id, permission_type, granted_by)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "permission",
			"(document_id, folder_id, user_id, permission_type, granted_by)", batch)
	}
	return nil
}

func (g *Generator) generateAuditLogs(ctx context.Context, pool *pgxpool.Pool, count, numDocuments, numFolders, numUsers int) error {
	logging.Info().Int("count", count).Msg("Generating audit logs")

	actions := []string{
		"view", "download", "upload", "edit", "delete", "restore",
		"share", "unshare", "move", "copy", "rename", "permission_change",
	}
	actionWeights := []int{30, 15, 10, 15, 5, 2, 8, 3, 5, 3, 2, 2}

	batch := make([]string, 0, g.cfg.BatchSize)

	for i := 0; i < count; i++ {
		action := datagen.ChooseWeighted(g.faker, actions, actionWeights)

		docID := "NULL"
		folderID := "NULL"
		if g.faker.Float64(0, 1) > 0.3 {
			docID = fmt.Sprintf("%d", g.faker.Int(1, numDocuments))
		} else {
			folderID = fmt.Sprintf("%d", g.faker.Int(1, numFolders))
		}

		details := fmt.Sprintf(`{"action": "%s", "ref": "%s"}`,
			action, g.faker.UUID()[:8])
		ipAddr := fmt.Sprintf("192.168.%d.%d", g.faker.Int(1, 255), g.faker.Int(1, 255))

		batch = append(batch, fmt.Sprintf("(%d, %s, %s, '%s', '%s'::jsonb, '%s')",
			g.faker.Int(1, numUsers), docID, folderID, action, details, ipAddr))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "audit_log",
				"(user_id, document_id, folder_id, action, details, ip_address)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "audit_log",
			"(user_id, document_id, folder_id, action, details, ip_address)", batch)
	}
	return nil
}

func (g *Generator) generateShareLinks(ctx context.Context, pool *pgxpool.Pool, numDocuments, numFolders, numUsers int) error {
	logging.Info().Msg("Generating share links")

	accessTypes := []string{"view", "download", "edit"}

	batch := make([]string, 0, g.cfg.BatchSize)

	// Document share links
	numDocLinks := numDocuments / 10
	for i := 0; i < numDocLinks; i++ {
		batch = append(batch, fmt.Sprintf("(%d, NULL, '%s', %d, '%s', %d)",
			g.faker.Int(1, numDocuments),
			g.faker.UUID(),
			g.faker.Int(1, numUsers),
			datagen.Choose(g.faker, accessTypes),
			g.faker.Int(1, 100)))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "share_link",
				"(document_id, folder_id, token, created_by, access_type, max_downloads)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	// Folder share links
	numFolderLinks := numFolders / 10
	for i := 0; i < numFolderLinks; i++ {
		batch = append(batch, fmt.Sprintf("(NULL, %d, '%s', %d, '%s', NULL)",
			g.faker.Int(1, numFolders),
			g.faker.UUID(),
			g.faker.Int(1, numUsers),
			datagen.Choose(g.faker, accessTypes)))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "share_link",
				"(document_id, folder_id, token, created_by, access_type, max_downloads)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		return g.executeBatchInsert(ctx, pool, "share_link",
			"(document_id, folder_id, token, created_by, access_type, max_downloads)", batch)
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
