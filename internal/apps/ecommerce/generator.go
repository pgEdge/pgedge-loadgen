package ecommerce

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/datagen"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen/embeddings"
	"github.com/pgEdge/pgedge-loadgen/internal/logging"
)

// Table sizes for size calculation
var tableSizes = []datagen.TableSizeInfo{
	{Name: "category", BaseRowSize: 150, ScaleRatio: 100, IndexFactor: 1.1},
	{Name: "brand", BaseRowSize: 150, ScaleRatio: 50, IndexFactor: 1.1},
	{Name: "product", BaseRowSize: 2000, ScaleRatio: 10000, IndexFactor: 1.5}, // Includes embedding
	{Name: "inventory", BaseRowSize: 50, ScaleRatio: 20000, IndexFactor: 1.2}, // 2 per product
	{Name: "customer", BaseRowSize: 300, ScaleRatio: 50000, IndexFactor: 1.2},
	{Name: "cart", BaseRowSize: 50, ScaleRatio: 10000, IndexFactor: 1.2},
	{Name: "cart_item", BaseRowSize: 30, ScaleRatio: 30000, IndexFactor: 1.2},
	{Name: "orders", BaseRowSize: 200, ScaleRatio: 100000, IndexFactor: 1.3},
	{Name: "order_item", BaseRowSize: 50, ScaleRatio: 300000, IndexFactor: 1.3},
	{Name: "product_review", BaseRowSize: 2000, ScaleRatio: 50000, IndexFactor: 1.5},
}

// Reference data
var productCategories = []string{
	"Electronics", "Clothing", "Home & Garden", "Sports", "Toys",
	"Books", "Health", "Automotive", "Jewelry", "Food",
}

var productAdjectives = []string{
	"Premium", "Professional", "Deluxe", "Essential", "Ultimate",
	"Classic", "Modern", "Vintage", "Eco-friendly", "Smart",
}

var warehouses = []string{"EAST", "WEST", "CENTRAL"}

// Generator generates test data for the ecommerce schema.
type Generator struct {
	faker      *datagen.Faker
	cfg        datagen.BatchInsertConfig
	embedder   embeddings.Embedder
	dimensions int
}

// NewGenerator creates a new ecommerce data generator.
func NewGenerator(embedder embeddings.Embedder, dimensions int) *Generator {
	return &Generator{
		faker:      datagen.NewFaker(),
		cfg:        datagen.DefaultBatchConfig(),
		embedder:   embedder,
		dimensions: dimensions,
	}
}

// GenerateData generates test data to approximately fill the target size.
func (g *Generator) GenerateData(ctx context.Context, pool *pgxpool.Pool, targetSize int64) error {
	// Adjust table sizes based on embedding dimensions
	adjustedSizes := make([]datagen.TableSizeInfo, len(tableSizes))
	copy(adjustedSizes, tableSizes)
	for i := range adjustedSizes {
		if adjustedSizes[i].Name == "product" || adjustedSizes[i].Name == "product_review" {
			// Vector storage: dimensions * 4 bytes
			adjustedSizes[i].BaseRowSize += int64(g.dimensions * 4)
		}
	}

	calc := datagen.NewSizeCalculator(adjustedSizes)
	rowCounts := calc.CalculateRowCounts(targetSize)

	scaleFactor := max(1, int(rowCounts["product"]/10000))

	logging.Info().
		Int("scale_factor", scaleFactor).
		Int("dimensions", g.dimensions).
		Str("estimated_size", datagen.FormatSize(calc.EstimatedSize(rowCounts))).
		Msg("Generating ecommerce data")

	// Generate reference data
	numCategories := scaleFactor * 100
	if err := g.generateCategories(ctx, pool, numCategories); err != nil {
		return fmt.Errorf("failed to generate categories: %w", err)
	}

	numBrands := scaleFactor * 50
	if err := g.generateBrands(ctx, pool, numBrands); err != nil {
		return fmt.Errorf("failed to generate brands: %w", err)
	}

	numProducts := scaleFactor * 10000
	if err := g.generateProducts(ctx, pool, numProducts, numCategories, numBrands); err != nil {
		return fmt.Errorf("failed to generate products: %w", err)
	}

	if err := g.generateInventory(ctx, pool, numProducts); err != nil {
		return fmt.Errorf("failed to generate inventory: %w", err)
	}

	numCustomers := scaleFactor * 50000
	if err := g.generateCustomers(ctx, pool, numCustomers); err != nil {
		return fmt.Errorf("failed to generate customers: %w", err)
	}

	numOrders := scaleFactor * 100000
	if err := g.generateOrders(ctx, pool, numOrders, numCustomers, numProducts); err != nil {
		return fmt.Errorf("failed to generate orders: %w", err)
	}

	numReviews := scaleFactor * 50000
	if err := g.generateReviews(ctx, pool, numReviews, numProducts, numCustomers); err != nil {
		return fmt.Errorf("failed to generate reviews: %w", err)
	}

	return nil
}

func (g *Generator) generateCategories(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating categories")
	batch := make([]string, 0, g.cfg.BatchSize)

	for i := 1; i <= count; i++ {
		baseCategory := productCategories[(i-1)%len(productCategories)]
		name := fmt.Sprintf("%s %s", datagen.Choose(g.faker, productAdjectives), baseCategory)

		var parentID string
		if i > len(productCategories) && g.faker.Int(1, 3) == 1 {
			parentID = fmt.Sprintf("%d", g.faker.Int(1, len(productCategories)))
		} else {
			parentID = "NULL"
		}

		batch = append(batch, fmt.Sprintf("('%s', '%s', %s)",
			escapeSingleQuote(name),
			escapeSingleQuote(g.faker.Sentence(10)),
			parentID,
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "category", "(name, description, parent_id)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "category", "(name, description, parent_id)", batch); err != nil {
			return err
		}
	}

	logging.Info().Msg("categories complete")
	return nil
}

func (g *Generator) generateBrands(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating brands")
	batch := make([]string, 0, g.cfg.BatchSize)

	for i := 1; i <= count; i++ {
		name := g.faker.Company()
		if len(name) > 100 {
			name = name[:100]
		}

		// Remove spaces and apostrophes for URL-safe domain name
		domain := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(name, " ", ""), "'", ""))
		batch = append(batch, fmt.Sprintf("('%s', '%s', 'https://www.%s.com')",
			escapeSingleQuote(name),
			escapeSingleQuote(g.faker.Sentence(8)),
			domain,
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "brand", "(name, description, website)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "brand", "(name, description, website)", batch); err != nil {
			return err
		}
	}

	logging.Info().Msg("brands complete")
	return nil
}

func (g *Generator) generateProducts(ctx context.Context, pool *pgxpool.Pool, count, numCategories, numBrands int) error {
	logging.Info().Int("count", count).Msg("Generating products")
	batch := make([]string, 0, g.cfg.BatchSize/10) // Smaller batches due to embeddings
	progress := datagen.NewProgressReporter("product", int64(count), g.cfg.ProgressInterval)

	for i := 1; i <= count; i++ {
		name := g.faker.ProductName()
		if len(name) > 200 {
			name = name[:200]
		}
		description := g.faker.ProductDescription()
		price := g.faker.Float64(5, 2000)
		cost := price * g.faker.Float64(0.3, 0.7)

		// Generate embedding for product
		embeddingText := name + " " + description
		embedding := g.embedder.Embed(embeddingText)
		embeddingStr := formatEmbedding(embedding)

		batch = append(batch, fmt.Sprintf("('%s', '%s', '%s', %d, %d, %.2f, %.2f, %.2f, TRUE, '%s')",
			fmt.Sprintf("SKU-%08d", i),
			escapeSingleQuote(name),
			escapeSingleQuote(description),
			g.faker.Int(1, numCategories),
			g.faker.Int(1, numBrands),
			price,
			cost,
			g.faker.Float64(0.1, 50),
			embeddingStr,
		))

		if len(batch) >= g.cfg.BatchSize/10 {
			if err := g.executeBatchInsert(ctx, pool, "product",
				"(sku, name, description, category_id, brand_id, price, cost, weight, is_active, embedding)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "product",
			"(sku, name, description, category_id, brand_id, price, cost, weight, is_active, embedding)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateInventory(ctx context.Context, pool *pgxpool.Pool, numProducts int) error {
	logging.Info().Msg("Generating inventory")
	batch := make([]string, 0, g.cfg.BatchSize)

	for p := 1; p <= numProducts; p++ {
		for _, wh := range warehouses {
			qty := g.faker.Int(0, 1000)
			reserved := g.faker.Int(0, qty/10)

			batch = append(batch, fmt.Sprintf("(%d, '%s', %d, %d)",
				p, wh, qty, reserved,
			))

			if len(batch) >= g.cfg.BatchSize {
				if err := g.executeBatchInsert(ctx, pool, "inventory",
					"(product_id, warehouse, quantity, reserved)", batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "inventory",
			"(product_id, warehouse, quantity, reserved)", batch); err != nil {
			return err
		}
	}

	logging.Info().Msg("inventory complete")
	return nil
}

func (g *Generator) generateCustomers(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating customers")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("customer", int64(count), g.cfg.ProgressInterval)

	usStates := []string{"AL", "AK", "AZ", "CA", "CO", "FL", "GA", "IL", "NY", "TX", "WA"}

	for i := 1; i <= count; i++ {
		firstName := g.faker.FirstName()
		lastName := g.faker.LastName()

		batch = append(batch, fmt.Sprintf("('customer%d@example.com', '%s', '%s', '%s', '%s', NULL, '%s', '%s', '%s', 'USA')",
			i,
			escapeSingleQuote(firstName),
			escapeSingleQuote(lastName),
			g.faker.Phone(),
			escapeSingleQuote(g.faker.Street()),
			escapeSingleQuote(g.faker.City()),
			datagen.Choose(g.faker, usStates),
			g.faker.Zip(),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "customer",
				"(email, first_name, last_name, phone, address_line1, address_line2, city, state, postal_code, country)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "customer",
			"(email, first_name, last_name, phone, address_line1, address_line2, city, state, postal_code, country)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateOrders(ctx context.Context, pool *pgxpool.Pool, count, numCustomers, numProducts int) error {
	logging.Info().Int("count", count).Msg("Generating orders")
	orderBatch := make([]string, 0, g.cfg.BatchSize/10)
	itemBatch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("orders", int64(count), int64(count/10))

	statuses := []string{"pending", "processing", "shipped", "delivered", "cancelled"}
	baseDate := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 1; i <= count; i++ {
		customerID := g.faker.Int(1, numCustomers)
		status := datagen.Choose(g.faker, statuses)
		numItems := g.faker.Int(1, 5)

		var subtotal float64
		for j := 1; j <= numItems; j++ {
			productID := g.faker.Int(1, numProducts)
			qty := g.faker.Int(1, 3)
			unitPrice := g.faker.Float64(10, 500)
			totalPrice := float64(qty) * unitPrice
			subtotal += totalPrice

			itemBatch = append(itemBatch, fmt.Sprintf("(%d, %d, %d, %.2f, %.2f)",
				i, productID, qty, unitPrice, totalPrice,
			))
		}

		tax := subtotal * 0.08
		shipping := g.faker.Float64(0, 20)
		total := subtotal + tax + shipping

		orderDate := baseDate.Add(time.Duration(g.faker.Int(0, 365*2*24)) * time.Hour)

		orderBatch = append(orderBatch, fmt.Sprintf("(%d, '%s', %.2f, %.2f, %.2f, %.2f, '%s', '%s', '%s')",
			customerID, status, subtotal, tax, shipping, total,
			escapeSingleQuote(g.faker.Street()+" "+g.faker.City()),
			escapeSingleQuote(g.faker.Street()+" "+g.faker.City()),
			orderDate.Format("2006-01-02 15:04:05"),
		))

		if len(orderBatch) >= g.cfg.BatchSize/10 {
			if err := g.executeBatchInsert(ctx, pool, "orders",
				"(customer_id, status, subtotal, tax, shipping, total, shipping_address, billing_address, created_at)", orderBatch); err != nil {
				return err
			}
			if err := g.executeBatchInsert(ctx, pool, "order_item",
				"(order_id, product_id, quantity, unit_price, total_price)", itemBatch); err != nil {
				return err
			}
			progress.Update(int64(len(orderBatch)))
			orderBatch = orderBatch[:0]
			itemBatch = itemBatch[:0]
		}
	}

	if len(orderBatch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "orders",
			"(customer_id, status, subtotal, tax, shipping, total, shipping_address, billing_address, created_at)", orderBatch); err != nil {
			return err
		}
		if err := g.executeBatchInsert(ctx, pool, "order_item",
			"(order_id, product_id, quantity, unit_price, total_price)", itemBatch); err != nil {
			return err
		}
		progress.Update(int64(len(orderBatch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateReviews(ctx context.Context, pool *pgxpool.Pool, count, numProducts, numCustomers int) error {
	logging.Info().Int("count", count).Msg("Generating product reviews")
	batch := make([]string, 0, g.cfg.BatchSize/10)
	progress := datagen.NewProgressReporter("product_review", int64(count), g.cfg.ProgressInterval)

	reviewTitles := []string{"Great product!", "Disappointed", "Exactly what I needed",
		"Good value", "Not as described", "Highly recommend", "Average quality"}

	for i := 1; i <= count; i++ {
		productID := g.faker.Int(1, numProducts)
		customerID := g.faker.Int(1, numCustomers)
		rating := g.faker.Int(1, 5)
		title := datagen.Choose(g.faker, reviewTitles)
		text := g.faker.Sentence(20)

		// Generate embedding for review
		embeddingText := title + " " + text
		embedding := g.embedder.Embed(embeddingText)
		embeddingStr := formatEmbedding(embedding)

		batch = append(batch, fmt.Sprintf("(%d, %d, %d, '%s', '%s', %d, %t, '%s')",
			productID, customerID, rating,
			escapeSingleQuote(title),
			escapeSingleQuote(text),
			g.faker.Int(0, 100),
			g.faker.Int(1, 2) == 1,
			embeddingStr,
		))

		if len(batch) >= g.cfg.BatchSize/10 {
			if err := g.executeBatchInsert(ctx, pool, "product_review",
				"(product_id, customer_id, rating, title, review_text, helpful_votes, verified, embedding)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "product_review",
			"(product_id, customer_id, rating, title, review_text, helpful_votes, verified, embedding)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
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

func formatEmbedding(embedding []float32) string {
	parts := make([]string, len(embedding))
	for i, v := range embedding {
		parts[i] = fmt.Sprintf("%.6f", v)
	}
	return "[" + strings.Join(parts, ",") + "]"
}
