//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Portions copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

package ecommerce

import (
	"context"
	"time"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen/embeddings"
)

// Query weights for e-commerce workload
var queryWeights = map[string]int{
	"semantic_search":  30,
	"category_browse":  20,
	"similar_products": 15,
	"add_to_cart":      10,
	"checkout":         5,
	"submit_review":    5,
	"order_history":    10,
	"inventory_check":  5,
}

// QueryExecutor executes ecommerce queries.
type QueryExecutor struct {
	faker         *datagen.Faker
	embedder      embeddings.Embedder
	numProducts   int
	numCustomers  int
	numCategories int
	numOrders     int
}

// NewQueryExecutor creates a new query executor.
func NewQueryExecutor(embedder embeddings.Embedder, numProducts, numCustomers, numCategories, numOrders int) *QueryExecutor {
	return &QueryExecutor{
		faker:         datagen.NewFaker(),
		embedder:      embedder,
		numProducts:   max(1, numProducts),
		numCustomers:  max(1, numCustomers),
		numCategories: max(1, numCategories),
		numOrders:     max(1, numOrders),
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
	case "category_browse":
		rowsAffected, err = e.executeCategoryBrowse(ctx, db)
	case "similar_products":
		rowsAffected, err = e.executeSimilarProducts(ctx, db)
	case "add_to_cart":
		rowsAffected, err = e.executeAddToCart(ctx, db)
	case "checkout":
		rowsAffected, err = e.executeCheckout(ctx, db)
	case "submit_review":
		rowsAffected, err = e.executeSubmitReview(ctx, db)
	case "order_history":
		rowsAffected, err = e.executeOrderHistory(ctx, db)
	case "inventory_check":
		rowsAffected, err = e.executeInventoryCheck(ctx, db)
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

// Semantic Search - Vector similarity search for products
func (e *QueryExecutor) executeSemanticSearch(ctx context.Context, db apps.DB) (int64, error) {
	// Generate a search query
	searchTerms := []string{
		"comfortable running shoes",
		"waterproof outdoor jacket",
		"wireless bluetooth headphones",
		"organic cotton t-shirt",
		"stainless steel water bottle",
		"ergonomic office chair",
		"portable power bank",
		"lightweight laptop bag",
	}
	searchQuery := datagen.Choose(e.faker, searchTerms)
	queryEmbedding := e.embedder.Embed(searchQuery)

	rows, err := db.Query(ctx, `
        SELECT p.id, p.name, p.description, p.price, c.name AS category,
               1 - (p.embedding <=> $1::vector) AS similarity
        FROM product p
        JOIN category c ON p.category_id = c.id
        WHERE p.is_active = TRUE
        ORDER BY p.embedding <=> $1::vector
        LIMIT 20
    `, formatEmbeddingForQuery(queryEmbedding))
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

// Category Browse - Traditional category-based browsing
func (e *QueryExecutor) executeCategoryBrowse(ctx context.Context, db apps.DB) (int64, error) {
	categoryID := e.faker.Int(1, e.numCategories)

	rows, err := db.Query(ctx, `
        SELECT p.id, p.name, p.price, b.name AS brand,
               COALESCE(AVG(r.rating), 0) AS avg_rating,
               COUNT(r.id) AS review_count
        FROM product p
        LEFT JOIN brand b ON p.brand_id = b.id
        LEFT JOIN product_review r ON p.id = r.product_id
        WHERE p.category_id = $1 AND p.is_active = TRUE
        GROUP BY p.id, p.name, p.price, b.name
        ORDER BY p.price
        LIMIT 50
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

// Similar Products - Find products similar to a given product
func (e *QueryExecutor) executeSimilarProducts(ctx context.Context, db apps.DB) (int64, error) {
	productID := e.faker.Int(1, e.numProducts)

	rows, err := db.Query(ctx, `
        SELECT p2.id, p2.name, p2.price,
               1 - (p2.embedding <=> p1.embedding) AS similarity
        FROM product p1, product p2
        WHERE p1.id = $1
            AND p2.id != p1.id
            AND p2.is_active = TRUE
        ORDER BY p2.embedding <=> p1.embedding
        LIMIT 10
    `, productID)
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

// Add to Cart - Add a product to cart
func (e *QueryExecutor) executeAddToCart(ctx context.Context, db apps.DB) (int64, error) {
	customerID := e.faker.Int(1, e.numCustomers)
	productID := e.faker.Int(1, e.numProducts)

	// Get or create cart
	var cartID int
	err := db.QueryRow(ctx, `
        INSERT INTO cart (customer_id)
        VALUES ($1)
        ON CONFLICT DO NOTHING
        RETURNING id
    `, customerID).Scan(&cartID)
	if err != nil {
		// Cart might already exist, get it
		err = db.QueryRow(ctx, `SELECT id FROM cart WHERE customer_id = $1 ORDER BY id DESC LIMIT 1`, customerID).Scan(&cartID)
		if err != nil {
			return 0, err
		}
	}

	// Add item to cart
	_, err = db.Exec(ctx, `
        INSERT INTO cart_item (cart_id, product_id, quantity)
        VALUES ($1, $2, $3)
        ON CONFLICT (cart_id, product_id)
        DO UPDATE SET quantity = cart_item.quantity + $3, added_at = NOW()
    `, cartID, productID, e.faker.Int(1, 3))
	if err != nil {
		return 0, err
	}

	return 1, nil
}

// Checkout - Create an order from cart
func (e *QueryExecutor) executeCheckout(ctx context.Context, db apps.DB) (int64, error) {
	customerID := e.faker.Int(1, e.numCustomers)

	// Check if customer has a cart with items
	var cartID int
	var itemCount int
	err := db.QueryRow(ctx, `
        SELECT c.id, COUNT(ci.id)
        FROM cart c
        LEFT JOIN cart_item ci ON c.id = ci.cart_id
        WHERE c.customer_id = $1
        GROUP BY c.id
        ORDER BY c.id DESC
        LIMIT 1
    `, customerID).Scan(&cartID, &itemCount)
	if err != nil || itemCount == 0 {
		// No cart or empty cart - simulate adding items first
		return 0, nil
	}

	// Calculate order totals
	var subtotal float64
	err = db.QueryRow(ctx, `
        SELECT COALESCE(SUM(p.price * ci.quantity), 0)
        FROM cart_item ci
        JOIN product p ON ci.product_id = p.id
        WHERE ci.cart_id = $1
    `, cartID).Scan(&subtotal)
	if err != nil {
		return 0, err
	}

	if subtotal == 0 {
		return 0, nil
	}

	tax := subtotal * 0.08
	shipping := 9.99
	total := subtotal + tax + shipping

	// Create order
	var orderID int
	err = db.QueryRow(ctx, `
        INSERT INTO orders (customer_id, status, subtotal, tax, shipping, total, shipping_address, billing_address)
        VALUES ($1, 'pending', $2, $3, $4, $5, 'Default Address', 'Default Address')
        RETURNING id
    `, customerID, subtotal, tax, shipping, total).Scan(&orderID)
	if err != nil {
		return 0, err
	}

	// Create order items from cart
	_, err = db.Exec(ctx, `
        INSERT INTO order_item (order_id, product_id, quantity, unit_price, total_price)
        SELECT $1, ci.product_id, ci.quantity, p.price, p.price * ci.quantity
        FROM cart_item ci
        JOIN product p ON ci.product_id = p.id
        WHERE ci.cart_id = $2
    `, orderID, cartID)
	if err != nil {
		return 1, err
	}

	// Clear cart
	_, err = db.Exec(ctx, `DELETE FROM cart_item WHERE cart_id = $1`, cartID)
	if err != nil {
		return 1, err
	}

	return 1, nil
}

// Submit Review - Add a product review
func (e *QueryExecutor) executeSubmitReview(ctx context.Context, db apps.DB) (int64, error) {
	productID := e.faker.Int(1, e.numProducts)
	customerID := e.faker.Int(1, e.numCustomers)
	rating := e.faker.Int(1, 5)
	title := datagen.Choose(e.faker, []string{"Great!", "Good value", "Disappointed", "Highly recommend", "Average"})
	text := e.faker.Sentence(15)

	embedding := e.embedder.Embed(title + " " + text)

	_, err := db.Exec(ctx, `
        INSERT INTO product_review (product_id, customer_id, rating, title, review_text, verified, embedding)
        VALUES ($1, $2, $3, $4, $5, $6, $7::vector)
    `, productID, customerID, rating, title, text, e.faker.Bool(), formatEmbeddingForQuery(embedding))
	if err != nil {
		return 0, err
	}

	return 1, nil
}

// Order History - Get customer order history
func (e *QueryExecutor) executeOrderHistory(ctx context.Context, db apps.DB) (int64, error) {
	customerID := e.faker.Int(1, e.numCustomers)

	rows, err := db.Query(ctx, `
        SELECT o.id, o.status, o.total, o.created_at,
               COUNT(oi.id) AS item_count
        FROM orders o
        LEFT JOIN order_item oi ON o.id = oi.order_id
        WHERE o.customer_id = $1
        GROUP BY o.id, o.status, o.total, o.created_at
        ORDER BY o.created_at DESC
        LIMIT 20
    `, customerID)
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

// Inventory Check - Check stock levels
func (e *QueryExecutor) executeInventoryCheck(ctx context.Context, db apps.DB) (int64, error) {
	productID := e.faker.Int(1, e.numProducts)

	rows, err := db.Query(ctx, `
        SELECT i.warehouse, i.quantity, i.reserved,
               (i.quantity - i.reserved) AS available
        FROM inventory i
        WHERE i.product_id = $1
    `, productID)
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

func formatEmbeddingForQuery(embedding []float32) string {
	parts := make([]string, len(embedding))
	for i, v := range embedding {
		parts[i] = datagen.FormatFloat(v)
	}
	return "[" + join(parts, ",") + "]"
}

func join(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += sep + parts[i]
	}
	return result
}
