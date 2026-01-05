//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Portions copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

package wholesale

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/datagen"
	"github.com/pgEdge/pgedge-loadgen/internal/logging"
)

// Table size information for the wholesale schema
// Based on TPC-C scaling rules
var tableSizes = []datagen.TableSizeInfo{
	{Name: "warehouse", BaseRowSize: 89, ScaleRatio: 1, IndexFactor: 1.1},
	{Name: "district", BaseRowSize: 95, ScaleRatio: 10, IndexFactor: 1.2},       // 10 per warehouse
	{Name: "customer", BaseRowSize: 655, ScaleRatio: 30000, IndexFactor: 1.3},   // 3000 per district
	{Name: "history", BaseRowSize: 46, ScaleRatio: 30000, IndexFactor: 1.1},     // 1 per customer initially
	{Name: "item", BaseRowSize: 82, ScaleRatio: 100000, IndexFactor: 1.2},       // Fixed 100k items
	{Name: "stock", BaseRowSize: 306, ScaleRatio: 100000, IndexFactor: 1.2},     // 100k per warehouse
	{Name: "orders", BaseRowSize: 24, ScaleRatio: 30000, IndexFactor: 1.3},      // 1 per customer initially
	{Name: "new_orders", BaseRowSize: 12, ScaleRatio: 9000, IndexFactor: 1.1},   // ~30% of orders
	{Name: "order_line", BaseRowSize: 54, ScaleRatio: 300000, IndexFactor: 1.2}, // ~10 per order
}

// Generator generates test data for the wholesale schema.
type Generator struct {
	faker *datagen.Faker
	cfg   datagen.BatchInsertConfig
}

// NewGenerator creates a new wholesale data generator.
func NewGenerator() *Generator {
	return &Generator{
		faker: datagen.NewFaker(),
		cfg:   datagen.DefaultBatchConfig(),
	}
}

// GenerateData generates test data to approximately fill the target size.
func (g *Generator) GenerateData(ctx context.Context, pool *pgxpool.Pool, targetSize int64) error {
	calc := datagen.NewSizeCalculator(tableSizes)
	rowCounts := calc.CalculateRowCounts(targetSize)

	// Calculate number of warehouses (scale factor)
	numWarehouses := max(1, int(rowCounts["warehouse"]))

	// Recalculate based on TPC-C scaling rules
	numDistricts := numWarehouses * 10
	numCustomersPerDistrict := 3000
	numItems := 100000

	logging.Info().
		Int("warehouses", numWarehouses).
		Int("districts", numDistricts).
		Int("customers_per_district", numCustomersPerDistrict).
		Int("items", numItems).
		Str("estimated_size", datagen.FormatSize(calc.EstimatedSize(rowCounts))).
		Msg("Generating wholesale data")

	// Generate items first (no dependencies)
	if err := g.generateItems(ctx, pool, numItems); err != nil {
		return fmt.Errorf("failed to generate items: %w", err)
	}

	// Generate warehouses and related data
	for w := 1; w <= numWarehouses; w++ {
		if err := g.generateWarehouse(ctx, pool, w); err != nil {
			return fmt.Errorf("failed to generate warehouse %d: %w", w, err)
		}

		// Generate stock for this warehouse
		if err := g.generateStock(ctx, pool, w, numItems); err != nil {
			return fmt.Errorf("failed to generate stock for warehouse %d: %w", w, err)
		}

		// Generate districts for this warehouse
		for d := 1; d <= 10; d++ {
			if err := g.generateDistrict(ctx, pool, w, d); err != nil {
				return fmt.Errorf("failed to generate district %d-%d: %w", w, d, err)
			}

			// Generate customers for this district
			if err := g.generateCustomers(ctx, pool, w, d, numCustomersPerDistrict); err != nil {
				return fmt.Errorf("failed to generate customers for district %d-%d: %w", w, d, err)
			}

			// Generate orders for this district
			if err := g.generateOrders(ctx, pool, w, d, numCustomersPerDistrict); err != nil {
				return fmt.Errorf("failed to generate orders for district %d-%d: %w", w, d, err)
			}
		}

		logging.Info().
			Int("warehouse", w).
			Int("total", numWarehouses).
			Msg("Warehouse complete")
	}

	return nil
}

func (g *Generator) generateWarehouse(ctx context.Context, pool *pgxpool.Pool, wID int) error {
	_, err := pool.Exec(ctx, `
        INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    `,
		wID,
		g.faker.StringN(10),
		datagen.Truncate(g.faker.Street(), 20),
		g.faker.StringN(20),
		datagen.Truncate(g.faker.City(), 20),
		g.faker.State(),
		g.faker.Digits(9),
		g.faker.Float64(0, 0.2),
		300000.00,
	)
	return err
}

func (g *Generator) generateDistrict(ctx context.Context, pool *pgxpool.Pool, wID, dID int) error {
	_, err := pool.Exec(ctx, `
        INSERT INTO district (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `,
		dID,
		wID,
		g.faker.StringN(10),
		datagen.Truncate(g.faker.Street(), 20),
		g.faker.StringN(20),
		datagen.Truncate(g.faker.City(), 20),
		g.faker.State(),
		g.faker.Digits(9),
		g.faker.Float64(0, 0.2),
		30000.00,
		3001, // Next order ID starts at 3001 after initial 3000 orders
	)
	return err
}

func (g *Generator) generateItems(ctx context.Context, pool *pgxpool.Pool, numItems int) error {
	logging.Info().Int("items", numItems).Msg("Generating items")

	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("item", int64(numItems), g.cfg.ProgressInterval)

	for i := 1; i <= numItems; i++ {
		data := g.faker.StringN(50)
		// 10% of items are "original" (contain "ORIGINAL" in data)
		if g.faker.Int(1, 100) <= 10 {
			pos := g.faker.Int(0, min(30, len(data)-8))
			if pos >= 0 && pos+8 <= len(data) {
				data = data[:pos] + "ORIGINAL" + data[pos+8:]
			}
		}

		productName := g.faker.ProductName()
		if len(productName) > 24 {
			productName = productName[:24]
		}

		batch = append(batch, fmt.Sprintf("(%d, %d, '%s', %.2f, '%s')",
			i,
			g.faker.Int(1, 10000),
			escapeSingleQuote(productName),
			g.faker.Float64(1, 100),
			escapeSingleQuote(data),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "item", "(i_id, i_im_id, i_name, i_price, i_data)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "item", "(i_id, i_im_id, i_name, i_price, i_data)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}

	progress.Done()
	return nil
}

func (g *Generator) generateStock(ctx context.Context, pool *pgxpool.Pool, wID, numItems int) error {
	logging.Debug().Int("warehouse", wID).Msg("Generating stock")

	batch := make([]string, 0, g.cfg.BatchSize)

	for i := 1; i <= numItems; i++ {
		data := g.faker.StringN(50)
		if g.faker.Int(1, 100) <= 10 {
			pos := g.faker.Int(0, 30)
			data = data[:pos] + "ORIGINAL" + data[pos+8:]
		}

		batch = append(batch, fmt.Sprintf("(%d, %d, %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', 0, 0, 0, '%s')",
			i, wID,
			g.faker.Int(10, 100),
			g.faker.StringN(24), g.faker.StringN(24), g.faker.StringN(24),
			g.faker.StringN(24), g.faker.StringN(24), g.faker.StringN(24),
			g.faker.StringN(24), g.faker.StringN(24), g.faker.StringN(24),
			g.faker.StringN(24),
			escapeSingleQuote(data),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "stock",
				"(s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data)",
				batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "stock",
			"(s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data)",
			batch); err != nil {
			return err
		}
	}

	return nil
}

func (g *Generator) generateCustomers(ctx context.Context, pool *pgxpool.Pool, wID, dID, numCustomers int) error {
	batch := make([]string, 0, g.cfg.BatchSize)
	historyBatch := make([]string, 0, g.cfg.BatchSize)
	now := time.Now()

	for c := 1; c <= numCustomers; c++ {
		lastName := generateLastName(c - 1)
		credit := "GC"
		if g.faker.Int(1, 100) <= 10 {
			credit = "BC"
		}

		batch = append(batch, fmt.Sprintf(
			"(%d, %d, %d, '%s', 'OE', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', 50000.00, %.4f, -10.00, 10.00, 1, 0, '%s')",
			c, dID, wID,
			escapeSingleQuote(datagen.Truncate(g.faker.FirstName(), 16)),
			escapeSingleQuote(lastName),
			escapeSingleQuote(datagen.Truncate(g.faker.Street(), 20)),
			g.faker.StringN(20),
			escapeSingleQuote(datagen.Truncate(g.faker.City(), 20)),
			g.faker.State(),
			g.faker.Digits(9),
			g.faker.Digits(16),
			now.Format("2006-01-02 15:04:05"),
			credit,
			g.faker.Float64(0, 0.5),
			escapeSingleQuote(g.faker.StringN(300)),
		))

		// Generate history record
		historyBatch = append(historyBatch, fmt.Sprintf(
			"(%d, %d, %d, %d, %d, '%s', 10.00, '%s')",
			c, dID, wID, dID, wID,
			now.Format("2006-01-02 15:04:05"),
			g.faker.StringN(24),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "customer",
				"(c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data)",
				batch); err != nil {
				return err
			}
			batch = batch[:0]

			if err := g.executeBatchInsert(ctx, pool, "history",
				"(h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)",
				historyBatch); err != nil {
				return err
			}
			historyBatch = historyBatch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "customer",
			"(c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data)",
			batch); err != nil {
			return err
		}
		if err := g.executeBatchInsert(ctx, pool, "history",
			"(h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)",
			historyBatch); err != nil {
			return err
		}
	}

	return nil
}

func (g *Generator) generateOrders(ctx context.Context, pool *pgxpool.Pool, wID, dID, numOrders int) error {
	// Shuffle customer IDs for random order assignment
	customerIDs := make([]int, numOrders)
	for i := range customerIDs {
		customerIDs[i] = i + 1
	}
	g.shuffleInts(customerIDs)

	orderBatch := make([]string, 0, g.cfg.BatchSize)
	newOrderBatch := make([]string, 0, g.cfg.BatchSize)
	orderLineBatch := make([]string, 0, g.cfg.BatchSize)
	now := time.Now()

	for o := 1; o <= numOrders; o++ {
		olCnt := g.faker.Int(5, 15)
		carrierID := "NULL"
		if o < numOrders*7/10 { // 70% of orders are delivered
			carrierID = fmt.Sprintf("%d", g.faker.Int(1, 10))
		}

		orderBatch = append(orderBatch, fmt.Sprintf(
			"(%d, %d, %d, %d, '%s', %s, %d, 1)",
			o, dID, wID, customerIDs[o-1],
			now.Add(-time.Duration(numOrders-o)*time.Minute).Format("2006-01-02 15:04:05"),
			carrierID, olCnt,
		))

		// New orders for undelivered orders (last 30%)
		if o >= numOrders*7/10 {
			newOrderBatch = append(newOrderBatch, fmt.Sprintf("(%d, %d, %d)", o, dID, wID))
		}

		// Generate order lines
		for ol := 1; ol <= olCnt; ol++ {
			deliveryD := "NULL"
			if carrierID != "NULL" {
				deliveryD = fmt.Sprintf("'%s'", now.Format("2006-01-02 15:04:05"))
			}
			amount := 0.00
			if carrierID == "NULL" {
				amount = g.faker.Float64(0.01, 9999.99)
			}

			orderLineBatch = append(orderLineBatch, fmt.Sprintf(
				"(%d, %d, %d, %d, %d, %d, %s, 5, %.2f, '%s')",
				o, dID, wID, ol,
				g.faker.Int(1, 100000), // item id
				wID,                    // supply warehouse
				deliveryD, amount,
				g.faker.StringN(24),
			))
		}

		if len(orderBatch) >= g.cfg.BatchSize/10 { // Smaller batches due to order_line explosion
			if err := g.executeBatchInsert(ctx, pool, "orders",
				"(o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)",
				orderBatch); err != nil {
				return err
			}
			orderBatch = orderBatch[:0]

			if len(newOrderBatch) > 0 {
				if err := g.executeBatchInsert(ctx, pool, "new_orders",
					"(no_o_id, no_d_id, no_w_id)", newOrderBatch); err != nil {
					return err
				}
				newOrderBatch = newOrderBatch[:0]
			}

			if err := g.executeBatchInsert(ctx, pool, "order_line",
				"(ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info)",
				orderLineBatch); err != nil {
				return err
			}
			orderLineBatch = orderLineBatch[:0]
		}
	}

	// Insert remaining batches
	if len(orderBatch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "orders",
			"(o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)",
			orderBatch); err != nil {
			return err
		}
	}
	if len(newOrderBatch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "new_orders",
			"(no_o_id, no_d_id, no_w_id)", newOrderBatch); err != nil {
			return err
		}
	}
	if len(orderLineBatch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "order_line",
			"(ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info)",
			orderLineBatch); err != nil {
			return err
		}
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

func (g *Generator) shuffleInts(slice []int) {
	for i := len(slice) - 1; i > 0; i-- {
		j := g.faker.Int(0, i)
		slice[i], slice[j] = slice[j], slice[i]
	}
}

// generateLastName generates a last name using the TPC-C syllable method.
func generateLastName(num int) string {
	syllables := []string{
		"BAR", "OUGHT", "ABLE", "PRI", "PRES",
		"ESE", "ANTI", "CALLY", "ATION", "EING",
	}

	// For numbers 0-999, use specific mapping
	// For numbers >= 1000, use random
	if num >= 1000 {
		num = num % 1000
	}

	return syllables[num/100] + syllables[(num/10)%10] + syllables[num%10]
}

func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
