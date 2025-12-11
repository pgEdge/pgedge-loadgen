package wholesale

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen"
)

// QueryExecutor executes wholesale queries.
type QueryExecutor struct {
	faker         *datagen.Faker
	numWarehouses int
}

// NewQueryExecutor creates a new query executor.
func NewQueryExecutor(numWarehouses int) *QueryExecutor {
	return &QueryExecutor{
		faker:         datagen.NewFaker(),
		numWarehouses: max(1, numWarehouses),
	}
}

// ExecuteRandomQuery executes a random query based on the TPC-C weights.
func (e *QueryExecutor) ExecuteRandomQuery(ctx context.Context, pool *pgxpool.Pool) apps.QueryResult {
	// Select query type based on weights
	queryType := e.selectQueryType()

	start := time.Now()
	var err error

	switch queryType {
	case "new_order":
		err = e.executeNewOrder(ctx, pool)
	case "payment":
		err = e.executePayment(ctx, pool)
	case "order_status":
		err = e.executeOrderStatus(ctx, pool)
	case "delivery":
		err = e.executeDelivery(ctx, pool)
	case "stock_level":
		err = e.executeStockLevel(ctx, pool)
	}

	return apps.QueryResult{
		QueryName: queryType,
		Duration:  time.Since(start).Nanoseconds(),
		Error:     err,
	}
}

func (e *QueryExecutor) selectQueryType() string {
	types := []string{"new_order", "payment", "order_status", "delivery", "stock_level"}
	weights := []int{45, 43, 4, 4, 4}
	return datagen.ChooseWeighted(e.faker, types, weights)
}

// New Order transaction
func (e *QueryExecutor) executeNewOrder(ctx context.Context, pool *pgxpool.Pool) error {
	wID := e.faker.Int(1, e.numWarehouses)
	dID := e.faker.Int(1, 10)
	cID := e.faker.Int(1, 3000)
	olCnt := e.faker.Int(5, 15)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Get warehouse tax
	var wTax float64
	err = tx.QueryRow(ctx, "SELECT w_tax FROM warehouse WHERE w_id = $1", wID).Scan(&wTax)
	if err != nil {
		return err
	}

	// Get district info and update next order ID
	var dTax float64
	var oID int
	err = tx.QueryRow(ctx, `
        UPDATE district SET d_next_o_id = d_next_o_id + 1
        WHERE d_w_id = $1 AND d_id = $2
        RETURNING d_tax, d_next_o_id - 1
    `, wID, dID).Scan(&dTax, &oID)
	if err != nil {
		return err
	}

	// Get customer info
	var cDiscount float64
	var cLast, cCredit string
	err = tx.QueryRow(ctx, `
        SELECT c_discount, c_last, c_credit
        FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3
    `, wID, dID, cID).Scan(&cDiscount, &cLast, &cCredit)
	if err != nil {
		return err
	}

	// Insert order
	_, err = tx.Exec(ctx, `
        INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
        VALUES ($1, $2, $3, $4, $5, $6, 1)
    `, oID, dID, wID, cID, time.Now(), olCnt)
	if err != nil {
		return err
	}

	// Insert new_order
	_, err = tx.Exec(ctx, `
        INSERT INTO new_orders (no_o_id, no_d_id, no_w_id)
        VALUES ($1, $2, $3)
    `, oID, dID, wID)
	if err != nil {
		return err
	}

	// Process order lines
	for ol := 1; ol <= olCnt; ol++ {
		iID := e.faker.Int(1, 100000)
		qty := e.faker.Int(1, 10)

		// Get item info
		var iPrice float64
		var iName, iData string
		err = tx.QueryRow(ctx, `
            SELECT i_price, i_name, i_data FROM item WHERE i_id = $1
        `, iID).Scan(&iPrice, &iName, &iData)
		if err != nil {
			if err == pgx.ErrNoRows {
				// Invalid item - rollback (1% of transactions per TPC-C)
				return tx.Rollback(ctx)
			}
			return err
		}

		// Get and update stock
		var sQty int
		var sDistInfo string
		distCol := fmt.Sprintf("s_dist_%02d", dID)
		err = tx.QueryRow(ctx, fmt.Sprintf(`
            UPDATE stock SET s_quantity = CASE WHEN s_quantity >= $3 + 10 THEN s_quantity - $3 ELSE s_quantity + 91 END,
                s_ytd = s_ytd + $3, s_order_cnt = s_order_cnt + 1
            WHERE s_i_id = $1 AND s_w_id = $2
            RETURNING s_quantity, %s
        `, distCol), iID, wID, qty).Scan(&sQty, &sDistInfo)
		if err != nil {
			return err
		}

		// Calculate amount
		amount := float64(qty) * iPrice

		// Insert order line
		_, err = tx.Exec(ctx, `
            INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, oID, dID, wID, ol, iID, wID, qty, amount, sDistInfo)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// Payment transaction
func (e *QueryExecutor) executePayment(ctx context.Context, pool *pgxpool.Pool) error {
	wID := e.faker.Int(1, e.numWarehouses)
	dID := e.faker.Int(1, 10)
	cID := e.faker.Int(1, 3000)
	amount := e.faker.Float64(1, 5000)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Update warehouse YTD
	_, err = tx.Exec(ctx, `
        UPDATE warehouse SET w_ytd = w_ytd + $2 WHERE w_id = $1
    `, wID, amount)
	if err != nil {
		return err
	}

	// Update district YTD
	_, err = tx.Exec(ctx, `
        UPDATE district SET d_ytd = d_ytd + $3 WHERE d_w_id = $1 AND d_id = $2
    `, wID, dID, amount)
	if err != nil {
		return err
	}

	// Update customer
	_, err = tx.Exec(ctx, `
        UPDATE customer SET c_balance = c_balance - $4,
            c_ytd_payment = c_ytd_payment + $4,
            c_payment_cnt = c_payment_cnt + 1
        WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3
    `, wID, dID, cID, amount)
	if err != nil {
		return err
	}

	// Insert history
	_, err = tx.Exec(ctx, `
        INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    `, cID, dID, wID, dID, wID, time.Now(), amount, e.faker.StringN(24))
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// Order Status transaction
func (e *QueryExecutor) executeOrderStatus(ctx context.Context, pool *pgxpool.Pool) error {
	wID := e.faker.Int(1, e.numWarehouses)
	dID := e.faker.Int(1, 10)
	cID := e.faker.Int(1, 3000)

	// Get customer info
	var cFirst, cMiddle, cLast string
	var cBalance float64
	err := pool.QueryRow(ctx, `
        SELECT c_first, c_middle, c_last, c_balance
        FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3
    `, wID, dID, cID).Scan(&cFirst, &cMiddle, &cLast, &cBalance)
	if err != nil {
		return err
	}

	// Get latest order
	var oID int
	var oCarrierID *int
	var oEntryD time.Time
	err = pool.QueryRow(ctx, `
        SELECT o_id, o_carrier_id, o_entry_d
        FROM orders WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3
        ORDER BY o_id DESC LIMIT 1
    `, wID, dID, cID).Scan(&oID, &oCarrierID, &oEntryD)
	if err != nil && err != pgx.ErrNoRows {
		return err
	}

	if oID > 0 {
		// Get order lines
		rows, err := pool.Query(ctx, `
            SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
            FROM order_line WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3
        `, wID, dID, oID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var olIID, olSupplyWID, olQty int
			var olAmount float64
			var olDeliveryD *time.Time
			if err := rows.Scan(&olIID, &olSupplyWID, &olQty, &olAmount, &olDeliveryD); err != nil {
				return err
			}
		}
	}

	return nil
}

// Delivery transaction
func (e *QueryExecutor) executeDelivery(ctx context.Context, pool *pgxpool.Pool) error {
	wID := e.faker.Int(1, e.numWarehouses)
	carrierID := e.faker.Int(1, 10)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Process each district
	for dID := 1; dID <= 10; dID++ {
		// Get oldest undelivered order
		var oID int
		err := tx.QueryRow(ctx, `
            SELECT no_o_id FROM new_orders
            WHERE no_w_id = $1 AND no_d_id = $2
            ORDER BY no_o_id LIMIT 1 FOR UPDATE
        `, wID, dID).Scan(&oID)
		if err != nil {
			if err == pgx.ErrNoRows {
				continue // No pending orders for this district
			}
			return err
		}

		// Delete from new_orders
		_, err = tx.Exec(ctx, `
            DELETE FROM new_orders WHERE no_w_id = $1 AND no_d_id = $2 AND no_o_id = $3
        `, wID, dID, oID)
		if err != nil {
			return err
		}

		// Get customer ID and update order
		var cID int
		err = tx.QueryRow(ctx, `
            UPDATE orders SET o_carrier_id = $4
            WHERE o_w_id = $1 AND o_d_id = $2 AND o_id = $3
            RETURNING o_c_id
        `, wID, dID, oID, carrierID).Scan(&cID)
		if err != nil {
			return err
		}

		// Get total amount from order lines
		var totalAmount float64
		err = tx.QueryRow(ctx, `
            SELECT COALESCE(SUM(ol_amount), 0) FROM order_line
            WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3
        `, wID, dID, oID).Scan(&totalAmount)
		if err != nil {
			return err
		}

		// Update order lines with delivery date
		_, err = tx.Exec(ctx, `
            UPDATE order_line SET ol_delivery_d = $4
            WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3
        `, wID, dID, oID, time.Now())
		if err != nil {
			return err
		}

		// Update customer balance
		_, err = tx.Exec(ctx, `
            UPDATE customer SET c_balance = c_balance + $4, c_delivery_cnt = c_delivery_cnt + 1
            WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3
        `, wID, dID, cID, totalAmount)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// Stock Level transaction
func (e *QueryExecutor) executeStockLevel(ctx context.Context, pool *pgxpool.Pool) error {
	wID := e.faker.Int(1, e.numWarehouses)
	dID := e.faker.Int(1, 10)
	threshold := e.faker.Int(10, 20)

	// Get next order ID
	var nextOID int
	err := pool.QueryRow(ctx, `
        SELECT d_next_o_id FROM district WHERE d_w_id = $1 AND d_id = $2
    `, wID, dID).Scan(&nextOID)
	if err != nil {
		return err
	}

	// Count items below threshold
	var lowStock int
	err = pool.QueryRow(ctx, `
        SELECT COUNT(DISTINCT s_i_id)
        FROM order_line
        JOIN stock ON s_i_id = ol_i_id AND s_w_id = ol_w_id
        WHERE ol_w_id = $1 AND ol_d_id = $2
            AND ol_o_id >= $3 AND ol_o_id < $4
            AND s_quantity < $5
    `, wID, dID, nextOID-20, nextOID, threshold).Scan(&lowStock)
	if err != nil {
		return err
	}

	return nil
}
