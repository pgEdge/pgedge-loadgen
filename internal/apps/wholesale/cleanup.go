package wholesale

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/pgEdge/pgedge-loadgen/internal/logging"
)

// MaintainSize checks if the database has grown beyond the target size
// and deletes old orders to bring it back within bounds.
// Returns the number of orders deleted and any error.
func (a *App) MaintainSize(ctx context.Context, conn *pgx.Conn, targetSize int64) (int64, error) {
	// Get current size of orders and order_line tables
	var currentSize int64
	err := conn.QueryRow(ctx, `
        SELECT COALESCE(pg_total_relation_size('orders'), 0) +
               COALESCE(pg_total_relation_size('order_line'), 0) +
               COALESCE(pg_total_relation_size('new_orders'), 0)
    `).Scan(&currentSize)
	if err != nil {
		return 0, fmt.Errorf("failed to get current size: %w", err)
	}

	// Calculate threshold (110% of target)
	threshold := int64(float64(targetSize) * 1.10)

	// If under threshold, nothing to do
	if currentSize <= threshold {
		logging.Debug().
			Int64("current_size", currentSize).
			Int64("target_size", targetSize).
			Int64("threshold", threshold).
			Msg("Size within bounds, no cleanup needed")
		return 0, nil
	}

	logging.Info().
		Int64("current_size", currentSize).
		Int64("target_size", targetSize).
		Int64("threshold", threshold).
		Msg("Size exceeds threshold, starting cleanup")

	// Calculate how much we need to delete to get back to target
	excess := currentSize - targetSize

	// Estimate average order size (orders + order_lines)
	// Average order has ~10 order_lines, each row is ~100-200 bytes
	// Conservative estimate: ~1500 bytes per order (including order_lines)
	avgOrderSize := int64(1500)
	ordersToDelete := excess / avgOrderSize
	if ordersToDelete < 100 {
		ordersToDelete = 100 // Minimum batch
	}
	if ordersToDelete > 10000 {
		ordersToDelete = 10000 // Maximum batch per cleanup cycle
	}

	// Delete oldest orders in batches
	var totalDeleted int64
	batchSize := int64(1000)

	for totalDeleted < ordersToDelete {
		remaining := ordersToDelete - totalDeleted
		if remaining > batchSize {
			remaining = batchSize
		}

		deleted, err := a.deleteOldestOrders(ctx, conn, remaining)
		if err != nil {
			if totalDeleted > 0 {
				// Partial success
				logging.Warn().
					Err(err).
					Int64("deleted_so_far", totalDeleted).
					Msg("Cleanup partially completed with error")
				return totalDeleted, nil
			}
			return 0, fmt.Errorf("failed to delete orders: %w", err)
		}

		totalDeleted += deleted

		// If we deleted fewer than requested, we've run out of orders
		if deleted < remaining {
			break
		}
	}

	return totalDeleted, nil
}

// deleteOldestOrders deletes the oldest orders and their related records.
func (a *App) deleteOldestOrders(ctx context.Context, conn *pgx.Conn, limit int64) (int64, error) {
	// Start transaction for atomic delete
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Find the oldest orders to delete
	// We need to select by the composite primary key (o_w_id, o_d_id, o_id)
	rows, err := tx.Query(ctx, `
        SELECT o_w_id, o_d_id, o_id
        FROM orders
        ORDER BY o_entry_d ASC
        LIMIT $1
    `, limit)
	if err != nil {
		return 0, fmt.Errorf("failed to select orders to delete: %w", err)
	}

	type orderKey struct {
		wID int
		dID int
		oID int
	}
	var ordersToDelete []orderKey

	for rows.Next() {
		var k orderKey
		if err := rows.Scan(&k.wID, &k.dID, &k.oID); err != nil {
			rows.Close()
			return 0, fmt.Errorf("failed to scan order key: %w", err)
		}
		ordersToDelete = append(ordersToDelete, k)
	}
	rows.Close()

	if len(ordersToDelete) == 0 {
		return 0, nil
	}

	// Delete in correct order due to foreign key constraints:
	// 1. order_line (references orders)
	// 2. new_orders (references orders)
	// 3. orders

	for _, k := range ordersToDelete {
		// Delete order_lines
		_, err = tx.Exec(ctx, `
            DELETE FROM order_line
            WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3
        `, k.wID, k.dID, k.oID)
		if err != nil {
			return 0, fmt.Errorf("failed to delete order_lines: %w", err)
		}

		// Delete from new_orders (may not exist if already delivered)
		_, err = tx.Exec(ctx, `
            DELETE FROM new_orders
            WHERE no_w_id = $1 AND no_d_id = $2 AND no_o_id = $3
        `, k.wID, k.dID, k.oID)
		if err != nil {
			return 0, fmt.Errorf("failed to delete new_orders: %w", err)
		}

		// Delete order
		_, err = tx.Exec(ctx, `
            DELETE FROM orders
            WHERE o_w_id = $1 AND o_d_id = $2 AND o_id = $3
        `, k.wID, k.dID, k.oID)
		if err != nil {
			return 0, fmt.Errorf("failed to delete order: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit cleanup transaction: %w", err)
	}

	return int64(len(ordersToDelete)), nil
}
