//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

package brokerage

import (
	"context"
	"fmt"
	"time"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen"
)

// Query weights based on TPC-E specification
var queryWeights = map[string]int{
	"broker_volume":     5,
	"customer_position": 13,
	"market_feed":       1,
	"market_watch":      18,
	"security_detail":   14,
	"trade_lookup":      8,
	"trade_order":       10,
	"trade_result":      10,
	"trade_status":      19,
	"trade_update":      2,
}

// QueryExecutor executes brokerage queries.
type QueryExecutor struct {
	faker         *datagen.Faker
	numCustomers  int
	numAccounts   int
	numSecurities int
	numTrades     int
	numBrokers    int
}

// NewQueryExecutor creates a new query executor.
func NewQueryExecutor(numCustomers, numAccounts, numSecurities, numTrades, numBrokers int) *QueryExecutor {
	return &QueryExecutor{
		faker:         datagen.NewFaker(),
		numCustomers:  max(1, numCustomers),
		numAccounts:   max(1, numAccounts),
		numSecurities: max(1, numSecurities),
		numTrades:     max(1, numTrades),
		numBrokers:    max(1, numBrokers),
	}
}

// ExecuteRandomQuery executes a random query based on the TPC-E weights.
func (e *QueryExecutor) ExecuteRandomQuery(ctx context.Context, db apps.DB) apps.QueryResult {
	queryType := e.selectQueryType()

	start := time.Now()
	var err error
	var rowsAffected int64

	switch queryType {
	case "broker_volume":
		rowsAffected, err = e.executeBrokerVolume(ctx, db)
	case "customer_position":
		rowsAffected, err = e.executeCustomerPosition(ctx, db)
	case "market_feed":
		rowsAffected, err = e.executeMarketFeed(ctx, db)
	case "market_watch":
		rowsAffected, err = e.executeMarketWatch(ctx, db)
	case "security_detail":
		rowsAffected, err = e.executeSecurityDetail(ctx, db)
	case "trade_lookup":
		rowsAffected, err = e.executeTradeLookup(ctx, db)
	case "trade_order":
		rowsAffected, err = e.executeTradeOrder(ctx, db)
	case "trade_result":
		rowsAffected, err = e.executeTradeResult(ctx, db)
	case "trade_status":
		rowsAffected, err = e.executeTradeStatus(ctx, db)
	case "trade_update":
		rowsAffected, err = e.executeTradeUpdate(ctx, db)
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

// Broker Volume - Calculate total trade volume per broker
func (e *QueryExecutor) executeBrokerVolume(ctx context.Context, db apps.DB) (int64, error) {
	sectorID := datagen.Choose(e.faker, []string{"EN", "MT", "IN", "CD", "CS", "HC", "FN", "IT", "TS", "UT", "RE"})

	rows, err := db.Query(ctx, `
        SELECT b.b_name, SUM(t.t_qty * t.t_trade_price) AS volume
        FROM broker b
        JOIN customer_account ca ON b.b_id = ca.ca_b_id
        JOIN trade t ON ca.ca_id = t.t_ca_id
        JOIN security s ON t.t_s_symb = s.s_symb
        JOIN company co ON s.s_co_id = co.co_id
        JOIN industry i ON co.co_in_id = i.in_id
        WHERE i.in_sc_id = $1
            AND t.t_st_id = 'CMPT'
        GROUP BY b.b_id, b.b_name
        ORDER BY volume DESC
        LIMIT 10
    `, sectorID)
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

// Customer Position - Get customer portfolio information
func (e *QueryExecutor) executeCustomerPosition(ctx context.Context, db apps.DB) (int64, error) {
	customerID := e.faker.Int(1, e.numCustomers)

	// Get customer info and accounts
	rows, err := db.Query(ctx, `
        SELECT c.c_l_name, c.c_f_name, ca.ca_id, ca.ca_name, ca.ca_bal,
               COALESCE(SUM(hs.hs_qty * lt.lt_price), 0) AS assets
        FROM customer c
        JOIN customer_account ca ON c.c_id = ca.ca_c_id
        LEFT JOIN holding_summary hs ON ca.ca_id = hs.hs_ca_id
        LEFT JOIN last_trade lt ON hs.hs_s_symb = lt.lt_s_symb
        WHERE c.c_id = $1
        GROUP BY c.c_id, c.c_l_name, c.c_f_name, ca.ca_id, ca.ca_name, ca.ca_bal
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

// Market Feed - Update security prices (simulated market data)
func (e *QueryExecutor) executeMarketFeed(ctx context.Context, db apps.DB) (int64, error) {
	// Update a few random securities' last trade info
	numUpdates := e.faker.Int(1, 5)
	var totalRows int64

	for i := 0; i < numUpdates; i++ {
		secIdx := e.faker.Int(1, e.numSecurities)
		symbol := fmt.Sprintf("SYM%06d", secIdx)
		newPrice := e.faker.Float64(5, 500)
		volume := int64(e.faker.Int(1000, 100000))

		result, err := db.Exec(ctx, `
            UPDATE last_trade
            SET lt_price = $2, lt_vol = lt_vol + $3, lt_dts = NOW()
            WHERE lt_s_symb = $1
        `, symbol, newPrice, volume)
		if err != nil {
			return totalRows, err
		}
		totalRows += result.RowsAffected()
	}
	return totalRows, nil
}

// Market Watch - Check prices of watched securities
func (e *QueryExecutor) executeMarketWatch(ctx context.Context, db apps.DB) (int64, error) {
	customerID := e.faker.Int(1, e.numCustomers)

	rows, err := db.Query(ctx, `
        SELECT s.s_symb, s.s_name, lt.lt_price, lt.lt_open_price,
               (lt.lt_price - lt.lt_open_price) AS change,
               CASE WHEN lt.lt_open_price > 0
                    THEN (lt.lt_price - lt.lt_open_price) / lt.lt_open_price * 100
                    ELSE 0 END AS pct_change
        FROM watch_list wl
        JOIN watch_item wi ON wl.wl_id = wi.wi_wl_id
        JOIN security s ON wi.wi_s_symb = s.s_symb
        JOIN last_trade lt ON s.s_symb = lt.lt_s_symb
        WHERE wl.wl_c_id = $1
        ORDER BY ABS(lt.lt_price - lt.lt_open_price) DESC
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

// Security Detail - Get detailed security information
func (e *QueryExecutor) executeSecurityDetail(ctx context.Context, db apps.DB) (int64, error) {
	secIdx := e.faker.Int(1, e.numSecurities)
	symbol := fmt.Sprintf("SYM%06d", secIdx)

	var sName, coName, exName string
	var pe, high52, low52, dividend, yield float64
	var numOut int64

	err := db.QueryRow(ctx, `
        SELECT s.s_name, s.s_pe, s.s_52wk_high, s.s_52wk_low,
               s.s_dividend, s.s_yield, s.s_num_out,
               co.co_name, e.ex_name
        FROM security s
        JOIN company co ON s.s_co_id = co.co_id
        JOIN exchange e ON s.s_ex_id = e.ex_id
        WHERE s.s_symb = $1
    `, symbol).Scan(&sName, &pe, &high52, &low52, &dividend, &yield, &numOut, &coName, &exName)
	if err != nil {
		return 0, err
	}

	// Also get recent trades for this security
	rows, err := db.Query(ctx, `
        SELECT t.t_dts, t.t_trade_price, t.t_qty
        FROM trade t
        WHERE t.t_s_symb = $1 AND t.t_st_id = 'CMPT'
        ORDER BY t.t_dts DESC
        LIMIT 10
    `, symbol)
	if err != nil {
		return 1, err
	}
	defer rows.Close()

	var count int64 = 1
	for rows.Next() {
		count++
	}
	return count, rows.Err()
}

// Trade Lookup - Find trades by various criteria
func (e *QueryExecutor) executeTradeLookup(ctx context.Context, db apps.DB) (int64, error) {
	accountID := e.faker.Int(1, e.numAccounts)
	startDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Add(
		time.Duration(e.faker.Int(0, 365*3)) * 24 * time.Hour)
	endDate := startDate.Add(30 * 24 * time.Hour)

	rows, err := db.Query(ctx, `
        SELECT t.t_id, t.t_dts, t.t_s_symb, t.t_qty, t.t_trade_price,
               t.t_chrg, t.t_comm, tt.tt_name, st.st_name
        FROM trade t
        JOIN trade_type tt ON t.t_tt_id = tt.tt_id
        JOIN status_type st ON t.t_st_id = st.st_id
        WHERE t.t_ca_id = $1
            AND t.t_dts >= $2
            AND t.t_dts < $3
        ORDER BY t.t_dts DESC
        LIMIT 20
    `, accountID, startDate, endDate)
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

// Trade Order - Place a new trade order
func (e *QueryExecutor) executeTradeOrder(ctx context.Context, db apps.DB) (int64, error) {
	accountID := e.faker.Int(1, e.numAccounts)
	secIdx := e.faker.Int(1, e.numSecurities)
	symbol := fmt.Sprintf("SYM%06d", secIdx)
	ttID := datagen.Choose(e.faker, []string{"TMB", "TMS", "TLB", "TLS"})
	qty := e.faker.Int(100, 1000)
	isCash := e.faker.Int(0, 1) == 1

	// Get current price
	var currentPrice float64
	err := db.QueryRow(ctx, `SELECT lt_price FROM last_trade WHERE lt_s_symb = $1`, symbol).Scan(&currentPrice)
	if err != nil {
		return 0, err
	}

	// Insert trade with retry logic for race conditions on ID generation
	var newTradeID int64
	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		err = db.QueryRow(ctx, `
            INSERT INTO trade (t_id, t_dts, t_st_id, t_tt_id, t_is_cash, t_s_symb,
                              t_qty, t_bid_price, t_ca_id, t_exec_name, t_trade_price,
                              t_chrg, t_comm, t_tax, t_lifo)
            VALUES ((SELECT COALESCE(MAX(t_id), 0) + 1 FROM trade), NOW(), 'PNDG', $1, $2, $3, $4, $5, $6, 'System', NULL, 0, 0, 0, TRUE)
            RETURNING t_id
        `, ttID, isCash, symbol, qty, currentPrice, accountID).Scan(&newTradeID)
		if err == nil {
			break
		}
		// Check if it's a duplicate key error, if so retry
		if attempt < maxRetries-1 {
			continue
		}
		return 0, err
	}

	// Insert trade history
	_, err = db.Exec(ctx, `
        INSERT INTO trade_history (th_t_id, th_dts, th_st_id)
        VALUES ($1, NOW(), 'SBMT')
        ON CONFLICT (th_t_id, th_st_id) DO NOTHING
    `, newTradeID)
	if err != nil {
		return 0, err
	}

	return 1, nil
}

// Trade Result - Process completed trade
func (e *QueryExecutor) executeTradeResult(ctx context.Context, db apps.DB) (int64, error) {
	// Use transaction with FOR UPDATE SKIP LOCKED to prevent race conditions
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	// Find a pending trade to complete (lock it to prevent other workers)
	var tradeID int64
	var symbol string
	var qty int
	var accountID int
	var bidPrice float64

	err = tx.QueryRow(ctx, `
        SELECT t_id, t_s_symb, t_qty, t_ca_id, t_bid_price
        FROM trade
        WHERE t_st_id = 'PNDG'
        ORDER BY t_dts
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    `).Scan(&tradeID, &symbol, &qty, &accountID, &bidPrice)
	if err != nil {
		// No pending trades available - that's okay
		return 0, nil
	}

	// Get current price
	var currentPrice float64
	err = tx.QueryRow(ctx, `SELECT lt_price FROM last_trade WHERE lt_s_symb = $1`, symbol).Scan(&currentPrice)
	if err != nil {
		return 0, err
	}

	// Update trade to completed
	_, err = tx.Exec(ctx, `
        UPDATE trade
        SET t_st_id = 'CMPT', t_trade_price = $2, t_chrg = $3, t_comm = $4
        WHERE t_id = $1
    `, tradeID, currentPrice, e.faker.Float64(0, 20), e.faker.Float64(0, 50))
	if err != nil {
		return 0, err
	}

	// Add trade history (use ON CONFLICT to handle edge cases)
	_, err = tx.Exec(ctx, `
        INSERT INTO trade_history (th_t_id, th_dts, th_st_id)
        VALUES ($1, NOW(), 'CMPT')
        ON CONFLICT (th_t_id, th_st_id) DO NOTHING
    `, tradeID)
	if err != nil {
		return 0, err
	}

	// Add settlement (use ON CONFLICT to handle edge cases)
	_, err = tx.Exec(ctx, `
        INSERT INTO settlement (se_t_id, se_cash_type, se_cash_due_date, se_amt)
        VALUES ($1, 'Cash Account', NOW() + INTERVAL '3 days', $2)
        ON CONFLICT (se_t_id) DO NOTHING
    `, tradeID, float64(qty)*currentPrice)
	if err != nil {
		return 0, err
	}

	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}

	return 1, nil
}

// Trade Status - Check status of recent trades
func (e *QueryExecutor) executeTradeStatus(ctx context.Context, db apps.DB) (int64, error) {
	accountID := e.faker.Int(1, e.numAccounts)

	rows, err := db.Query(ctx, `
        SELECT t.t_id, t.t_dts, t.t_s_symb, t.t_qty, t.t_bid_price,
               t.t_trade_price, st.st_name, tt.tt_name
        FROM trade t
        JOIN status_type st ON t.t_st_id = st.st_id
        JOIN trade_type tt ON t.t_tt_id = tt.tt_id
        WHERE t.t_ca_id = $1
        ORDER BY t.t_dts DESC
        LIMIT 50
    `, accountID)
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

// Trade Update - Modify an existing pending trade
func (e *QueryExecutor) executeTradeUpdate(ctx context.Context, db apps.DB) (int64, error) {
	accountID := e.faker.Int(1, e.numAccounts)

	// Find a pending trade to update
	var tradeID int64
	err := db.QueryRow(ctx, `
        SELECT t_id FROM trade
        WHERE t_ca_id = $1 AND t_st_id = 'PNDG'
        ORDER BY t_dts DESC
        LIMIT 1
    `, accountID).Scan(&tradeID)
	if err != nil {
		// No pending trades to update
		return 0, nil
	}

	// Update the trade's execution name
	newExecName := e.faker.Name()
	if len(newExecName) > 64 {
		newExecName = newExecName[:64]
	}

	result, err := db.Exec(ctx, `
        UPDATE trade SET t_exec_name = $2
        WHERE t_id = $1 AND t_st_id = 'PNDG'
    `, tradeID, newExecName)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected(), nil
}
