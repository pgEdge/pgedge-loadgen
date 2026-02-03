//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

package retail

import (
	"context"
	"time"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen"
)

// Query weights - TPC-DS style decision support queries
var queryWeights = map[string]int{
	"store_sales_by_date":     15,
	"store_sales_by_item":     12,
	"store_sales_by_customer": 10,
	"web_sales_analysis":      12,
	"catalog_sales_analysis":  10,
	"cross_channel_sales":     8,
	"customer_demographics":   8,
	"promotion_effect":        7,
	"inventory_analysis":      6,
	"store_comparison":        6,
	"time_series_sales":       6,
}

// QueryExecutor executes retail queries.
type QueryExecutor struct {
	faker        *datagen.Faker
	numItems     int
	numCustomers int
	numStores    int
}

// NewQueryExecutor creates a new query executor.
func NewQueryExecutor(numItems, numCustomers, numStores int) *QueryExecutor {
	return &QueryExecutor{
		faker:        datagen.NewFaker(),
		numItems:     max(1, numItems),
		numCustomers: max(1, numCustomers),
		numStores:    max(1, numStores),
	}
}

// ExecuteRandomQuery executes a random analytical query based on weights.
func (e *QueryExecutor) ExecuteRandomQuery(ctx context.Context, db apps.DB) apps.QueryResult {
	queryType := e.selectQueryType()

	start := time.Now()
	var err error
	var rowsAffected int64

	switch queryType {
	case "store_sales_by_date":
		rowsAffected, err = e.executeStoreSalesByDate(ctx, db)
	case "store_sales_by_item":
		rowsAffected, err = e.executeStoreSalesByItem(ctx, db)
	case "store_sales_by_customer":
		rowsAffected, err = e.executeStoreSalesByCustomer(ctx, db)
	case "web_sales_analysis":
		rowsAffected, err = e.executeWebSalesAnalysis(ctx, db)
	case "catalog_sales_analysis":
		rowsAffected, err = e.executeCatalogSalesAnalysis(ctx, db)
	case "cross_channel_sales":
		rowsAffected, err = e.executeCrossChannelSales(ctx, db)
	case "customer_demographics":
		rowsAffected, err = e.executeCustomerDemographics(ctx, db)
	case "promotion_effect":
		rowsAffected, err = e.executePromotionEffect(ctx, db)
	case "inventory_analysis":
		rowsAffected, err = e.executeInventoryAnalysis(ctx, db)
	case "store_comparison":
		rowsAffected, err = e.executeStoreComparison(ctx, db)
	case "time_series_sales":
		rowsAffected, err = e.executeTimeSeriesSales(ctx, db)
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

// Store Sales by Date - Aggregate store sales by date dimension
func (e *QueryExecutor) executeStoreSalesByDate(ctx context.Context, db apps.DB) (int64, error) {
	year := e.faker.Int(1998, 2002)

	rows, err := db.Query(ctx, `
        SELECT d.d_year, d.d_moy, d.d_day_name,
               SUM(ss.ss_net_paid) AS total_sales,
               SUM(ss.ss_quantity) AS total_quantity,
               COUNT(*) AS transaction_count
        FROM store_sales ss
        JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        WHERE d.d_year = $1
        GROUP BY d.d_year, d.d_moy, d.d_day_name
        ORDER BY d.d_year, d.d_moy
    `, year)
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

// Store Sales by Item - Top selling items
func (e *QueryExecutor) executeStoreSalesByItem(ctx context.Context, db apps.DB) (int64, error) {
	categoryID := e.faker.Int(1, len(categories))

	rows, err := db.Query(ctx, `
        SELECT i.i_item_id, i.i_product_name, i.i_category, i.i_brand,
               SUM(ss.ss_net_paid) AS total_revenue,
               SUM(ss.ss_quantity) AS units_sold,
               AVG(ss.ss_sales_price) AS avg_price
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        WHERE i.i_category_id = $1
        GROUP BY i.i_item_id, i.i_product_name, i.i_category, i.i_brand
        ORDER BY total_revenue DESC
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

// Store Sales by Customer - Customer purchase patterns
func (e *QueryExecutor) executeStoreSalesByCustomer(ctx context.Context, db apps.DB) (int64, error) {
	state := datagen.Choose(e.faker, usStates)

	rows, err := db.Query(ctx, `
        SELECT c.c_customer_id, c.c_first_name, c.c_last_name,
               ca.ca_state, ca.ca_city,
               SUM(ss.ss_net_paid) AS total_spent,
               COUNT(*) AS purchase_count,
               AVG(ss.ss_net_paid) AS avg_transaction
        FROM store_sales ss
        JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
        JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
        WHERE ca.ca_state = $1
        GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name, ca.ca_state, ca.ca_city
        ORDER BY total_spent DESC
        LIMIT 50
    `, state)
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

// Web Sales Analysis - Web channel performance
func (e *QueryExecutor) executeWebSalesAnalysis(ctx context.Context, db apps.DB) (int64, error) {
	year := e.faker.Int(1998, 2002)

	rows, err := db.Query(ctx, `
        SELECT d.d_year, d.d_quarter_name,
               SUM(ws.ws_net_paid) AS total_revenue,
               SUM(ws.ws_ext_ship_cost) AS shipping_cost,
               SUM(ws.ws_net_profit) AS net_profit,
               COUNT(DISTINCT ws.ws_order_number) AS order_count
        FROM web_sales ws
        JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
        WHERE d.d_year = $1
        GROUP BY d.d_year, d.d_quarter_name
        ORDER BY d.d_quarter_name
    `, year)
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

// Catalog Sales Analysis - Catalog channel performance
func (e *QueryExecutor) executeCatalogSalesAnalysis(ctx context.Context, db apps.DB) (int64, error) {
	year := e.faker.Int(1998, 2002)

	rows, err := db.Query(ctx, `
        SELECT d.d_year, d.d_moy,
               SUM(cs.cs_net_paid) AS total_revenue,
               SUM(cs.cs_ext_ship_cost) AS shipping_cost,
               AVG(cs.cs_sales_price) AS avg_item_price,
               COUNT(DISTINCT cs.cs_order_number) AS order_count
        FROM catalog_sales cs
        JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
        WHERE d.d_year = $1
        GROUP BY d.d_year, d.d_moy
        ORDER BY d.d_moy
    `, year)
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

// Cross Channel Sales - Compare sales across channels
func (e *QueryExecutor) executeCrossChannelSales(ctx context.Context, db apps.DB) (int64, error) {
	year := e.faker.Int(1998, 2002)

	rows, err := db.Query(ctx, `
        SELECT d.d_year, d.d_quarter_name,
               COALESCE(ss.store_revenue, 0) AS store_revenue,
               COALESCE(ws.web_revenue, 0) AS web_revenue,
               COALESCE(cs.catalog_revenue, 0) AS catalog_revenue
        FROM date_dim d
        LEFT JOIN (
            SELECT ss_sold_date_sk, SUM(ss_net_paid) AS store_revenue
            FROM store_sales GROUP BY ss_sold_date_sk
        ) ss ON d.d_date_sk = ss.ss_sold_date_sk
        LEFT JOIN (
            SELECT ws_sold_date_sk, SUM(ws_net_paid) AS web_revenue
            FROM web_sales GROUP BY ws_sold_date_sk
        ) ws ON d.d_date_sk = ws.ws_sold_date_sk
        LEFT JOIN (
            SELECT cs_sold_date_sk, SUM(cs_net_paid) AS catalog_revenue
            FROM catalog_sales GROUP BY cs_sold_date_sk
        ) cs ON d.d_date_sk = cs.cs_sold_date_sk
        WHERE d.d_year = $1
        GROUP BY d.d_year, d.d_quarter_name, ss.store_revenue, ws.web_revenue, cs.catalog_revenue
        ORDER BY d.d_quarter_name
        LIMIT 100
    `, year)
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

// Customer Demographics Analysis
func (e *QueryExecutor) executeCustomerDemographics(ctx context.Context, db apps.DB) (int64, error) {
	rows, err := db.Query(ctx, `
        SELECT cd.cd_gender, cd.cd_marital_status, cd.cd_education_status,
               COUNT(DISTINCT c.c_customer_sk) AS customer_count,
               SUM(ss.ss_net_paid) AS total_spent,
               AVG(ss.ss_net_paid) AS avg_spent
        FROM store_sales ss
        JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
        JOIN customer_demographics cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
        GROUP BY cd.cd_gender, cd.cd_marital_status, cd.cd_education_status
        ORDER BY total_spent DESC
        LIMIT 20
    `)
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

// Promotion Effect Analysis
func (e *QueryExecutor) executePromotionEffect(ctx context.Context, db apps.DB) (int64, error) {
	rows, err := db.Query(ctx, `
        SELECT p.p_promo_name, p.p_channel_dmail, p.p_channel_email, p.p_channel_tv,
               SUM(ss.ss_net_paid) AS total_revenue,
               SUM(ss.ss_coupon_amt) AS total_coupons,
               COUNT(*) AS transaction_count,
               AVG(ss.ss_ext_discount_amt) AS avg_discount
        FROM store_sales ss
        JOIN promotion p ON ss.ss_promo_sk = p.p_promo_sk
        WHERE p.p_discount_active = 'Y'
        GROUP BY p.p_promo_sk, p.p_promo_name, p.p_channel_dmail, p.p_channel_email, p.p_channel_tv
        ORDER BY total_revenue DESC
        LIMIT 20
    `)
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

// Inventory Analysis
func (e *QueryExecutor) executeInventoryAnalysis(ctx context.Context, db apps.DB) (int64, error) {
	warehouseSK := e.faker.Int(1, 5)

	rows, err := db.Query(ctx, `
        SELECT w.w_warehouse_name, i.i_category, i.i_brand,
               SUM(inv.inv_quantity_on_hand) AS total_inventory,
               AVG(inv.inv_quantity_on_hand) AS avg_inventory
        FROM inventory inv
        JOIN item i ON inv.inv_item_sk = i.i_item_sk
        JOIN warehouse w ON inv.inv_warehouse_sk = w.w_warehouse_sk
        WHERE inv.inv_warehouse_sk = $1
        GROUP BY w.w_warehouse_name, i.i_category, i.i_brand
        ORDER BY total_inventory DESC
        LIMIT 50
    `, warehouseSK)
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

// Store Comparison
func (e *QueryExecutor) executeStoreComparison(ctx context.Context, db apps.DB) (int64, error) {
	rows, err := db.Query(ctx, `
        SELECT s.s_store_name, s.s_city, s.s_state,
               SUM(ss.ss_net_paid) AS total_revenue,
               SUM(ss.ss_net_profit) AS total_profit,
               COUNT(*) AS transaction_count,
               AVG(ss.ss_net_paid) AS avg_transaction
        FROM store_sales ss
        JOIN store s ON ss.ss_store_sk = s.s_store_sk
        GROUP BY s.s_store_sk, s.s_store_name, s.s_city, s.s_state
        ORDER BY total_revenue DESC
    `)
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

// Time Series Sales Analysis
func (e *QueryExecutor) executeTimeSeriesSales(ctx context.Context, db apps.DB) (int64, error) {
	rows, err := db.Query(ctx, `
        SELECT d.d_year, d.d_moy, t.t_hour,
               SUM(ss.ss_net_paid) AS hourly_sales,
               COUNT(*) AS transaction_count
        FROM store_sales ss
        JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        JOIN time_dim t ON ss.ss_sold_time_sk = t.t_time_sk
        GROUP BY d.d_year, d.d_moy, t.t_hour
        ORDER BY d.d_year, d.d_moy, t.t_hour
        LIMIT 100
    `)
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
