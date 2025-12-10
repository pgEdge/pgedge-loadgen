package analytics

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/apps"
	"github.com/pgEdge/pgedge-loadgen/internal/datagen"
)

// Query weights - TPC-H queries with realistic OLAP distribution
// Weights adjusted based on query execution time to prevent resource contention
var queryWeights = map[string]int{
	"pricing_summary":       0,  // Q1 - Disabled: Heavy aggregation (~7s) causes timeouts
	"min_cost_supplier":     5,  // Q2
	"shipping_priority":     8,  // Q3
	"order_priority":        6,  // Q4
	"local_supplier_volume": 5,  // Q5
	"revenue_forecast":      10, // Q6 - Simple, fast
	"volume_shipping":       4,  // Q7
	"market_share":          2,  // Q8 - Complex join (~2.3s), reduced weight
	"product_profit":        5,  // Q9
	"returned_items":        6,  // Q10
	"important_stock":       4,  // Q11
	"shipping_modes":        4,  // Q12 - Reduced weight (~2.2s)
	"customer_distribution": 5,  // Q13
	"promotion_effect":      6,  // Q14
	"top_supplier":          4,  // Q15
	"parts_supplier":        3,  // Q16
	"small_quantity":        0,  // Q17 - Disabled: correlated subquery is too slow
	"large_volume":          0,  // Q18 - Disabled: Very slow (~3.7s) causes timeouts
	"discounted_revenue":    2,  // Q19 - Complex conditionals (~2.3s)
}

// QueryExecutor executes analytics queries.
type QueryExecutor struct {
	faker        *datagen.Faker
	numSuppliers int
	numParts     int
	numCustomers int
	numOrders    int
}

// NewQueryExecutor creates a new query executor.
func NewQueryExecutor(numSuppliers, numParts, numCustomers, numOrders int) *QueryExecutor {
	return &QueryExecutor{
		faker:        datagen.NewFaker(),
		numSuppliers: max(1, numSuppliers),
		numParts:     max(1, numParts),
		numCustomers: max(1, numCustomers),
		numOrders:    max(1, numOrders),
	}
}

// ExecuteRandomQuery executes a random analytical query based on weights.
func (e *QueryExecutor) ExecuteRandomQuery(ctx context.Context, pool *pgxpool.Pool) apps.QueryResult {
	queryType := e.selectQueryType()

	start := time.Now()
	var err error
	var rowsAffected int64

	switch queryType {
	case "pricing_summary":
		rowsAffected, err = e.executePricingSummary(ctx, pool)
	case "min_cost_supplier":
		rowsAffected, err = e.executeMinCostSupplier(ctx, pool)
	case "shipping_priority":
		rowsAffected, err = e.executeShippingPriority(ctx, pool)
	case "order_priority":
		rowsAffected, err = e.executeOrderPriority(ctx, pool)
	case "local_supplier_volume":
		rowsAffected, err = e.executeLocalSupplierVolume(ctx, pool)
	case "revenue_forecast":
		rowsAffected, err = e.executeRevenueForecast(ctx, pool)
	case "volume_shipping":
		rowsAffected, err = e.executeVolumeShipping(ctx, pool)
	case "market_share":
		rowsAffected, err = e.executeMarketShare(ctx, pool)
	case "product_profit":
		rowsAffected, err = e.executeProductProfit(ctx, pool)
	case "returned_items":
		rowsAffected, err = e.executeReturnedItems(ctx, pool)
	case "important_stock":
		rowsAffected, err = e.executeImportantStock(ctx, pool)
	case "shipping_modes":
		rowsAffected, err = e.executeShippingModes(ctx, pool)
	case "customer_distribution":
		rowsAffected, err = e.executeCustomerDistribution(ctx, pool)
	case "promotion_effect":
		rowsAffected, err = e.executePromotionEffect(ctx, pool)
	case "top_supplier":
		rowsAffected, err = e.executeTopSupplier(ctx, pool)
	case "parts_supplier":
		rowsAffected, err = e.executePartsSupplier(ctx, pool)
	case "small_quantity":
		rowsAffected, err = e.executeSmallQuantity(ctx, pool)
	case "large_volume":
		rowsAffected, err = e.executeLargeVolume(ctx, pool)
	case "discounted_revenue":
		rowsAffected, err = e.executeDiscountedRevenue(ctx, pool)
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

// Q1: Pricing Summary Report - aggregates lineitem data
func (e *QueryExecutor) executePricingSummary(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	// Random date range within valid period
	deltaDays := e.faker.Int(60, 120)

	rows, err := pool.Query(ctx, `
        SELECT
            l_returnflag,
            l_linestatus,
            SUM(l_quantity) AS sum_qty,
            SUM(l_extendedprice) AS sum_base_price,
            SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
            AVG(l_quantity) AS avg_qty,
            AVG(l_extendedprice) AS avg_price,
            AVG(l_discount) AS avg_disc,
            COUNT(*) AS count_order
        FROM lineitem
        WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '1 day' * $1
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    `, deltaDays)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		count++
		var rf, ls string
		var sumQty, sumBase, sumDisc, sumCharge, avgQty, avgPrice, avgDisc float64
		var cntOrder int64
		if err := rows.Scan(&rf, &ls, &sumQty, &sumBase, &sumDisc, &sumCharge,
			&avgQty, &avgPrice, &avgDisc, &cntOrder); err != nil {
			return count, err
		}
	}
	return count, rows.Err()
}

// Q2: Minimum Cost Supplier Query
func (e *QueryExecutor) executeMinCostSupplier(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	size := e.faker.Int(1, 50)
	ptype := datagen.Choose(e.faker, typeMats)
	regionKey := e.faker.Int(0, 4)

	rows, err := pool.Query(ctx, `
        SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
        FROM part, supplier, partsupp, nation, region
        WHERE p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND p_size = $1
            AND p_type LIKE '%' || $2
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_regionkey = $3
            AND ps_supplycost = (
                SELECT MIN(ps_supplycost)
                FROM partsupp, supplier, nation, region
                WHERE p_partkey = ps_partkey
                    AND s_suppkey = ps_suppkey
                    AND s_nationkey = n_nationkey
                    AND n_regionkey = r_regionkey
                    AND r_regionkey = $3
            )
        ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
        LIMIT 100
    `, size, ptype, regionKey)
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

// Q3: Shipping Priority Query
func (e *QueryExecutor) executeShippingPriority(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	segment := datagen.Choose(e.faker, segments)

	rows, err := pool.Query(ctx, `
        SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
               o_orderdate, o_shippriority
        FROM customer, orders, lineitem
        WHERE c_mktsegment = $1
            AND c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND o_orderdate < DATE '1995-03-15'
            AND l_shipdate > DATE '1995-03-15'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC, o_orderdate
        LIMIT 10
    `, segment)
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

// Q4: Order Priority Checking Query
func (e *QueryExecutor) executeOrderPriority(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	// Pick a random quarter in the data range
	year := e.faker.Int(1993, 1997)
	month := (e.faker.Int(0, 3) * 3) + 1

	rows, err := pool.Query(ctx, `
        SELECT o_orderpriority, COUNT(*) AS order_count
        FROM orders
        WHERE o_orderdate >= DATE '1993-01-01' + INTERVAL '1 year' * $1 + INTERVAL '1 month' * $2
            AND o_orderdate < DATE '1993-01-01' + INTERVAL '1 year' * $1 + INTERVAL '1 month' * ($2 + 3)
            AND EXISTS (
                SELECT * FROM lineitem
                WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
            )
        GROUP BY o_orderpriority
        ORDER BY o_orderpriority
    `, year-1993, month-1)
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

// Q5: Local Supplier Volume Query
func (e *QueryExecutor) executeLocalSupplierVolume(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	regionKey := e.faker.Int(0, 4)
	year := e.faker.Int(1993, 1997)

	rows, err := pool.Query(ctx, `
        SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM customer, orders, lineitem, supplier, nation, region
        WHERE c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND l_suppkey = s_suppkey
            AND c_nationkey = s_nationkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_regionkey = $1
            AND o_orderdate >= DATE '1993-01-01' + INTERVAL '1 year' * $2
            AND o_orderdate < DATE '1994-01-01' + INTERVAL '1 year' * $2
        GROUP BY n_name
        ORDER BY revenue DESC
    `, regionKey, year-1993)
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

// Q6: Forecasting Revenue Change Query
func (e *QueryExecutor) executeRevenueForecast(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	year := e.faker.Int(1993, 1997)
	discount := float64(e.faker.Int(2, 9)) / 100.0
	quantity := e.faker.Int(24, 25)

	var revenue float64
	err := pool.QueryRow(ctx, `
        SELECT COALESCE(SUM(l_extendedprice * l_discount), 0) AS revenue
        FROM lineitem
        WHERE l_shipdate >= DATE '1993-01-01' + INTERVAL '1 year' * $1
            AND l_shipdate < DATE '1994-01-01' + INTERVAL '1 year' * $1
            AND l_discount BETWEEN $2 - 0.01 AND $2 + 0.01
            AND l_quantity < $3
    `, year-1993, discount, quantity).Scan(&revenue)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// Q7: Volume Shipping Query
func (e *QueryExecutor) executeVolumeShipping(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	nation1 := e.faker.Int(0, 24)
	nation2 := (nation1 + e.faker.Int(1, 24)) % 25

	rows, err := pool.Query(ctx, `
        SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue
        FROM (
            SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation,
                   EXTRACT(YEAR FROM l_shipdate) AS l_year,
                   l_extendedprice * (1 - l_discount) AS volume
            FROM supplier, lineitem, orders, customer, nation n1, nation n2
            WHERE s_suppkey = l_suppkey
                AND o_orderkey = l_orderkey
                AND c_custkey = o_custkey
                AND s_nationkey = n1.n_nationkey
                AND c_nationkey = n2.n_nationkey
                AND ((n1.n_nationkey = $1 AND n2.n_nationkey = $2)
                    OR (n1.n_nationkey = $2 AND n2.n_nationkey = $1))
                AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        ) AS shipping
        GROUP BY supp_nation, cust_nation, l_year
        ORDER BY supp_nation, cust_nation, l_year
    `, nation1, nation2)
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

// Q8: National Market Share Query
func (e *QueryExecutor) executeMarketShare(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	nationKey := e.faker.Int(0, 24)
	ptype := datagen.Choose(e.faker, types)

	rows, err := pool.Query(ctx, `
        SELECT o_year, COALESCE(SUM(CASE WHEN s_nationkey = $1 THEN volume ELSE 0 END) / NULLIF(SUM(volume), 0), 0) AS mkt_share
        FROM (
            SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year,
                   l_extendedprice * (1 - l_discount) AS volume,
                   s_nationkey
            FROM part, supplier, lineitem, orders, customer, nation, region
            WHERE p_partkey = l_partkey
                AND s_suppkey = l_suppkey
                AND l_orderkey = o_orderkey
                AND o_custkey = c_custkey
                AND c_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND p_type LIKE $2 || '%'
                AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        ) AS all_nations
        GROUP BY o_year
        ORDER BY o_year
    `, nationKey, ptype)
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

// Q9: Product Type Profit Measure Query
func (e *QueryExecutor) executeProductProfit(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	color := datagen.Choose(e.faker, []string{"green", "blue", "red", "yellow", "white"})

	rows, err := pool.Query(ctx, `
        SELECT nation, o_year, SUM(amount) AS sum_profit
        FROM (
            SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year,
                   l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
            FROM part, supplier, lineitem, partsupp, orders, nation
            WHERE s_suppkey = l_suppkey
                AND ps_suppkey = l_suppkey
                AND ps_partkey = l_partkey
                AND p_partkey = l_partkey
                AND o_orderkey = l_orderkey
                AND s_nationkey = n_nationkey
                AND p_name LIKE '%' || $1 || '%'
        ) AS profit
        GROUP BY nation, o_year
        ORDER BY nation, o_year DESC
    `, color)
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

// Q10: Returned Item Reporting Query
func (e *QueryExecutor) executeReturnedItems(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	year := e.faker.Int(1993, 1995)
	quarter := e.faker.Int(0, 3)

	rows, err := pool.Query(ctx, `
        SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
               c_acctbal, n_name, c_address, c_phone, c_comment
        FROM customer, orders, lineitem, nation
        WHERE c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND o_orderdate >= DATE '1993-01-01' + INTERVAL '1 year' * $1 + INTERVAL '3 months' * $2
            AND o_orderdate < DATE '1993-04-01' + INTERVAL '1 year' * $1 + INTERVAL '3 months' * $2
            AND l_returnflag = 'R'
            AND c_nationkey = n_nationkey
        GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        ORDER BY revenue DESC
        LIMIT 20
    `, year-1993, quarter)
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

// Q11: Important Stock Identification Query
func (e *QueryExecutor) executeImportantStock(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	nationKey := e.faker.Int(0, 24)
	fraction := 0.0001

	rows, err := pool.Query(ctx, `
        SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value
        FROM partsupp, supplier, nation
        WHERE ps_suppkey = s_suppkey
            AND s_nationkey = n_nationkey
            AND n_nationkey = $1
        GROUP BY ps_partkey
        HAVING SUM(ps_supplycost * ps_availqty) > (
            SELECT SUM(ps_supplycost * ps_availqty) * $2
            FROM partsupp, supplier, nation
            WHERE ps_suppkey = s_suppkey
                AND s_nationkey = n_nationkey
                AND n_nationkey = $1
        )
        ORDER BY value DESC
    `, nationKey, fraction)
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

// Q12: Shipping Modes and Order Priority Query
func (e *QueryExecutor) executeShippingModes(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	shipmode1 := datagen.Choose(e.faker, shipModes)
	shipmode2 := datagen.Choose(e.faker, shipModes)
	year := e.faker.Int(1993, 1997)

	rows, err := pool.Query(ctx, `
        SELECT l_shipmode,
               SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
                        THEN 1 ELSE 0 END) AS high_line_count,
               SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
                        THEN 1 ELSE 0 END) AS low_line_count
        FROM orders, lineitem
        WHERE o_orderkey = l_orderkey
            AND l_shipmode IN ($1, $2)
            AND l_commitdate < l_receiptdate
            AND l_shipdate < l_commitdate
            AND l_receiptdate >= DATE '1993-01-01' + INTERVAL '1 year' * $3
            AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1 year' * $3
        GROUP BY l_shipmode
        ORDER BY l_shipmode
    `, shipmode1, shipmode2, year-1993)
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

// Q13: Customer Distribution Query
func (e *QueryExecutor) executeCustomerDistribution(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	word1 := datagen.Choose(e.faker, []string{"special", "pending", "unusual", "express"})
	word2 := datagen.Choose(e.faker, []string{"requests", "packages", "accounts", "deposits"})

	rows, err := pool.Query(ctx, `
        SELECT c_count, COUNT(*) AS custdist
        FROM (
            SELECT c_custkey, COUNT(o_orderkey) AS c_count
            FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey
                AND o_comment NOT LIKE '%' || $1 || '%' || $2 || '%'
            GROUP BY c_custkey
        ) AS c_orders
        GROUP BY c_count
        ORDER BY custdist DESC, c_count DESC
    `, word1, word2)
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

// Q14: Promotion Effect Query
func (e *QueryExecutor) executePromotionEffect(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	year := e.faker.Int(1993, 1997)
	month := e.faker.Int(1, 12)

	var promoRevenue float64
	err := pool.QueryRow(ctx, `
        SELECT COALESCE(100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                                 THEN l_extendedprice * (1 - l_discount)
                                 ELSE 0 END) / NULLIF(SUM(l_extendedprice * (1 - l_discount)), 0), 0) AS promo_revenue
        FROM lineitem, part
        WHERE l_partkey = p_partkey
            AND l_shipdate >= DATE '1993-01-01' + INTERVAL '1 year' * $1 + INTERVAL '1 month' * $2
            AND l_shipdate < DATE '1993-02-01' + INTERVAL '1 year' * $1 + INTERVAL '1 month' * $2
    `, year-1993, month-1).Scan(&promoRevenue)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// Q15: Top Supplier Query
func (e *QueryExecutor) executeTopSupplier(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	year := e.faker.Int(1993, 1997)
	quarter := e.faker.Int(0, 3)

	rows, err := pool.Query(ctx, `
        WITH revenue AS (
            SELECT l_suppkey AS supplier_no,
                   SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
            FROM lineitem
            WHERE l_shipdate >= DATE '1993-01-01' + INTERVAL '1 year' * $1 + INTERVAL '3 months' * $2
                AND l_shipdate < DATE '1993-04-01' + INTERVAL '1 year' * $1 + INTERVAL '3 months' * $2
            GROUP BY l_suppkey
        )
        SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
        FROM supplier, revenue
        WHERE s_suppkey = supplier_no
            AND total_revenue = (SELECT MAX(total_revenue) FROM revenue)
        ORDER BY s_suppkey
    `, year-1993, quarter)
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

// Q16: Parts/Supplier Relationship Query
func (e *QueryExecutor) executePartsSupplier(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	brand := fmt.Sprintf("Brand#%d", e.faker.Int(1, 5)*10+e.faker.Int(1, 5))
	ptype := datagen.Choose(e.faker, types)

	rows, err := pool.Query(ctx, `
        SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt
        FROM partsupp, part
        WHERE p_partkey = ps_partkey
            AND p_brand <> $1
            AND p_type NOT LIKE $2 || '%'
            AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
            AND ps_suppkey NOT IN (
                SELECT s_suppkey FROM supplier
                WHERE s_comment LIKE '%Customer%Complaints%'
            )
        GROUP BY p_brand, p_type, p_size
        ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
    `, brand, ptype)
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

// Q17: Small-Quantity-Order Revenue Query
func (e *QueryExecutor) executeSmallQuantity(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	brand := fmt.Sprintf("Brand#%d", e.faker.Int(1, 5)*10+e.faker.Int(1, 5))
	container := datagen.Choose(e.faker, containers)

	var avgYearly float64
	err := pool.QueryRow(ctx, `
        SELECT COALESCE(SUM(l_extendedprice) / 7.0, 0) AS avg_yearly
        FROM lineitem, part
        WHERE p_partkey = l_partkey
            AND p_brand = $1
            AND p_container = $2
            AND l_quantity < (
                SELECT 0.2 * AVG(l_quantity)
                FROM lineitem li2
                WHERE li2.l_partkey = part.p_partkey
            )
    `, brand, container).Scan(&avgYearly)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// Q18: Large Volume Customer Query
func (e *QueryExecutor) executeLargeVolume(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	quantity := e.faker.Int(312, 315)

	rows, err := pool.Query(ctx, `
        SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity)
        FROM customer, orders, lineitem
        WHERE o_orderkey IN (
                SELECT l_orderkey FROM lineitem
                GROUP BY l_orderkey
                HAVING SUM(l_quantity) > $1
            )
            AND c_custkey = o_custkey
            AND o_orderkey = l_orderkey
        GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        ORDER BY o_totalprice DESC, o_orderdate
        LIMIT 100
    `, quantity)
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

// Q19: Discounted Revenue Query
func (e *QueryExecutor) executeDiscountedRevenue(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	brand1 := fmt.Sprintf("Brand#%d", e.faker.Int(1, 5)*10+e.faker.Int(1, 5))
	brand2 := fmt.Sprintf("Brand#%d", e.faker.Int(1, 5)*10+e.faker.Int(1, 5))
	brand3 := fmt.Sprintf("Brand#%d", e.faker.Int(1, 5)*10+e.faker.Int(1, 5))

	var revenue float64
	err := pool.QueryRow(ctx, `
        SELECT COALESCE(SUM(l_extendedprice * (1 - l_discount)), 0) AS revenue
        FROM lineitem, part
        WHERE p_partkey = l_partkey
            AND (
                (p_brand = $1
                    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                    AND l_quantity >= 1 AND l_quantity <= 11
                    AND p_size BETWEEN 1 AND 5
                    AND l_shipmode IN ('AIR', 'REG AIR')
                    AND l_shipinstruct = 'DELIVER IN PERSON')
                OR
                (p_brand = $2
                    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                    AND l_quantity >= 10 AND l_quantity <= 20
                    AND p_size BETWEEN 1 AND 10
                    AND l_shipmode IN ('AIR', 'REG AIR')
                    AND l_shipinstruct = 'DELIVER IN PERSON')
                OR
                (p_brand = $3
                    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                    AND l_quantity >= 20 AND l_quantity <= 30
                    AND p_size BETWEEN 1 AND 15
                    AND l_shipmode IN ('AIR', 'REG AIR')
                    AND l_shipinstruct = 'DELIVER IN PERSON')
            )
    `, brand1, brand2, brand3).Scan(&revenue)
	if err != nil {
		return 0, err
	}
	return 1, nil
}
