//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Portions copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

package analytics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/datagen"
	"github.com/pgEdge/pgedge-loadgen/internal/logging"
)

// TPC-H scale factor table sizes
var tableSizes = []datagen.TableSizeInfo{
	{Name: "region", BaseRowSize: 124, ScaleRatio: 5, IndexFactor: 1.1},         // Fixed 5 regions
	{Name: "nation", BaseRowSize: 128, ScaleRatio: 25, IndexFactor: 1.1},        // Fixed 25 nations
	{Name: "supplier", BaseRowSize: 159, ScaleRatio: 10000, IndexFactor: 1.2},   // 10k per SF
	{Name: "part", BaseRowSize: 155, ScaleRatio: 200000, IndexFactor: 1.2},      // 200k per SF
	{Name: "partsupp", BaseRowSize: 144, ScaleRatio: 800000, IndexFactor: 1.2},  // 4 per part
	{Name: "customer", BaseRowSize: 179, ScaleRatio: 150000, IndexFactor: 1.2},  // 150k per SF
	{Name: "orders", BaseRowSize: 104, ScaleRatio: 1500000, IndexFactor: 1.3},   // 1.5M per SF
	{Name: "lineitem", BaseRowSize: 112, ScaleRatio: 6000000, IndexFactor: 1.3}, // ~4 per order
}

// Reference data
var regions = []string{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}
var nations = []struct {
	name   string
	region int
}{
	{"ALGERIA", 0}, {"ARGENTINA", 1}, {"BRAZIL", 1}, {"CANADA", 1}, {"EGYPT", 4},
	{"ETHIOPIA", 0}, {"FRANCE", 3}, {"GERMANY", 3}, {"INDIA", 2}, {"INDONESIA", 2},
	{"IRAN", 4}, {"IRAQ", 4}, {"JAPAN", 2}, {"JORDAN", 4}, {"KENYA", 0},
	{"MOROCCO", 0}, {"MOZAMBIQUE", 0}, {"PERU", 1}, {"CHINA", 2}, {"ROMANIA", 3},
	{"SAUDI ARABIA", 4}, {"VIETNAM", 2}, {"RUSSIA", 3}, {"UNITED KINGDOM", 3}, {"UNITED STATES", 1},
}

var segments = []string{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"}
var priorities = []string{"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"}
var shipModes = []string{"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"}
var shipInstructs = []string{"DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"}
var containers = []string{"SM CASE", "SM BOX", "SM PACK", "SM PKG", "MED BAG", "MED BOX", "MED PKG", "MED PACK", "LG CASE", "LG BOX", "LG PACK", "LG PKG", "JUMBO BAG", "JUMBO BOX", "JUMBO PACK", "JUMBO PKG", "WRAP CASE", "WRAP BOX", "WRAP PACK", "WRAP PKG"}
var types = []string{"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"}
var typeAdjs = []string{"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"}
var typeMats = []string{"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"}

// Generator generates test data for the analytics schema.
type Generator struct {
	faker *datagen.Faker
	cfg   datagen.BatchInsertConfig
}

// NewGenerator creates a new analytics data generator.
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

	// Calculate scale factor
	scaleFactor := max(1, int(rowCounts["supplier"]/10000))

	logging.Info().
		Int("scale_factor", scaleFactor).
		Str("estimated_size", datagen.FormatSize(calc.EstimatedSize(rowCounts))).
		Msg("Generating analytics data")

	// Generate reference data first
	if err := g.generateRegions(ctx, pool); err != nil {
		return fmt.Errorf("failed to generate regions: %w", err)
	}
	if err := g.generateNations(ctx, pool); err != nil {
		return fmt.Errorf("failed to generate nations: %w", err)
	}

	// Generate main tables
	numSuppliers := scaleFactor * 10000
	if err := g.generateSuppliers(ctx, pool, numSuppliers); err != nil {
		return fmt.Errorf("failed to generate suppliers: %w", err)
	}

	numParts := scaleFactor * 200000
	if err := g.generateParts(ctx, pool, numParts); err != nil {
		return fmt.Errorf("failed to generate parts: %w", err)
	}

	if err := g.generatePartSupp(ctx, pool, numParts, numSuppliers); err != nil {
		return fmt.Errorf("failed to generate partsupp: %w", err)
	}

	numCustomers := scaleFactor * 150000
	if err := g.generateCustomers(ctx, pool, numCustomers); err != nil {
		return fmt.Errorf("failed to generate customers: %w", err)
	}

	numOrders := scaleFactor * 1500000
	if err := g.generateOrders(ctx, pool, numOrders, numCustomers, numParts, numSuppliers); err != nil {
		return fmt.Errorf("failed to generate orders: %w", err)
	}

	return nil
}

func (g *Generator) generateRegions(ctx context.Context, pool *pgxpool.Pool) error {
	logging.Info().Msg("Generating regions")
	for i, name := range regions {
		_, err := pool.Exec(ctx, `
			INSERT INTO region (r_regionkey, r_name, r_comment) VALUES ($1, $2, $3)
		`, i, name, g.faker.Sentence(10))
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *Generator) generateNations(ctx context.Context, pool *pgxpool.Pool) error {
	logging.Info().Msg("Generating nations")
	for i, n := range nations {
		_, err := pool.Exec(ctx, `
			INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) VALUES ($1, $2, $3, $4)
		`, i, n.name, n.region, g.faker.Sentence(10))
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *Generator) generateSuppliers(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating suppliers")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("supplier", int64(count), g.cfg.ProgressInterval)

	for i := 1; i <= count; i++ {
		batch = append(batch, fmt.Sprintf("(%d, 'Supplier#%09d', '%s', %d, '%s', %.2f, '%s')",
			i, i,
			escapeSingleQuote(datagen.Truncate(g.faker.Street(), 40)),
			g.faker.Int(0, 24),
			g.faker.Digits(15),
			g.faker.Float64(-999.99, 9999.99),
			escapeSingleQuote(g.faker.Sentence(5)),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "supplier",
				"(s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "supplier",
			"(s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateParts(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating parts")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("part", int64(count), g.cfg.ProgressInterval)

	for i := 1; i <= count; i++ {
		partType := fmt.Sprintf("%s %s %s",
			datagen.Choose(g.faker, types),
			datagen.Choose(g.faker, typeAdjs),
			datagen.Choose(g.faker, typeMats))

		productName := g.faker.ProductName()
		if len(productName) > 55 {
			productName = productName[:55]
		}

		batch = append(batch, fmt.Sprintf("(%d, '%s', 'Manufacturer#%d', 'Brand#%d', '%s', %d, '%s', %.2f, '%s')",
			i,
			escapeSingleQuote(productName),
			g.faker.Int(1, 5),
			g.faker.Int(1, 5)*10+g.faker.Int(1, 5),
			partType,
			g.faker.Int(1, 50),
			datagen.Choose(g.faker, containers),
			float64(90000+i)/100.0,
			escapeSingleQuote(datagen.Truncate(g.faker.Sentence(3), 23)),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "part",
				"(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "part",
			"(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generatePartSupp(ctx context.Context, pool *pgxpool.Pool, numParts, numSuppliers int) error {
	logging.Info().Msg("Generating partsupp")
	batch := make([]string, 0, g.cfg.BatchSize)
	total := int64(numParts * 4)
	progress := datagen.NewProgressReporter("partsupp", total, g.cfg.ProgressInterval)

	for p := 1; p <= numParts; p++ {
		for s := 0; s < 4; s++ {
			suppKey := (p + s*(numSuppliers/4) + (p-1)/numSuppliers) % numSuppliers
			if suppKey == 0 {
				suppKey = 1
			}

			batch = append(batch, fmt.Sprintf("(%d, %d, %d, %.2f, '%s')",
				p, suppKey,
				g.faker.Int(1, 9999),
				g.faker.Float64(1, 1000),
				escapeSingleQuote(g.faker.Sentence(10)),
			))

			if len(batch) >= g.cfg.BatchSize {
				if err := g.executeBatchInsert(ctx, pool, "partsupp",
					"(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)", batch); err != nil {
					return err
				}
				progress.Update(int64(len(batch)))
				batch = batch[:0]
			}
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "partsupp",
			"(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateCustomers(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating customers")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("customer", int64(count), g.cfg.ProgressInterval)

	for i := 1; i <= count; i++ {
		batch = append(batch, fmt.Sprintf("(%d, 'Customer#%09d', '%s', %d, '%s', %.2f, '%s', '%s')",
			i, i,
			escapeSingleQuote(datagen.Truncate(g.faker.Street(), 40)),
			g.faker.Int(0, 24),
			g.faker.Digits(15),
			g.faker.Float64(-999.99, 9999.99),
			datagen.Choose(g.faker, segments),
			escapeSingleQuote(g.faker.Sentence(5)),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "customer",
				"(c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "customer",
			"(c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateOrders(ctx context.Context, pool *pgxpool.Pool, count, numCustomers, numParts, numSuppliers int) error {
	logging.Info().Int("count", count).Msg("Generating orders and lineitems")
	orderBatch := make([]string, 0, g.cfg.BatchSize/10)
	lineitemBatch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("orders", int64(count), int64(count/10))

	baseDate := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)

	for o := 1; o <= count; o++ {
		custKey := (o-1)%numCustomers + 1
		orderDate := baseDate.AddDate(0, 0, g.faker.Int(0, 2556)) // ~7 years
		status := "F"
		if orderDate.After(time.Date(1995, 6, 17, 0, 0, 0, 0, time.UTC)) {
			status = "O"
		}

		lineCount := g.faker.Int(1, 7)
		var totalPrice float64

		for l := 1; l <= lineCount; l++ {
			partKey := g.faker.Int(1, numParts)
			suppIdx := g.faker.Int(0, 3)
			suppKey := (partKey + suppIdx*(numSuppliers/4) + (partKey-1)/numSuppliers) % numSuppliers
			if suppKey == 0 {
				suppKey = 1
			}

			qty := float64(g.faker.Int(1, 50))
			price := float64(90000+partKey) / 100.0
			discount := float64(g.faker.Int(0, 10)) / 100.0
			tax := float64(g.faker.Int(0, 8)) / 100.0
			extPrice := qty * price
			totalPrice += extPrice * (1 - discount) * (1 + tax)

			shipDate := orderDate.AddDate(0, 0, g.faker.Int(1, 121))
			commitDate := orderDate.AddDate(0, 0, g.faker.Int(30, 90))
			receiptDate := shipDate.AddDate(0, 0, g.faker.Int(1, 30))

			returnFlag := "N"
			if receiptDate.Before(time.Date(1995, 6, 17, 0, 0, 0, 0, time.UTC)) {
				if g.faker.Int(1, 100) <= 25 {
					returnFlag = "R"
				} else {
					returnFlag = "A"
				}
			}

			lineStatus := "F"
			if shipDate.After(time.Date(1995, 6, 17, 0, 0, 0, 0, time.UTC)) {
				lineStatus = "O"
			}

			lineitemBatch = append(lineitemBatch, fmt.Sprintf(
				"(%d, %d, %d, %d, %.2f, %.2f, %.2f, %.2f, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
				o, partKey, suppKey, l,
				qty, extPrice, discount, tax,
				returnFlag, lineStatus,
				shipDate.Format("2006-01-02"),
				commitDate.Format("2006-01-02"),
				receiptDate.Format("2006-01-02"),
				datagen.Choose(g.faker, shipInstructs),
				datagen.Choose(g.faker, shipModes),
				escapeSingleQuote(datagen.Truncate(g.faker.Sentence(3), 44)),
			))
		}

		orderBatch = append(orderBatch, fmt.Sprintf(
			"(%d, %d, '%s', %.2f, '%s', '%s', 'Clerk#%09d', 0, '%s')",
			o, custKey, status, totalPrice,
			orderDate.Format("2006-01-02"),
			datagen.Choose(g.faker, priorities),
			g.faker.Int(1, 1000),
			escapeSingleQuote(datagen.Truncate(g.faker.Sentence(3), 79)),
		))

		if len(orderBatch) >= g.cfg.BatchSize/10 {
			if err := g.executeBatchInsert(ctx, pool, "orders",
				"(o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)", orderBatch); err != nil {
				return err
			}

			if err := g.executeBatchInsert(ctx, pool, "lineitem",
				"(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)", lineitemBatch); err != nil {
				return err
			}

			progress.Update(int64(len(orderBatch)))
			orderBatch = orderBatch[:0]
			lineitemBatch = lineitemBatch[:0]
		}
	}

	if len(orderBatch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "orders",
			"(o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)", orderBatch); err != nil {
			return err
		}
		if err := g.executeBatchInsert(ctx, pool, "lineitem",
			"(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)", lineitemBatch); err != nil {
			return err
		}
		progress.Update(int64(len(orderBatch)))
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
