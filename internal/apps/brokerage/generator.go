package brokerage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/datagen"
	"github.com/pgEdge/pgedge-loadgen/internal/logging"
)

// TPC-E scale factor table sizes
var tableSizes = []datagen.TableSizeInfo{
	{Name: "exchange", BaseRowSize: 150, ScaleRatio: 4, IndexFactor: 1.1},
	{Name: "status_type", BaseRowSize: 20, ScaleRatio: 5, IndexFactor: 1.1},
	{Name: "trade_type", BaseRowSize: 30, ScaleRatio: 5, IndexFactor: 1.1},
	{Name: "sector", BaseRowSize: 40, ScaleRatio: 12, IndexFactor: 1.1},
	{Name: "industry", BaseRowSize: 60, ScaleRatio: 102, IndexFactor: 1.1},
	{Name: "company", BaseRowSize: 250, ScaleRatio: 5000, IndexFactor: 1.2},
	{Name: "security", BaseRowSize: 200, ScaleRatio: 6850, IndexFactor: 1.2},
	{Name: "customer", BaseRowSize: 300, ScaleRatio: 5000, IndexFactor: 1.2},
	{Name: "broker", BaseRowSize: 120, ScaleRatio: 50, IndexFactor: 1.2},
	{Name: "customer_account", BaseRowSize: 80, ScaleRatio: 5000, IndexFactor: 1.2},
	{Name: "holding_summary", BaseRowSize: 30, ScaleRatio: 10000, IndexFactor: 1.2},
	{Name: "watch_list", BaseRowSize: 20, ScaleRatio: 5000, IndexFactor: 1.2},
	{Name: "watch_item", BaseRowSize: 25, ScaleRatio: 50000, IndexFactor: 1.2},
	{Name: "trade", BaseRowSize: 150, ScaleRatio: 250000, IndexFactor: 1.3},
	{Name: "trade_history", BaseRowSize: 30, ScaleRatio: 750000, IndexFactor: 1.3},
	{Name: "settlement", BaseRowSize: 60, ScaleRatio: 250000, IndexFactor: 1.2},
	{Name: "last_trade", BaseRowSize: 50, ScaleRatio: 6850, IndexFactor: 1.1},
	{Name: "daily_market", BaseRowSize: 40, ScaleRatio: 250000, IndexFactor: 1.3},
}

// Reference data
var exchanges = []struct {
	id, name string
	numSymb  int
}{
	{"NYSE", "New York Stock Exchange", 3000},
	{"NASDAQ", "NASDAQ Stock Market", 3000},
	{"AMEX", "American Stock Exchange", 500},
	{"PCX", "NYSE Arca", 350},
}

var statusTypes = []struct {
	id, name string
}{
	{"ACTV", "Active"},
	{"CMPT", "Completed"},
	{"CNCL", "Canceled"},
	{"PNDG", "Pending"},
	{"SBMT", "Submitted"},
}

var tradeTypes = []struct {
	id, name string
	isSell   bool
	isMarket bool
}{
	{"TMB", "Market-Buy", false, true},
	{"TMS", "Market-Sell", true, true},
	{"TLB", "Limit-Buy", false, false},
	{"TLS", "Limit-Sell", true, false},
	{"TSL", "Stop-Loss", true, false},
}

var sectors = []struct {
	id, name string
}{
	{"EN", "Energy"},
	{"MT", "Materials"},
	{"IN", "Industrials"},
	{"CD", "Consumer Discretionary"},
	{"CS", "Consumer Staples"},
	{"HC", "Health Care"},
	{"FN", "Financials"},
	{"IT", "Information Technology"},
	{"TS", "Telecom Services"},
	{"UT", "Utilities"},
	{"RE", "Real Estate"},
	{"OT", "Other"},
}

var industries = []struct {
	id, name, sector string
}{
	{"OG", "Oil & Gas", "EN"},
	{"CM", "Chemicals", "MT"},
	{"AE", "Aerospace", "IN"},
	{"AU", "Automobiles", "CD"},
	{"FD", "Food Products", "CS"},
	{"PH", "Pharmaceuticals", "HC"},
	{"BK", "Banks", "FN"},
	{"SW", "Software", "IT"},
	{"TL", "Telecom", "TS"},
	{"EL", "Electric Utilities", "UT"},
	{"RI", "REITs", "RE"},
}

// Generator generates test data for the brokerage schema.
type Generator struct {
	faker *datagen.Faker
	cfg   datagen.BatchInsertConfig
}

// NewGenerator creates a new brokerage data generator.
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

	scaleFactor := max(1, int(rowCounts["customer"]/5000))

	logging.Info().
		Int("scale_factor", scaleFactor).
		Str("estimated_size", datagen.FormatSize(calc.EstimatedSize(rowCounts))).
		Msg("Generating brokerage data")

	// Generate reference data first
	if err := g.generateExchanges(ctx, pool); err != nil {
		return fmt.Errorf("failed to generate exchanges: %w", err)
	}
	if err := g.generateStatusTypes(ctx, pool); err != nil {
		return fmt.Errorf("failed to generate status types: %w", err)
	}
	if err := g.generateTradeTypes(ctx, pool); err != nil {
		return fmt.Errorf("failed to generate trade types: %w", err)
	}
	if err := g.generateSectors(ctx, pool); err != nil {
		return fmt.Errorf("failed to generate sectors: %w", err)
	}
	if err := g.generateIndustries(ctx, pool); err != nil {
		return fmt.Errorf("failed to generate industries: %w", err)
	}

	// Generate main tables
	numCompanies := scaleFactor * 5000
	if err := g.generateCompanies(ctx, pool, numCompanies); err != nil {
		return fmt.Errorf("failed to generate companies: %w", err)
	}

	numSecurities := scaleFactor * 6850
	if err := g.generateSecurities(ctx, pool, numSecurities, numCompanies); err != nil {
		return fmt.Errorf("failed to generate securities: %w", err)
	}

	numBrokers := scaleFactor * 50
	if err := g.generateBrokers(ctx, pool, numBrokers); err != nil {
		return fmt.Errorf("failed to generate brokers: %w", err)
	}

	numCustomers := scaleFactor * 5000
	if err := g.generateCustomers(ctx, pool, numCustomers); err != nil {
		return fmt.Errorf("failed to generate customers: %w", err)
	}

	numAccounts := scaleFactor * 5000
	if err := g.generateCustomerAccounts(ctx, pool, numAccounts, numCustomers, numBrokers); err != nil {
		return fmt.Errorf("failed to generate customer accounts: %w", err)
	}

	if err := g.generateWatchLists(ctx, pool, numCustomers, numSecurities); err != nil {
		return fmt.Errorf("failed to generate watch lists: %w", err)
	}

	numTrades := scaleFactor * 250000
	if err := g.generateTrades(ctx, pool, numTrades, numAccounts, numSecurities); err != nil {
		return fmt.Errorf("failed to generate trades: %w", err)
	}

	if err := g.generateLastTrades(ctx, pool, numSecurities); err != nil {
		return fmt.Errorf("failed to generate last trades: %w", err)
	}

	return nil
}

func (g *Generator) generateExchanges(ctx context.Context, pool *pgxpool.Pool) error {
	logging.Info().Msg("Generating exchanges")
	for _, ex := range exchanges {
		_, err := pool.Exec(ctx, `
            INSERT INTO exchange (ex_id, ex_name, ex_num_symb, ex_open, ex_close, ex_desc)
            VALUES ($1, $2, $3, 570, 960, $4)
        `, ex.id, ex.name, ex.numSymb, g.faker.Sentence(5))
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *Generator) generateStatusTypes(ctx context.Context, pool *pgxpool.Pool) error {
	logging.Info().Msg("Generating status types")
	for _, st := range statusTypes {
		_, err := pool.Exec(ctx, `
            INSERT INTO status_type (st_id, st_name) VALUES ($1, $2)
        `, st.id, st.name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *Generator) generateTradeTypes(ctx context.Context, pool *pgxpool.Pool) error {
	logging.Info().Msg("Generating trade types")
	for _, tt := range tradeTypes {
		_, err := pool.Exec(ctx, `
            INSERT INTO trade_type (tt_id, tt_name, tt_is_sell, tt_is_mrkt)
            VALUES ($1, $2, $3, $4)
        `, tt.id, tt.name, tt.isSell, tt.isMarket)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *Generator) generateSectors(ctx context.Context, pool *pgxpool.Pool) error {
	logging.Info().Msg("Generating sectors")
	for _, sc := range sectors {
		_, err := pool.Exec(ctx, `
            INSERT INTO sector (sc_id, sc_name) VALUES ($1, $2)
        `, sc.id, sc.name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *Generator) generateIndustries(ctx context.Context, pool *pgxpool.Pool) error {
	logging.Info().Msg("Generating industries")
	for _, in := range industries {
		_, err := pool.Exec(ctx, `
            INSERT INTO industry (in_id, in_name, in_sc_id) VALUES ($1, $2, $3)
        `, in.id, in.name, in.sector)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *Generator) generateCompanies(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating companies")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("company", int64(count), g.cfg.ProgressInterval)

	spRatings := []string{"AAA", "AA+", "AA", "A+", "A", "BBB", "BB", "B"}
	statusID := "ACTV"

	for i := 1; i <= count; i++ {
		industryIdx := i % len(industries)
		companyName := g.faker.Company()
		if len(companyName) > 60 {
			companyName = companyName[:60]
		}
		ceoName := g.faker.Name()
		if len(ceoName) > 100 {
			ceoName = ceoName[:100]
		}

		batch = append(batch, fmt.Sprintf("(%d, '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
			i,
			statusID,
			escapeSingleQuote(companyName),
			industries[industryIdx].id,
			datagen.Choose(g.faker, spRatings),
			escapeSingleQuote(ceoName),
			escapeSingleQuote(g.faker.Sentence(5)),
			g.faker.Date(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)).Format("2006-01-02"),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "company",
				"(co_id, co_st_id, co_name, co_in_id, co_sp_rate, co_ceo, co_desc, co_open_date)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "company",
			"(co_id, co_st_id, co_name, co_in_id, co_sp_rate, co_ceo, co_desc, co_open_date)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateSecurities(ctx context.Context, pool *pgxpool.Pool, count, numCompanies int) error {
	logging.Info().Int("count", count).Msg("Generating securities")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("security", int64(count), g.cfg.ProgressInterval)

	issues := []string{"COMMON", "PREF", "BOND", "CONVRT", "RIGHTS"}

	for i := 1; i <= count; i++ {
		symbol := fmt.Sprintf("SYM%06d", i)
		companyID := (i-1)%numCompanies + 1
		exchangeIdx := i % len(exchanges)
		price := g.faker.Float64(5, 500)
		high52 := price * (1 + g.faker.Float64(0.1, 0.5))
		low52 := price * (1 - g.faker.Float64(0.1, 0.4))

		batch = append(batch, fmt.Sprintf("('%s', '%s', 'ACTV', '%s', '%s', %d, %d, '%s', '%s', %.2f, %.2f, %.2f, %.2f, %.2f)",
			symbol,
			datagen.Choose(g.faker, issues),
			escapeSingleQuote(datagen.Truncate(g.faker.Company(), 70)),
			exchanges[exchangeIdx].id,
			companyID,
			int64(g.faker.Int(1000000, 1000000000)),
			g.faker.Date(time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)).Format("2006-01-02"),
			g.faker.Date(time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)).Format("2006-01-02"),
			g.faker.Float64(5, 100),
			high52,
			low52,
			g.faker.Float64(0, 5),
			g.faker.Float64(0, 8),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "security",
				"(s_symb, s_issue, s_st_id, s_name, s_ex_id, s_co_id, s_num_out, s_start_date, s_exch_date, s_pe, s_52wk_high, s_52wk_low, s_dividend, s_yield)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "security",
			"(s_symb, s_issue, s_st_id, s_name, s_ex_id, s_co_id, s_num_out, s_start_date, s_exch_date, s_pe, s_52wk_high, s_52wk_low, s_dividend, s_yield)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateBrokers(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating brokers")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("broker", int64(count), g.cfg.ProgressInterval)

	for i := 1; i <= count; i++ {
		brokerName := g.faker.Name()
		if len(brokerName) > 100 {
			brokerName = brokerName[:100]
		}
		batch = append(batch, fmt.Sprintf("(%d, 'ACTV', '%s', %d, %.2f)",
			i,
			escapeSingleQuote(brokerName),
			g.faker.Int(0, 10000),
			g.faker.Float64(0, 1000000),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "broker",
				"(b_id, b_st_id, b_name, b_num_trades, b_comm_total)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "broker",
			"(b_id, b_st_id, b_name, b_num_trades, b_comm_total)", batch); err != nil {
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
		gender := datagen.Choose(g.faker, []string{"M", "F"})
		tier := g.faker.Int(1, 3)

		batch = append(batch, fmt.Sprintf("(%d, '%s', 'ACTV', '%s', '%s', '%s', '%s', %d, '%s', '%s', '%s', '%s', '%s')",
			i,
			fmt.Sprintf("%09d", g.faker.Int(100000000, 999999999)),
			escapeSingleQuote(g.faker.LastName()),
			escapeSingleQuote(g.faker.FirstName()),
			g.faker.Letter()[0:1],
			gender,
			tier,
			g.faker.Date(time.Date(1950, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2005, 1, 1, 0, 0, 0, 0, time.UTC)).Format("2006-01-02"),
			"001",
			g.faker.Digits(3),
			g.faker.Digits(7),
			fmt.Sprintf("customer%d@example.com", i),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "customer",
				"(c_id, c_tax_id, c_st_id, c_l_name, c_f_name, c_m_name, c_gndr, c_tier, c_dob, c_ctry_1, c_area_1, c_local_1, c_email_1)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "customer",
			"(c_id, c_tax_id, c_st_id, c_l_name, c_f_name, c_m_name, c_gndr, c_tier, c_dob, c_ctry_1, c_area_1, c_local_1, c_email_1)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateCustomerAccounts(ctx context.Context, pool *pgxpool.Pool, count, numCustomers, numBrokers int) error {
	logging.Info().Int("count", count).Msg("Generating customer accounts")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("customer_account", int64(count), g.cfg.ProgressInterval)

	for i := 1; i <= count; i++ {
		customerID := (i-1)%numCustomers + 1
		brokerID := g.faker.Int(1, numBrokers)
		taxStatus := g.faker.Int(0, 2)

		batch = append(batch, fmt.Sprintf("(%d, %d, %d, '%s', %d, %.2f)",
			i,
			brokerID,
			customerID,
			fmt.Sprintf("Account #%d", i),
			taxStatus,
			g.faker.Float64(0, 1000000),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "customer_account",
				"(ca_id, ca_b_id, ca_c_id, ca_name, ca_tax_st, ca_bal)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "customer_account",
			"(ca_id, ca_b_id, ca_c_id, ca_name, ca_tax_st, ca_bal)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateWatchLists(ctx context.Context, pool *pgxpool.Pool, numCustomers, numSecurities int) error {
	logging.Info().Msg("Generating watch lists and items")

	// Create one watch list per customer
	wlBatch := make([]string, 0, g.cfg.BatchSize)
	wiBatch := make([]string, 0, g.cfg.BatchSize)

	for c := 1; c <= numCustomers; c++ {
		wlBatch = append(wlBatch, fmt.Sprintf("(%d, %d)", c, c))

		// Add 5-20 items per watch list
		numItems := g.faker.Int(5, 20)
		for j := 0; j < numItems; j++ {
			secIdx := g.faker.Int(1, numSecurities)
			symbol := fmt.Sprintf("SYM%06d", secIdx)
			wiBatch = append(wiBatch, fmt.Sprintf("(%d, '%s')", c, symbol))
		}

		if len(wlBatch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "watch_list", "(wl_id, wl_c_id)", wlBatch); err != nil {
				return err
			}
			if err := g.executeBatchInsertOnConflict(ctx, pool, "watch_item", "(wi_wl_id, wi_s_symb)", wiBatch); err != nil {
				return err
			}
			wlBatch = wlBatch[:0]
			wiBatch = wiBatch[:0]
		}
	}

	if len(wlBatch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "watch_list", "(wl_id, wl_c_id)", wlBatch); err != nil {
			return err
		}
		if err := g.executeBatchInsertOnConflict(ctx, pool, "watch_item", "(wi_wl_id, wi_s_symb)", wiBatch); err != nil {
			return err
		}
	}

	logging.Info().Msg("Watch lists complete")
	return nil
}

func (g *Generator) generateTrades(ctx context.Context, pool *pgxpool.Pool, count, numAccounts, numSecurities int) error {
	logging.Info().Int("count", count).Msg("Generating trades")
	tradeBatch := make([]string, 0, g.cfg.BatchSize/10)
	histBatch := make([]string, 0, g.cfg.BatchSize)
	settleBatch := make([]string, 0, g.cfg.BatchSize/10)
	progress := datagen.NewProgressReporter("trade", int64(count), int64(count/10))

	baseDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	ttIDs := []string{"TMB", "TMS", "TLB", "TLS", "TSL"}
	stIDs := []string{"CMPT", "CMPT", "CMPT", "CMPT", "CNCL"} // Most trades complete

	for i := 1; i <= count; i++ {
		accountID := g.faker.Int(1, numAccounts)
		secIdx := g.faker.Int(1, numSecurities)
		symbol := fmt.Sprintf("SYM%06d", secIdx)
		ttID := datagen.Choose(g.faker, ttIDs)
		stID := datagen.Choose(g.faker, stIDs)
		isCash := g.faker.Int(0, 1) == 1
		qty := g.faker.Int(100, 10000)
		price := g.faker.Float64(5, 500)
		tradePrice := price * (1 + g.faker.Float64(-0.02, 0.02))

		tradeDTS := baseDate.Add(time.Duration(g.faker.Int(0, 365*4*24)) * time.Hour)

		tradeBatch = append(tradeBatch, fmt.Sprintf("(%d, '%s', '%s', '%s', %t, '%s', %d, %.2f, %d, '%s', %.2f, %.2f, %.2f, %.2f, %t)",
			i,
			tradeDTS.Format("2006-01-02 15:04:05"),
			stID,
			ttID,
			isCash,
			symbol,
			qty,
			price,
			accountID,
			escapeSingleQuote(datagen.Truncate(g.faker.Name(), 64)),
			tradePrice,
			g.faker.Float64(0, 50),
			g.faker.Float64(0, 100),
			g.faker.Float64(0, 50),
			g.faker.Int(0, 1) == 1,
		))

		// Trade history - up to 3 status changes
		histBatch = append(histBatch, fmt.Sprintf("(%d, '%s', 'SBMT')", i, tradeDTS.Format("2006-01-02 15:04:05")))
		histBatch = append(histBatch, fmt.Sprintf("(%d, '%s', 'PNDG')", i, tradeDTS.Add(time.Second).Format("2006-01-02 15:04:05")))
		histBatch = append(histBatch, fmt.Sprintf("(%d, '%s', '%s')", i, tradeDTS.Add(time.Minute).Format("2006-01-02 15:04:05"), stID))

		// Settlement
		if stID == "CMPT" {
			settleBatch = append(settleBatch, fmt.Sprintf("(%d, 'Cash Account', '%s', %.2f)",
				i,
				tradeDTS.Add(3*24*time.Hour).Format("2006-01-02"),
				float64(qty)*tradePrice,
			))
		}

		if len(tradeBatch) >= g.cfg.BatchSize/10 {
			if err := g.executeBatchInsert(ctx, pool, "trade",
				"(t_id, t_dts, t_st_id, t_tt_id, t_is_cash, t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name, t_trade_price, t_chrg, t_comm, t_tax, t_lifo)", tradeBatch); err != nil {
				return err
			}
			if err := g.executeBatchInsert(ctx, pool, "trade_history", "(th_t_id, th_dts, th_st_id)", histBatch); err != nil {
				return err
			}
			if len(settleBatch) > 0 {
				if err := g.executeBatchInsert(ctx, pool, "settlement", "(se_t_id, se_cash_type, se_cash_due_date, se_amt)", settleBatch); err != nil {
					return err
				}
			}
			progress.Update(int64(len(tradeBatch)))
			tradeBatch = tradeBatch[:0]
			histBatch = histBatch[:0]
			settleBatch = settleBatch[:0]
		}
	}

	if len(tradeBatch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "trade",
			"(t_id, t_dts, t_st_id, t_tt_id, t_is_cash, t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name, t_trade_price, t_chrg, t_comm, t_tax, t_lifo)", tradeBatch); err != nil {
			return err
		}
		if err := g.executeBatchInsert(ctx, pool, "trade_history", "(th_t_id, th_dts, th_st_id)", histBatch); err != nil {
			return err
		}
		if len(settleBatch) > 0 {
			if err := g.executeBatchInsert(ctx, pool, "settlement", "(se_t_id, se_cash_type, se_cash_due_date, se_amt)", settleBatch); err != nil {
				return err
			}
		}
		progress.Update(int64(len(tradeBatch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateLastTrades(ctx context.Context, pool *pgxpool.Pool, numSecurities int) error {
	logging.Info().Int("count", numSecurities).Msg("Generating last trades")
	batch := make([]string, 0, g.cfg.BatchSize)

	baseDate := time.Now()

	for i := 1; i <= numSecurities; i++ {
		symbol := fmt.Sprintf("SYM%06d", i)
		price := g.faker.Float64(5, 500)
		openPrice := price * (1 + g.faker.Float64(-0.05, 0.05))

		batch = append(batch, fmt.Sprintf("('%s', '%s', %.2f, %.2f, %d)",
			symbol,
			baseDate.Add(-time.Duration(g.faker.Int(0, 3600))*time.Second).Format("2006-01-02 15:04:05"),
			price,
			openPrice,
			int64(g.faker.Int(10000, 10000000)),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "last_trade",
				"(lt_s_symb, lt_dts, lt_price, lt_open_price, lt_vol)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "last_trade",
			"(lt_s_symb, lt_dts, lt_price, lt_open_price, lt_vol)", batch); err != nil {
			return err
		}
	}

	logging.Info().Msg("Last trades complete")
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

func (g *Generator) executeBatchInsertOnConflict(ctx context.Context, pool *pgxpool.Pool, table, columns string, values []string) error {
	if len(values) == 0 {
		return nil
	}
	sql := fmt.Sprintf("INSERT INTO %s %s VALUES %s ON CONFLICT DO NOTHING", table, columns, strings.Join(values, ", "))
	_, err := pool.Exec(ctx, sql)
	return err
}

func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
