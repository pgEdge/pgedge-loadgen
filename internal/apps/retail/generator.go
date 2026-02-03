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
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pgEdge/pgedge-loadgen/internal/datagen"
	"github.com/pgEdge/pgedge-loadgen/internal/logging"
)

// TPC-DS scale factor table sizes
var tableSizes = []datagen.TableSizeInfo{
	{Name: "date_dim", BaseRowSize: 200, ScaleRatio: 73049, IndexFactor: 1.1},
	{Name: "time_dim", BaseRowSize: 80, ScaleRatio: 86400, IndexFactor: 1.1},
	{Name: "item", BaseRowSize: 300, ScaleRatio: 18000, IndexFactor: 1.2},
	{Name: "customer", BaseRowSize: 200, ScaleRatio: 100000, IndexFactor: 1.2},
	{Name: "customer_demographics", BaseRowSize: 50, ScaleRatio: 1920800, IndexFactor: 1.1},
	{Name: "household_demographics", BaseRowSize: 30, ScaleRatio: 7200, IndexFactor: 1.1},
	{Name: "customer_address", BaseRowSize: 150, ScaleRatio: 50000, IndexFactor: 1.2},
	{Name: "store", BaseRowSize: 350, ScaleRatio: 12, IndexFactor: 1.1},
	{Name: "warehouse", BaseRowSize: 200, ScaleRatio: 5, IndexFactor: 1.1},
	{Name: "promotion", BaseRowSize: 150, ScaleRatio: 300, IndexFactor: 1.1},
	{Name: "store_sales", BaseRowSize: 100, ScaleRatio: 2880000, IndexFactor: 1.3},
	{Name: "web_sales", BaseRowSize: 150, ScaleRatio: 720000, IndexFactor: 1.3},
	{Name: "catalog_sales", BaseRowSize: 150, ScaleRatio: 1440000, IndexFactor: 1.3},
	{Name: "inventory", BaseRowSize: 20, ScaleRatio: 11745000, IndexFactor: 1.2},
}

// Reference data
var dayNames = []string{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}
var categories = []string{"Electronics", "Clothing", "Home", "Garden", "Sports", "Toys", "Books", "Music", "Food", "Health"}
var brands = []string{"Brand A", "Brand B", "Brand C", "Brand D", "Brand E", "Brand F", "Brand G", "Brand H"}
var colors = []string{"Red", "Blue", "Green", "Yellow", "Black", "White", "Gray", "Brown", "Pink", "Purple"}
var sizes = []string{"XS", "S", "M", "L", "XL", "XXL", "N/A"}
var usStates = []string{"AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
	"HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
	"MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
	"NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
	"SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"}

// Generator generates test data for the retail schema.
type Generator struct {
	faker *datagen.Faker
	cfg   datagen.BatchInsertConfig
}

// NewGenerator creates a new retail data generator.
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

	scaleFactor := max(1, int(rowCounts["customer"]/100000))

	logging.Info().
		Int("scale_factor", scaleFactor).
		Str("estimated_size", datagen.FormatSize(calc.EstimatedSize(rowCounts))).
		Msg("Generating retail data")

	// Generate dimension tables first
	if err := g.generateDateDim(ctx, pool); err != nil {
		return fmt.Errorf("failed to generate date_dim: %w", err)
	}
	if err := g.generateTimeDim(ctx, pool); err != nil {
		return fmt.Errorf("failed to generate time_dim: %w", err)
	}

	numItems := scaleFactor * 18000
	if err := g.generateItems(ctx, pool, numItems); err != nil {
		return fmt.Errorf("failed to generate items: %w", err)
	}

	numCustomerDemo := min(scaleFactor*1920800, 100000) // Limit for sanity
	if err := g.generateCustomerDemographics(ctx, pool, numCustomerDemo); err != nil {
		return fmt.Errorf("failed to generate customer_demographics: %w", err)
	}

	numHouseholdDemo := scaleFactor * 7200
	if err := g.generateHouseholdDemographics(ctx, pool, numHouseholdDemo); err != nil {
		return fmt.Errorf("failed to generate household_demographics: %w", err)
	}

	numAddresses := scaleFactor * 50000
	if err := g.generateCustomerAddresses(ctx, pool, numAddresses); err != nil {
		return fmt.Errorf("failed to generate customer_address: %w", err)
	}

	numCustomers := scaleFactor * 100000
	if err := g.generateCustomers(ctx, pool, numCustomers, numAddresses, numCustomerDemo, numHouseholdDemo); err != nil {
		return fmt.Errorf("failed to generate customers: %w", err)
	}

	numStores := scaleFactor * 12
	if err := g.generateStores(ctx, pool, numStores); err != nil {
		return fmt.Errorf("failed to generate stores: %w", err)
	}

	numWarehouses := scaleFactor * 5
	if err := g.generateWarehouses(ctx, pool, numWarehouses); err != nil {
		return fmt.Errorf("failed to generate warehouses: %w", err)
	}

	numPromotions := scaleFactor * 300
	if err := g.generatePromotions(ctx, pool, numPromotions, numItems); err != nil {
		return fmt.Errorf("failed to generate promotions: %w", err)
	}

	// Generate fact tables
	numStoreSales := scaleFactor * 2880000
	if err := g.generateStoreSales(ctx, pool, numStoreSales, numItems, numCustomers,
		numAddresses, numStores, numPromotions, numCustomerDemo, numHouseholdDemo); err != nil {
		return fmt.Errorf("failed to generate store_sales: %w", err)
	}

	numWebSales := scaleFactor * 720000
	if err := g.generateWebSales(ctx, pool, numWebSales, numItems, numCustomers,
		numAddresses, numWarehouses, numPromotions, numCustomerDemo, numHouseholdDemo); err != nil {
		return fmt.Errorf("failed to generate web_sales: %w", err)
	}

	numCatalogSales := scaleFactor * 1440000
	if err := g.generateCatalogSales(ctx, pool, numCatalogSales, numItems, numCustomers,
		numAddresses, numWarehouses, numPromotions, numCustomerDemo, numHouseholdDemo); err != nil {
		return fmt.Errorf("failed to generate catalog_sales: %w", err)
	}

	return nil
}

func (g *Generator) generateDateDim(ctx context.Context, pool *pgxpool.Pool) error {
	logging.Info().Msg("Generating date_dim")
	batch := make([]string, 0, g.cfg.BatchSize)

	startDate := time.Date(1998, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2003, 12, 31, 0, 0, 0, 0, time.UTC)

	sk := 1
	for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
		dow := int(d.Weekday())
		if dow == 0 {
			dow = 7
		}

		weekSeq := sk / 7
		quarterSeq := (sk - 1) / 91
		monthSeq := (sk - 1) / 30
		qoy := (int(d.Month())-1)/3 + 1

		weekend := "N"
		if dow >= 6 {
			weekend = "Y"
		}

		holiday := "N"
		if (d.Month() == 12 && d.Day() == 25) || (d.Month() == 1 && d.Day() == 1) ||
			(d.Month() == 7 && d.Day() == 4) {
			holiday = "Y"
		}

		quarterName := fmt.Sprintf("%dQ%d", d.Year(), qoy)

		batch = append(batch, fmt.Sprintf("(%d, '%s', '%s', %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, '%s', '%s', '%s', '%s', '%s', %d, %d, %d, %d, 'N', 'N', 'N', 'N', 'N')",
			sk,
			fmt.Sprintf("AAAAAAAAA%07d", sk),
			d.Format("2006-01-02"),
			monthSeq,
			weekSeq,
			quarterSeq,
			d.Year(),
			dow,
			int(d.Month()),
			d.Day(),
			qoy,
			d.Year(),
			quarterSeq,
			weekSeq,
			dayNames[dow-1],
			quarterName,
			holiday,
			weekend,
			"N",
			(sk/30)*30+1,
			(sk/30)*30+30,
			sk-365,
			sk-91,
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "date_dim",
				"(d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
		sk++
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "date_dim",
			"(d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year)", batch); err != nil {
			return err
		}
	}

	logging.Info().Int("count", sk-1).Msg("date_dim complete")
	return nil
}

func (g *Generator) generateTimeDim(ctx context.Context, pool *pgxpool.Pool) error {
	logging.Info().Msg("Generating time_dim")
	batch := make([]string, 0, g.cfg.BatchSize)

	for sk := 0; sk < 86400; sk++ {
		hour := sk / 3600
		minute := (sk % 3600) / 60
		second := sk % 60

		ampm := "AM"
		if hour >= 12 {
			ampm = "PM"
		}

		shift := "third"
		if hour >= 8 && hour < 16 {
			shift = "first"
		} else if hour >= 16 && hour < 24 {
			shift = "second"
		}

		subShift := "night"
		if hour >= 6 && hour < 12 {
			subShift = "morning"
		} else if hour >= 12 && hour < 18 {
			subShift = "afternoon"
		} else if hour >= 18 && hour < 22 {
			subShift = "evening"
		}

		var mealTime string
		if hour >= 7 && hour < 9 {
			mealTime = "breakfast"
		} else if hour >= 12 && hour < 14 {
			mealTime = "lunch"
		} else if hour >= 18 && hour < 20 {
			mealTime = "dinner"
		}

		mealTimeVal := "NULL"
		if mealTime != "" {
			mealTimeVal = fmt.Sprintf("'%s'", mealTime)
		}

		batch = append(batch, fmt.Sprintf("(%d, '%s', %d, %d, %d, %d, '%s', '%s', '%s', %s)",
			sk,
			fmt.Sprintf("AAAAAAAAA%05d", sk),
			sk,
			hour,
			minute,
			second,
			ampm,
			shift,
			subShift,
			mealTimeVal,
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "time_dim",
				"(t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "time_dim",
			"(t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time)", batch); err != nil {
			return err
		}
	}

	logging.Info().Msg("time_dim complete")
	return nil
}

func (g *Generator) generateItems(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating items")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("item", int64(count), g.cfg.ProgressInterval)

	for i := 1; i <= count; i++ {
		catIdx := i % len(categories)
		brandIdx := i % len(brands)
		colorIdx := i % len(colors)
		sizeIdx := i % len(sizes)

		price := g.faker.Float64(1, 1000)
		wholesale := price * 0.6

		productDesc := g.faker.ProductDescription()
		if len(productDesc) > 200 {
			productDesc = productDesc[:200]
		}

		productName := g.faker.ProductName()
		if len(productName) > 50 {
			productName = productName[:50]
		}

		batch = append(batch, fmt.Sprintf("(%d, '%s', NULL, NULL, '%s', %.2f, %.2f, %d, '%s', %d, 'Class%d', %d, '%s', %d, 'Manufact%d', '%s', NULL, '%s', 'Each', 'Unknown', %d, '%s')",
			i,
			fmt.Sprintf("AAAAAAAAA%07d", i),
			escapeSingleQuote(productDesc),
			price,
			wholesale,
			brandIdx+1,
			brands[brandIdx],
			catIdx+1,
			catIdx+1,
			catIdx+1,
			categories[catIdx],
			i%50+1,
			i%50+1,
			sizes[sizeIdx],
			colors[colorIdx],
			i%100+1,
			escapeSingleQuote(productName),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "item",
				"(i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "item",
			"(i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateCustomerDemographics(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating customer_demographics")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("customer_demographics", int64(count), g.cfg.ProgressInterval)

	genders := []string{"M", "F"}
	maritalStatuses := []string{"M", "S", "D", "W", "U"}
	educationStatuses := []string{"Primary", "Secondary", "College", "Graduate", "Unknown"}
	creditRatings := []string{"Low", "Medium", "High", "Unknown"}

	for i := 1; i <= count; i++ {
		batch = append(batch, fmt.Sprintf("(%d, '%s', '%s', '%s', %d, '%s', %d, %d, %d)",
			i,
			datagen.Choose(g.faker, genders),
			datagen.Choose(g.faker, maritalStatuses),
			datagen.Choose(g.faker, educationStatuses),
			g.faker.Int(500, 10000),
			datagen.Choose(g.faker, creditRatings),
			g.faker.Int(0, 6),
			g.faker.Int(0, 6),
			g.faker.Int(0, 6),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "customer_demographics",
				"(cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "customer_demographics",
			"(cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateHouseholdDemographics(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating household_demographics")
	batch := make([]string, 0, g.cfg.BatchSize)

	buyPotentials := []string{"Unknown", "Low", "Medium", "High", "Very High"}

	for i := 1; i <= count; i++ {
		batch = append(batch, fmt.Sprintf("(%d, %d, '%s', %d, %d)",
			i,
			g.faker.Int(1, 20),
			datagen.Choose(g.faker, buyPotentials),
			g.faker.Int(0, 6),
			g.faker.Int(0, 4),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "household_demographics",
				"(hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "household_demographics",
			"(hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count)", batch); err != nil {
			return err
		}
	}

	logging.Info().Msg("household_demographics complete")
	return nil
}

func (g *Generator) generateCustomerAddresses(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating customer_address")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("customer_address", int64(count), g.cfg.ProgressInterval)

	for i := 1; i <= count; i++ {
		state := datagen.Choose(g.faker, usStates)
		batch = append(batch, fmt.Sprintf("(%d, '%s', '%s', '%s', 'St', NULL, '%s', '%s', '%s', '%s', 'United States', -5.00, 'residential')",
			i,
			fmt.Sprintf("AAAAAAAAA%07d", i),
			g.faker.Digits(5),
			escapeSingleQuote(datagen.Truncate(g.faker.Street(), 60)),
			escapeSingleQuote(datagen.Truncate(g.faker.City(), 60)),
			escapeSingleQuote(datagen.Truncate(g.faker.City(), 30)),
			state,
			g.faker.Zip(),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "customer_address",
				"(ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "customer_address",
			"(ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateCustomers(ctx context.Context, pool *pgxpool.Pool, count, numAddresses, numCDemo, numHDemo int) error {
	logging.Info().Int("count", count).Msg("Generating customers")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("customer", int64(count), g.cfg.ProgressInterval)

	for i := 1; i <= count; i++ {
		addrSK := g.faker.Int(1, numAddresses)
		cDemoSK := g.faker.Int(1, numCDemo)
		hDemoSK := g.faker.Int(1, numHDemo)
		birthYear := g.faker.Int(1940, 2000)
		birthMonth := g.faker.Int(1, 12)
		birthDay := g.faker.Int(1, 28)

		batch = append(batch, fmt.Sprintf("(%d, '%s', %d, %d, %d, NULL, NULL, '%s', '%s', '%s', '%s', %d, %d, %d, 'United States', NULL, '%s', NULL)",
			i,
			fmt.Sprintf("AAAAAAAAA%07d", i),
			cDemoSK,
			hDemoSK,
			addrSK,
			datagen.Choose(g.faker, []string{"Mr.", "Mrs.", "Ms.", "Dr."}),
			escapeSingleQuote(datagen.Truncate(g.faker.FirstName(), 20)),
			escapeSingleQuote(datagen.Truncate(g.faker.LastName(), 30)),
			datagen.Choose(g.faker, []string{"Y", "N"}),
			birthDay,
			birthMonth,
			birthYear,
			fmt.Sprintf("customer%d@example.com", i),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "customer",
				"(c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date_sk)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "customer",
			"(c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date_sk)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateStores(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating stores")

	for i := 1; i <= count; i++ {
		state := datagen.Choose(g.faker, usStates)
		_, err := pool.Exec(ctx, `
            INSERT INTO store (s_store_sk, s_store_id, s_store_name, s_number_employees, s_floor_space,
                s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager,
                s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name,
                s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_percentage)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
        `, i, fmt.Sprintf("AAAAAAAAA%07d", i), fmt.Sprintf("Store #%d", i),
			g.faker.Int(10, 500), g.faker.Int(5000, 100000), "9AM-9PM",
			g.faker.Name(), i%5+1, "Medium", g.faker.Sentence(5), g.faker.Name(),
			i%3+1, fmt.Sprintf("Division %d", i%3+1), 1, "Retail Corp",
			g.faker.Digits(4), g.faker.Street(), "St", "",
			g.faker.City(), datagen.Truncate(g.faker.City(), 30), state, g.faker.Zip(),
			"United States", -5.00, g.faker.Float64(0.05, 0.10))
		if err != nil {
			return err
		}
	}

	logging.Info().Msg("stores complete")
	return nil
}

func (g *Generator) generateWarehouses(ctx context.Context, pool *pgxpool.Pool, count int) error {
	logging.Info().Int("count", count).Msg("Generating warehouses")

	for i := 1; i <= count; i++ {
		state := datagen.Choose(g.faker, usStates)
		_, err := pool.Exec(ctx, `
            INSERT INTO warehouse (w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft,
                w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state,
                w_zip, w_country, w_gmt_offset)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        `, i, fmt.Sprintf("AAAAAAAAA%07d", i), fmt.Sprintf("Warehouse %d", i),
			g.faker.Int(50000, 500000), g.faker.Digits(4), g.faker.Street(), "St", "",
			g.faker.City(), datagen.Truncate(g.faker.City(), 30), state, g.faker.Zip(),
			"United States", -5.00)
		if err != nil {
			return err
		}
	}

	logging.Info().Msg("warehouses complete")
	return nil
}

func (g *Generator) generatePromotions(ctx context.Context, pool *pgxpool.Pool, count, numItems int) error {
	logging.Info().Int("count", count).Msg("Generating promotions")
	batch := make([]string, 0, g.cfg.BatchSize)

	for i := 1; i <= count; i++ {
		itemSK := g.faker.Int(1, numItems)
		batch = append(batch, fmt.Sprintf("(%d, '%s', %d, %d, %d, %.2f, %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', NULL, '%s', '%s')",
			i,
			fmt.Sprintf("AAAAAAAAA%07d", i),
			g.faker.Int(1, 1000),
			g.faker.Int(1001, 2000),
			itemSK,
			g.faker.Float64(100, 10000),
			g.faker.Int(100, 1000),
			fmt.Sprintf("Promo %d", i),
			datagen.Choose(g.faker, []string{"Y", "N"}),
			datagen.Choose(g.faker, []string{"Y", "N"}),
			datagen.Choose(g.faker, []string{"Y", "N"}),
			datagen.Choose(g.faker, []string{"Y", "N"}),
			datagen.Choose(g.faker, []string{"Y", "N"}),
			datagen.Choose(g.faker, []string{"Y", "N"}),
			datagen.Choose(g.faker, []string{"Y", "N"}),
			datagen.Choose(g.faker, []string{"Y", "N"}),
			datagen.Choose(g.faker, []string{"Sale", "Clearance", "New"}),
			datagen.Choose(g.faker, []string{"Y", "N"}),
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "promotion",
				"(p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active)", batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "promotion",
			"(p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active)", batch); err != nil {
			return err
		}
	}

	logging.Info().Msg("promotions complete")
	return nil
}

func (g *Generator) generateStoreSales(ctx context.Context, pool *pgxpool.Pool, count, numItems, numCustomers, numAddresses, numStores, numPromos, numCDemo, numHDemo int) error {
	logging.Info().Int("count", count).Msg("Generating store_sales")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("store_sales", int64(count), int64(count/10))

	for i := 1; i <= count; i++ {
		dateSK := g.faker.Int(1, 2000)
		timeSK := g.faker.Int(0, 86399)
		itemSK := g.faker.Int(1, numItems)
		custSK := g.faker.Int(1, numCustomers)
		addrSK := g.faker.Int(1, numAddresses)
		storeSK := g.faker.Int(1, numStores)
		promoSK := g.faker.Int(1, numPromos)
		cDemoSK := g.faker.Int(1, numCDemo)
		hDemoSK := g.faker.Int(1, numHDemo)
		ticketNum := int64(i)

		qty := g.faker.Int(1, 100)
		wholesale := g.faker.Float64(1, 100)
		listPrice := wholesale * 1.5
		salesPrice := listPrice * (1 - g.faker.Float64(0, 0.3))
		extDiscount := float64(qty) * (listPrice - salesPrice)
		extSales := float64(qty) * salesPrice
		extWholesale := float64(qty) * wholesale
		extList := float64(qty) * listPrice
		extTax := extSales * 0.08
		couponAmt := g.faker.Float64(0, extSales*0.1)
		netPaid := extSales - couponAmt
		netPaidTax := netPaid + extTax
		netProfit := netPaid - extWholesale

		batch = append(batch, fmt.Sprintf("(%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f)",
			dateSK, timeSK, itemSK, custSK, cDemoSK, hDemoSK, addrSK, storeSK, promoSK, ticketNum,
			qty, wholesale, listPrice, salesPrice, extDiscount, extSales, extWholesale, extList, extTax, couponAmt, netPaid, netPaidTax, netProfit,
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "store_sales",
				"(ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "store_sales",
			"(ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateWebSales(ctx context.Context, pool *pgxpool.Pool, count, numItems, numCustomers, numAddresses, numWarehouses, numPromos, numCDemo, numHDemo int) error {
	logging.Info().Int("count", count).Msg("Generating web_sales")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("web_sales", int64(count), int64(count/10))

	for i := 1; i <= count; i++ {
		dateSK := g.faker.Int(1, 2000)
		timeSK := g.faker.Int(0, 86399)
		shipDateSK := dateSK + g.faker.Int(1, 14)
		itemSK := g.faker.Int(1, numItems)
		custSK := g.faker.Int(1, numCustomers)
		addrSK := g.faker.Int(1, numAddresses)
		warehouseSK := g.faker.Int(1, numWarehouses)
		promoSK := g.faker.Int(1, numPromos)
		cDemoSK := g.faker.Int(1, numCDemo)
		hDemoSK := g.faker.Int(1, numHDemo)
		orderNum := int64(i)

		qty := g.faker.Int(1, 100)
		wholesale := g.faker.Float64(1, 100)
		listPrice := wholesale * 1.5
		salesPrice := listPrice * (1 - g.faker.Float64(0, 0.3))
		extDiscount := float64(qty) * (listPrice - salesPrice)
		extSales := float64(qty) * salesPrice
		extWholesale := float64(qty) * wholesale
		extList := float64(qty) * listPrice
		extTax := extSales * 0.08
		couponAmt := g.faker.Float64(0, extSales*0.1)
		extShip := g.faker.Float64(5, 50)
		netPaid := extSales - couponAmt
		netPaidTax := netPaid + extTax
		netPaidShip := netPaid + extShip
		netPaidShipTax := netPaidShip + extTax
		netProfit := netPaid - extWholesale

		batch = append(batch, fmt.Sprintf("(%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, NULL, NULL, NULL, %d, %d, %d, %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f)",
			dateSK, timeSK, shipDateSK, itemSK, custSK, cDemoSK, hDemoSK, addrSK, custSK, cDemoSK, hDemoSK, addrSK,
			warehouseSK, promoSK, orderNum, qty, wholesale, listPrice, salesPrice, extDiscount, extSales,
			extWholesale, extList, extTax, couponAmt, extShip, netPaid, netPaidTax, netPaidShip, netPaidShipTax, netProfit,
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "web_sales",
				"(ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "web_sales",
			"(ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit)", batch); err != nil {
			return err
		}
		progress.Update(int64(len(batch)))
	}
	progress.Done()
	return nil
}

func (g *Generator) generateCatalogSales(ctx context.Context, pool *pgxpool.Pool, count, numItems, numCustomers, numAddresses, numWarehouses, numPromos, numCDemo, numHDemo int) error {
	logging.Info().Int("count", count).Msg("Generating catalog_sales")
	batch := make([]string, 0, g.cfg.BatchSize)
	progress := datagen.NewProgressReporter("catalog_sales", int64(count), int64(count/10))

	for i := 1; i <= count; i++ {
		dateSK := g.faker.Int(1, 2000)
		timeSK := g.faker.Int(0, 86399)
		shipDateSK := dateSK + g.faker.Int(1, 14)
		itemSK := g.faker.Int(1, numItems)
		custSK := g.faker.Int(1, numCustomers)
		addrSK := g.faker.Int(1, numAddresses)
		warehouseSK := g.faker.Int(1, numWarehouses)
		promoSK := g.faker.Int(1, numPromos)
		cDemoSK := g.faker.Int(1, numCDemo)
		hDemoSK := g.faker.Int(1, numHDemo)
		orderNum := int64(i)

		qty := g.faker.Int(1, 100)
		wholesale := g.faker.Float64(1, 100)
		listPrice := wholesale * 1.5
		salesPrice := listPrice * (1 - g.faker.Float64(0, 0.3))
		extDiscount := float64(qty) * (listPrice - salesPrice)
		extSales := float64(qty) * salesPrice
		extWholesale := float64(qty) * wholesale
		extList := float64(qty) * listPrice
		extTax := extSales * 0.08
		couponAmt := g.faker.Float64(0, extSales*0.1)
		extShip := g.faker.Float64(5, 50)
		netPaid := extSales - couponAmt
		netPaidTax := netPaid + extTax
		netPaidShip := netPaid + extShip
		netPaidShipTax := netPaidShip + extTax
		netProfit := netPaid - extWholesale

		batch = append(batch, fmt.Sprintf("(%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, NULL, NULL, NULL, %d, %d, %d, %d, %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f)",
			dateSK, timeSK, shipDateSK, custSK, cDemoSK, hDemoSK, addrSK, custSK, cDemoSK, hDemoSK, addrSK,
			warehouseSK, itemSK, promoSK, orderNum, qty, wholesale, listPrice, salesPrice, extDiscount, extSales,
			extWholesale, extList, extTax, couponAmt, extShip, netPaid, netPaidTax, netPaidShip, netPaidShipTax, netProfit,
		))

		if len(batch) >= g.cfg.BatchSize {
			if err := g.executeBatchInsert(ctx, pool, "catalog_sales",
				"(cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit)", batch); err != nil {
				return err
			}
			progress.Update(int64(len(batch)))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := g.executeBatchInsert(ctx, pool, "catalog_sales",
			"(cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit)", batch); err != nil {
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
