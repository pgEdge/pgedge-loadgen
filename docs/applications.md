# Applications

pgedge-loadgen includes seven fictional applications, each designed to
simulate realistic database workloads. Four are based on industry-standard
TPC benchmarks, and three use pgvector for semantic search capabilities.

## TPC-Based Applications

### Wholesale Supplier (TPC-C Based)

**Application name:** `wholesale`

A classic OLTP workload simulating a wholesale supplier with warehouses,
districts, customers, orders, and inventory management.

**Schema (9 tables):**

- `warehouse` - Distribution centers
- `district` - Districts within warehouses
- `customer` - Customer accounts
- `history` - Payment history
- `orders` - Order headers
- `order_line` - Order line items
- `new_orders` - Pending orders queue
- `item` - Product catalog
- `stock` - Inventory per warehouse

**Query Mix:**

| Query | Weight | Type | Description |
|-------|--------|------|-------------|
| New Order | 45% | Write | Create new customer orders |
| Payment | 43% | Write | Process customer payments |
| Order Status | 4% | Read | Check order status |
| Delivery | 4% | Write | Process deliveries |
| Stock Level | 4% | Read | Check inventory levels |

**Use Cases:**

- Testing OLTP performance
- Validating transaction isolation
- Testing write-heavy workloads

**Size Maintenance:**

The wholesale application continuously creates new orders, which can cause
unbounded database growth over long-running tests. By default, automatic
size maintenance is enabled: every 5 minutes, if the orders and order_line
tables exceed 110% of the target size specified during `init`, the oldest
orders are deleted to bring the database back to the target size. This
simulates real-world data archival practices.

To disable this behaviour and allow unbounded growth, use the
`--no-maintain-size` flag when running:

```bash
pgedge-loadgen run --app wholesale --no-maintain-size
```

---

### Analytics Warehouse (TPC-H Based)

**Application name:** `analytics`

An OLAP/Decision support workload with complex analytical queries on sales
and supplier data.

**Schema (8 tables):**

- `region` - Geographic regions
- `nation` - Countries
- `supplier` - Suppliers
- `customer` - Customer accounts
- `part` - Parts catalog
- `partsupp` - Part-supplier relationships
- `orders` - Order headers
- `lineitem` - Order line items

**Query Mix (22 analytical queries):**

| Query | Weight | Description |
|-------|--------|-------------|
| Pricing Summary | 4% | Revenue summary by return flag |
| Minimum Cost Supplier | 4% | Find cheapest supplier for parts |
| Shipping Priority | 5% | Unshipped orders by priority |
| Order Priority | 5% | Order priority distribution |
| Local Supplier Volume | 5% | Revenue from local suppliers |
| Forecasting Revenue | 5% | Revenue by discount analysis |
| Volume Shipping | 5% | Shipping volume between nations |
| National Market Share | 4% | Market share analysis |
| Product Type Profit | 4% | Profit margins by product |
| Returned Item Reporting | 5% | Analysis of returned items |
| Important Stock ID | 5% | Identify key inventory items |
| Shipping Modes | 5% | Order analysis by ship mode |
| Customer Distribution | 5% | Customer segmentation |
| Promotion Effect | 5% | Promotion impact analysis |
| Top Supplier | 4% | Top suppliers by revenue |
| Parts/Supplier | 5% | Part-supplier relationships |
| Small Quantity Orders | 5% | Small order analysis |
| Large Volume Customer | 4% | High-value customer analysis |
| Discounted Revenue | 5% | Revenue impact of discounts |
| Potential Part Promotion | 4% | Parts for promotion |
| Suppliers Kept Waiting | 4% | Supplier delivery analysis |
| Global Sales Opportunity | 4% | International sales potential |

**Use Cases:**

- Testing analytical query performance
- Validating parallel query execution
- Testing read-heavy workloads with complex joins

---

### Brokerage Firm (TPC-E Based)

**Application name:** `brokerage`

A mixed OLTP workload simulating a stock brokerage with customers, brokers,
accounts, securities, and trade transactions.

**Schema (key tables):**

- `customer` - Customer accounts
- `customer_account` - Trading accounts
- `broker` - Broker information
- `security` - Stock securities
- `exchange` - Stock exchanges
- `trade` - Trade transactions
- `trade_history` - Trade audit trail
- `holding` - Current positions
- `watch_list` - Customer watchlists
- `company` - Company information

**Query Mix:**

| Query | Weight | Type | Description |
|-------|--------|------|-------------|
| Broker Volume | 5% | Read | Broker trading performance |
| Customer Position | 13% | Read | Portfolio positions and values |
| Market Feed | 1% | Write | Update security prices |
| Market Watch | 18% | Read | Check watched securities |
| Security Detail | 14% | Read | Detailed security information |
| Trade Lookup | 8% | Read | Historical trade queries |
| Trade Order | 10% | Write | Place new trade orders |
| Trade Result | 10% | Write | Process completed trades |
| Trade Status | 19% | Read | Check order status |
| Trade Update | 2% | Write | Modify pending orders |

**Use Cases:**

- Testing mixed read/write workloads
- Validating complex transaction logic
- Testing financial application patterns

---

### Retail Analytics (TPC-DS Based)

**Application name:** `retail`

A complex decision support workload for retail scenarios with multi-channel
sales analysis.

**Schema (key tables):**

- **Fact tables:** `store_sales`, `web_sales`, `catalog_sales`, `inventory`
- **Dimension tables:** `customer`, `item`, `store`, `promotion`, `date_dim`,
  `time_dim`, `warehouse`, and more

**Query Mix:**

| Query | Weight | Description |
|-------|--------|-------------|
| Store Sales by Date | 15% | Aggregate sales by date |
| Store Sales by Item | 12% | Top selling items |
| Store Sales by Customer | 10% | Customer purchase patterns |
| Web Sales Analysis | 12% | Web channel performance |
| Catalog Sales Analysis | 10% | Catalog channel performance |
| Cross-Channel Sales | 8% | Compare all channels |
| Customer Demographics | 8% | Customer analysis |
| Promotion Effect | 7% | Promotion effectiveness |
| Inventory Analysis | 6% | Warehouse inventory |
| Store Comparison | 6% | Store performance comparison |
| Time Series Sales | 6% | Sales trend analysis |

**Use Cases:**

- Testing complex analytical workloads
- Validating multi-dimensional queries
- Testing data warehouse patterns

---

## pgvector Applications

These applications require the pgvector extension for semantic search
capabilities.

### E-commerce (Product Catalog)

**Application name:** `ecommerce`

An online store with semantic product search using vector embeddings.

**Schema:**

- `category` - Product categories
- `brand` - Product brands
- `product` - Products with embedding vectors
- `customer` - Customer accounts
- `cart` - Shopping carts
- `cart_item` - Cart contents
- `orders` - Order headers
- `order_item` - Order line items
- `review` - Product reviews with sentiment vectors

**Query Mix:**

| Query | Weight | Type | Description |
|-------|--------|------|-------------|
| Semantic Search | 25% | Read | Vector similarity product search |
| Similar Products | 15% | Read | Find similar products (KNN) |
| Category Browse | 15% | Read | Traditional category queries |
| Product Detail | 15% | Read | Single product lookup |
| Add to Cart | 10% | Write | Shopping cart operations |
| Place Order | 5% | Write | Order placement |
| Submit Review | 5% | Write | Review submission |
| Order History | 10% | Read | Customer order history |

**Embedding Configuration:**

```bash
# Random embeddings (default, fast)
pgedge-loadgen init --app ecommerce --embedding-mode random

# Using pgedge-vectorizer
pgedge-loadgen init --app ecommerce \
    --embedding-mode vectorizer \
    --vectorizer-url "http://localhost:8080"

# Using OpenAI
pgedge-loadgen init --app ecommerce \
    --embedding-mode openai \
    --openai-api-key "sk-..."
```

---

### Knowledge Base

**Application name:** `knowledgebase`

A FAQ/Documentation system with semantic question matching.

**Schema:**

- `category` - Article categories
- `article` - KB articles with content embeddings
- `article_section` - Article sections with embeddings
- `tag` - Article tags
- `article_tag` - Article-tag relationships
- `search_log` - Search history with query embeddings
- `feedback` - Article helpfulness ratings
- `kb_user` - Support agents and customers

**Query Mix:**

| Query | Weight | Type | Description |
|-------|--------|------|-------------|
| Semantic Search | 35% | Read | Find relevant articles |
| Similar Questions | 20% | Read | Match to previous searches |
| Category Browse | 15% | Read | Browse by category |
| Article View | 15% | Read | Read full article |
| Submit Feedback | 10% | Write | Rate article helpfulness |
| Admin Update | 5% | Write | Article CRUD operations |

---

### Document Management

**Application name:** `docmgmt`

An enterprise document management system with semantic search and similarity
detection.

**Schema:**

- `dm_user` - System users
- `folder` - Folder hierarchy
- `document` - Document metadata with embeddings
- `document_version` - Version history
- `document_chunk` - Chunked content with embeddings
- `tag` - Document tags
- `document_tag` - Document-tag relationships
- `permission` - Access control
- `audit_log` - Access audit trail

**Query Mix:**

| Query | Weight | Type | Description |
|-------|--------|------|-------------|
| Semantic Search | 30% | Read | Find documents by content |
| Similar Documents | 15% | Read | Find related documents |
| Folder Browse | 15% | Read | Navigate folder hierarchy |
| Document Retrieve | 15% | Read | Fetch document content |
| Version History | 5% | Read | View document versions |
| Upload Document | 10% | Write | Add new documents |
| Update Document | 5% | Write | Modify existing documents |
| Permission Check | 5% | Read | Access control queries |

---

## Choosing an Application

| If you need... | Choose |
|----------------|--------|
| Classic OLTP workload | `wholesale` |
| Analytical queries | `analytics` |
| Mixed read/write | `brokerage` |
| Complex decision support | `retail` |
| Semantic search testing | `ecommerce`, `knowledgebase`, `docmgmt` |
| pgvector validation | `ecommerce`, `knowledgebase`, `docmgmt` |

## Next Steps

- [Usage Profiles](profiles.md) - Configure temporal patterns
- [Configuration](configuration.md) - Set up configuration file
- [CLI Reference](cli-reference.md) - Command details
