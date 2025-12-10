# Usage Profiles

Usage profiles simulate different patterns of database activity based on
time of day and day of week. They make load simulations more realistic by
varying the query rate throughout the day.

## Available Profiles

### Local Office (`local-office`)

Simulates a business that operates during standard office hours in a single
timezone.

**Pattern:**

```
Activity Level
100% |        ████████      ████████
 80% |      ██        ██  ██        ██
 60% |    ██            ██            ██
 40% |  ██                              ██
 20% |██                                  ██████
  0% +──────────────────────────────────────────
     6AM  8AM  10AM 12PM  2PM  4PM  6PM  8PM 10PM
```

**Characteristics:**

| Time Period | Activity Level |
|------------|----------------|
| 6AM - 8AM | Ramp up to 100% |
| 8AM - 10:30AM | 100% (peak) |
| 10:30AM - 11AM | 70% (morning break) |
| 11AM - 12PM | 100% (peak) |
| 12PM - 1PM | 50% (lunch) |
| 1PM - 3:30PM | 100% (peak) |
| 3:30PM - 4PM | 70% (afternoon break) |
| 4PM - 6PM | 100% (peak) |
| 6PM - 10PM | 20% (evening) |
| 10PM - 6AM | 5% (overnight batch) |

**Weekend:** 10% of weekday activity

**Best for:**

- Single-location businesses
- Office applications
- Internal tools

**Example:**

```bash
pgedge-loadgen run \
    --app wholesale \
    --connections 50 \
    --profile local-office \
    --timezone "America/New_York"
```

---

### Global Enterprise (`global`)

Simulates a business operating 24/7 with activity following business hours
around the world.

**Pattern:**

```
Activity Level
100% |████████████████████████████████████████████
 80% |
 60% |
 40% |
 30% |────────────────────────────────────────────
  0% +──────────────────────────────────────────────
     12AM      6AM      12PM      6PM      12AM
               (UTC)
```

**Characteristics:**

| Period | Activity Level |
|--------|----------------|
| Minimum | 30% (never drops below) |
| Rolling peaks | Follow business hours across timezones |
| Quiet hours | 2AM - 4AM UTC (slight reduction) |
| Weekend | 60% of weekday activity |

**Best for:**

- Global enterprises
- SaaS applications
- Services with international users

**Example:**

```bash
pgedge-loadgen run \
    --app brokerage \
    --connections 100 \
    --profile global
```

---

### Regional Online Store (`store-regional`)

Simulates a regional e-commerce site with evening peak activity.

**Pattern:**

```
Activity Level
100% |                          ██████████
 80% |                        ██          ██
 60% |              ██████████              ██
 40% |        ██████
 20% |████████                                  ██
 15% |                                            ████
  0% +──────────────────────────────────────────────
     12AM    6AM    12PM    5PM    10PM    12AM
```

**Characteristics:**

| Time Period | Activity Level |
|------------|----------------|
| 12AM - 6AM | 15% (overnight) |
| 6AM - 12PM | 40% (morning) |
| 12PM - 5PM | 60% (afternoon) |
| 5PM - 10PM | 100% (evening peak) |
| 10PM - 12AM | 70% (late night) |

**Weekend:** 120% of weekday activity (higher shopping)

**Best for:**

- Regional e-commerce
- Consumer-facing applications
- Retail workloads

**Example:**

```bash
pgedge-loadgen run \
    --app ecommerce \
    --connections 75 \
    --profile store-regional \
    --timezone "America/Los_Angeles"
```

---

### Global Online Store (`store-global`)

Simulates a global e-commerce platform with multiple regional peaks.

**Pattern:**

```
Activity Level
100% |  ██    ████    ████    ████    ██
 80% |████  ██    ████    ████    ████  ████
 60% |    ██        ██        ██        ██
 40% |────────────────────────────────────────────
  0% +──────────────────────────────────────────────
     12AM      6AM      12PM      6PM      12AM
               (UTC)
```

**Characteristics:**

| Period | Activity Level |
|--------|----------------|
| Base activity | 40% minimum |
| Regional peaks | Evening hours in major markets |
| Weekend | 110% of weekday activity |

**Major market peaks (local evening time):**

- Asia-Pacific: 10PM - 12AM JST
- Europe: 6PM - 10PM CET
- Americas: 5PM - 10PM EST/PST

**Best for:**

- Global e-commerce
- International retail
- Consumer applications with worldwide users

**Example:**

```bash
pgedge-loadgen run \
    --app ecommerce \
    --connections 200 \
    --profile store-global
```

---

## Profile Comparison

| Profile | Min Activity | Max Activity | Weekend | Use Case |
|---------|-------------|--------------|---------|----------|
| `local-office` | 5% | 100% | 10% | Single-location business |
| `global` | 30% | 100% | 60% | 24/7 enterprise |
| `store-regional` | 15% | 100% | 120% | Regional e-commerce |
| `store-global` | 40% | 100% | 110% | Global retail |

## Timezone Configuration

The timezone setting determines when profile patterns apply:

```bash
# Use system timezone (default)
pgedge-loadgen run --profile local-office

# Specify timezone explicitly
pgedge-loadgen run --profile local-office --timezone "Europe/London"

# Use UTC
pgedge-loadgen run --profile local-office --timezone "UTC"
```

Common timezone identifiers:

- `America/New_York` - US Eastern
- `America/Los_Angeles` - US Pacific
- `Europe/London` - UK
- `Europe/Paris` - Central European
- `Asia/Tokyo` - Japan
- `Asia/Shanghai` - China
- `Australia/Sydney` - Australia Eastern

## How Profiles Affect Load

Profiles multiply the base query rate by the current activity level:

```
actual_rate = base_rate × profile_multiplier
```

For example, with 50 connections and `local-office` profile at lunch time
(50% activity):

- Base rate: ~500 queries/second
- Effective rate: ~250 queries/second

## Next Steps

- [Configuration](configuration.md) - Save profile settings in config file
- [CLI Reference](cli-reference.md) - All profile-related options
