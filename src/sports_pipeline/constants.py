"""Project-wide constants."""

SPORT_SOCCER = "soccer"
SPORT_BASKETBALL = "basketball"

# Market types
MARKET_GAME_OUTCOME = "game_outcome"
MARKET_TOTAL = "total"
MARKET_PLAYER_PROP = "player_prop"
MARKET_FUTURE = "future"

# Data layers
LAYER_BRONZE = "bronze"
LAYER_SILVER = "silver"
LAYER_GOLD = "gold"

# Edge confidence levels
CONFIDENCE_HIGH = "high"
CONFIDENCE_MEDIUM = "medium"
CONFIDENCE_LOW = "low"

# Suggested sides
SIDE_YES = "YES"
SIDE_NO = "NO"

# FBref base URL
FBREF_BASE_URL = "https://fbref.com/en"

# Kalshi API
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
KALSHI_SPORTS_CATEGORY = "Sports"

# Kafka topics
TOPIC_TICKS = "kalshi.ticks"
TOPIC_TRADES = "kalshi.trades"
TOPIC_BOOK = "kalshi.book"
TOPIC_FILLS = "kalshi.fills"
TOPIC_LIFECYCLE = "kalshi.lifecycle"
TOPIC_EDGES = "edges"
TOPIC_ORDERS = "orders"
TOPIC_RISK = "risk"
TOPIC_SYSTEM = "system"
