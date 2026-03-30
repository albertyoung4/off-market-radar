-- Add transaction tracking columns to fb_deal_posts
ALTER TABLE fb_deal_posts ADD COLUMN IF NOT EXISTS transaction_status TEXT;
ALTER TABLE fb_deal_posts ADD COLUMN IF NOT EXISTS transaction_sold_date TIMESTAMPTZ;
ALTER TABLE fb_deal_posts ADD COLUMN IF NOT EXISTS transaction_sold_price NUMERIC;
ALTER TABLE fb_deal_posts ADD COLUMN IF NOT EXISTS transaction_sale_source TEXT;
ALTER TABLE fb_deal_posts ADD COLUMN IF NOT EXISTS transaction_checked_at TIMESTAMPTZ;

-- Index for transaction queries
CREATE INDEX IF NOT EXISTS idx_fb_deal_posts_transaction_status ON fb_deal_posts(transaction_status);
CREATE INDEX IF NOT EXISTS idx_fb_deal_posts_transaction_sold_date ON fb_deal_posts(transaction_sold_date);

-- Add transaction rate columns to posters table
ALTER TABLE posters ADD COLUMN IF NOT EXISTS transaction_rate NUMERIC;
ALTER TABLE posters ADD COLUMN IF NOT EXISTS total_matched_deals INTEGER DEFAULT 0;
ALTER TABLE posters ADD COLUMN IF NOT EXISTS total_sold_deals INTEGER DEFAULT 0;
