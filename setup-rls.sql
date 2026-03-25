-- Run this in your Supabase SQL Editor to allow the public site to read processed deals.
-- This adds a SELECT policy for the anon role on processed (non-pending) deals only.

CREATE POLICY "anon_read_processed_deals"
ON public.fb_deal_posts
FOR SELECT
TO anon
USING (match_status != 'pending' OR match_status IS NULL);
