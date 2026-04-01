-- Run this in your Supabase SQL Editor to create the HC analysis table.
-- Stores House Canary value/rental estimates for matched deal properties.

CREATE TABLE IF NOT EXISTS public.hc_property_data (
  attom_id TEXT PRIMARY KEY,
  hc_value_estimate INTEGER,
  hc_rental_avm_lower INTEGER,
  hc_rental_avm_upper INTEGER,
  city TEXT,
  state TEXT,
  county TEXT,
  synced_at TIMESTAMPTZ DEFAULT NOW()
);

-- Allow anon role to read
CREATE POLICY "anon_read_hc_data"
ON public.hc_property_data
FOR SELECT
TO anon
USING (true);

-- Enable RLS
ALTER TABLE public.hc_property_data ENABLE ROW LEVEL SECURITY;
