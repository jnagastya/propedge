-- Run this in the Supabase SQL editor to set up user accounts
-- https://supabase.com/dashboard → SQL Editor

-- User profiles table (linked to Supabase Auth users)
create table if not exists user_profiles (
  id           uuid primary key references auth.users(id) on delete cascade,
  display_name text,
  balance      numeric(12,2) not null default 1000,
  bets         jsonb not null default '[]',
  preferences  jsonb not null default '{}',
  updated_at   timestamptz default now()
);

-- Row-level security: users can only read/write their own row
alter table user_profiles enable row level security;

-- Allow the service role (server) to read/write all rows
-- (The server uses the service key, which bypasses RLS automatically)

-- For reference: to allow users to access their own rows via anon key
-- (not needed since we use service key server-side)
-- create policy "own row" on user_profiles
--   using (auth.uid() = id)
--   with check (auth.uid() = id);
