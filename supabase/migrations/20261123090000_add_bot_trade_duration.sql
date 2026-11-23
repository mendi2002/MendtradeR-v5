-- Separa a duração de cada contrato da duração total da sessão automática.
-- A sessão continua limitada a 3 dias; os contratos podem ser configurados até 30 dias.
ALTER TABLE public.bot_settings
  ADD COLUMN IF NOT EXISTS trade_duration_seconds integer NOT NULL DEFAULT 60;

ALTER TABLE public.bot_settings
  DROP CONSTRAINT IF EXISTS bot_settings_trade_duration_seconds_check;

ALTER TABLE public.bot_settings
  ADD CONSTRAINT bot_settings_trade_duration_seconds_check
  CHECK (trade_duration_seconds IN (60, 300, 900, 1800, 3600, 14400, 86400, 604800, 2592000));
