{-# language BangPatterns #-}

-- | Exponential backoff with jitter for broker reconnection.
--
-- Algorithm ported from librdkafka (rdkafka_broker.c:2221-2269):
--   1. Initial backoff = reconnect.backoff.ms (default 100)
--   2. After each failure: backoff *= 2, capped at reconnect.backoff.max.ms
--   3. Jitter: random value between 75% and 150% of current backoff
--   4. On successful connect: reset to initial
module Kafka.Internal.Reconnect
  ( ReconnectState
  , newReconnectState
  , nextBackoffDelay
  , resetBackoff
  ) where

import Data.Bits (xor, shiftL, shiftR)
import Data.IORef (IORef, atomicModifyIORef', modifyIORef')
import Data.Word (Word64)

-- | Mutable reconnection state. Caller owns the IORef.
data ReconnectState = ReconnectState
  { rsBackoffMs  :: {-# UNPACK #-} !Int  -- current backoff (doubles on failure)
  , rsInitialMs  :: {-# UNPACK #-} !Int  -- initial value (reset target)
  , rsMaxMs      :: {-# UNPACK #-} !Int  -- upper cap
  , rsPrng       :: {-# UNPACK #-} !Word64  -- xorshift64 state
  }

newReconnectState :: Int -> Int -> Word64 -> ReconnectState
newReconnectState initial maxBackoff seed = ReconnectState
  { rsBackoffMs = initial
  , rsInitialMs = initial
  , rsMaxMs     = maxBackoff
  , rsPrng      = if seed == 0 then 0x853c49e6748fea9b else seed
  }

-- | Compute the next backoff delay in microseconds, update state.
-- Returns (delay_us, new_state).
-- Mirrors librdkafka's rd_jitter(75, 150) applied to the current backoff.
nextBackoffDelay :: IORef ReconnectState -> IO Int
nextBackoffDelay ref = atomicModifyIORef' ref $ \st ->
  let (jitterPct, prng') = xorshift64Range 75 150 (rsPrng st)
      delayMs = (rsBackoffMs st * jitterPct) `div` 100
      delayUs = delayMs * 1000
      nextBackoff = min (rsBackoffMs st * 2) (rsMaxMs st)
  in (st { rsBackoffMs = nextBackoff, rsPrng = prng' }, delayUs)

-- | Reset backoff to initial value after a successful connection.
resetBackoff :: IORef ReconnectState -> IO ()
resetBackoff ref = modifyIORef' ref $ \st ->
  st { rsBackoffMs = rsInitialMs st }

-- | Xorshift64 PRNG step. Returns next state.
xorshift64 :: Word64 -> Word64
xorshift64 !s0 =
  let s1 = xor s0 (shiftL s0 13)
      s2 = xor s1 (shiftR s1 7)
      s3 = xor s2 (shiftL s2 17)
  in s3

-- | Generate a random Int in [lo, hi] using xorshift64.
-- Returns (value, next_prng_state).
xorshift64Range :: Int -> Int -> Word64 -> (Int, Word64)
xorshift64Range lo hi prng =
  let prng' = xorshift64 prng
      range = hi - lo + 1
      val = lo + fromIntegral (prng' `mod` fromIntegral range)
  in (val, prng')
