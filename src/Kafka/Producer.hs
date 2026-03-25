{-# language
    BangPatterns
  , LambdaCase
  , OverloadedStrings
  #-}

-- | High-performance batching Kafka producer.
--
-- Messages are enqueued to per-broker TBQueues and batched
-- automatically based on linger.ms, batch.size, and batch.num.messages
-- thresholds — matching librdkafka's producer batching strategy.
--
-- The broker thread handles:
--   - Batch accumulation and flushing (Phase 4)
--   - Correlation ID tracking and response dispatch (Phase 2)
--   - Reconnection with exponential backoff + jitter (Phase 6)
--   - Backpressure via bounded TBQueues (Phase 7)
module Kafka.Producer
  ( KafkaProducer(..)
  , newProducer
  , closeProducer
  , produce
  , produceAsync
  , flushProducer
  ) where

import Control.Concurrent.MVar (MVar, newMVar, modifyMVar)
import Control.Concurrent.STM
import Data.Int (Int32)
import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IM
import Data.IORef (IORef, newIORef, atomicModifyIORef', readIORef)
import Data.Map.Strict (Map)
import Data.Primitive.ByteArray (ByteArray)

import qualified Data.Map.Strict as Map

import Kafka.Common
import Kafka.Client
import Kafka.Internal.Broker (enqueueProduce, BrokerOp(..), BrokerEnv(..))
import Kafka.Internal.Config

------------------------------------------------------------------------
-- Types
------------------------------------------------------------------------

data KafkaProducer = KafkaProducer
  { kpClient   :: !KafkaClient
  , kpCounters :: !(MVar (Map TopicName (IORef Int)))
    -- ^ Per-topic round-robin partition counters.
    -- MVar protects the map; IORef per-topic is contention-free.
  , kpConfig   :: !ClientConfig
  }

------------------------------------------------------------------------
-- Lifecycle
------------------------------------------------------------------------

-- | Create a new producer backed by a 'KafkaClient'.
--
-- The client must already be created and connected.
newProducer :: KafkaClient -> ClientConfig -> IO KafkaProducer
newProducer client cfg = do
  counters <- newMVar Map.empty
  pure KafkaProducer
    { kpClient   = client
    , kpCounters = counters
    , kpConfig   = cfg
    }

-- | Close the producer. Does NOT close the underlying 'KafkaClient'.
closeProducer :: KafkaProducer -> IO ()
closeProducer _ = pure ()
  -- Nothing to clean up — the broker threads keep running.
  -- Call closeClient on the KafkaClient to shut everything down.

------------------------------------------------------------------------
-- Produce API
------------------------------------------------------------------------

-- | Produce a message synchronously. Blocks until the broker confirms
-- delivery (or returns an error).
--
-- Partition selection is round-robin per topic.
-- Batching happens transparently in the broker thread.
produce :: KafkaProducer -> TopicName -> ByteArray -> IO (Either KafkaException ())
produce producer topic payload = do
  resultVar <- produceAsync producer topic payload
  atomically $ readTMVar resultVar

-- | Produce a message asynchronously. Returns a TMVar that will be
-- filled when the broker confirms delivery.
--
-- This is the non-blocking variant — the caller can choose to:
--   - Block:         atomically (readTMVar resultVar)
--   - Fire-and-forget: discard the TMVar
--   - Batch-wait:    collect multiple TMVars and wait on all
produceAsync :: KafkaProducer -> TopicName -> ByteArray
            -> IO (TMVar (Either KafkaException ()))
produceAsync producer topic payload = do
  -- Step 1: Ensure we have metadata for this topic
  partCount <- ensureTopicMetadata producer topic
  case partCount of
    Left err -> do
      -- Return a pre-filled TMVar with the error
      v <- newTMVarIO (Left err)
      pure v
    Right count -> do
      -- Step 2: Select partition (round-robin)
      part <- nextPartition producer topic count

      -- Step 3: Find the leader broker for this partition
      mBroker <- leaderBrokerFor (kpClient producer) topic part
      case mBroker of
        Nothing -> do
          v <- newTMVarIO (Left (KafkaException "no broker available"))
          pure v
        Just env -> do
          -- Step 4: Enqueue to broker thread (batching + send happen there)
          enqueueProduce env topic part payload

------------------------------------------------------------------------
-- Flush
------------------------------------------------------------------------

-- | Flush all pending produce messages across all brokers.
-- Blocks until every broker thread has flushed its current batch.
flushProducer :: KafkaProducer -> IO ()
flushProducer producer = do
  brokers <- readTVarIO (kcBrokers (kpClient producer))
  doneVars <- mapM flushOne (IM.elems brokers)
  mapM_ (\v -> atomically $ readTMVar v) doneVars
  where
    flushOne env = do
      done <- newEmptyTMVarIO
      atomically $ writeTBQueue (beOps env) (BrokerFlush done)
      pure done

------------------------------------------------------------------------
-- Internal: metadata
------------------------------------------------------------------------

-- | Ensure we have partition metadata for a topic.
-- Fetches from broker if not cached.
ensureTopicMetadata :: KafkaProducer -> TopicName -> IO (Either KafkaException Int32)
ensureTopicMetadata producer topic = do
  mCount <- partitionCountFor (kpClient producer) topic
  case mCount of
    Just count -> pure (Right count)
    Nothing -> do
      result <- refreshTopicMetadata (kpClient producer) topic
      case result of
        Left err -> pure (Left err)
        Right () -> do
          mCount' <- partitionCountFor (kpClient producer) topic
          case mCount' of
            Just count -> pure (Right count)
            Nothing -> pure (Left (KafkaException
              "topic not found after metadata refresh"))

------------------------------------------------------------------------
-- Internal: partitioning
------------------------------------------------------------------------

-- | Round-robin partition selection.
-- Each topic has its own counter (IORef Int) protected by the MVar map.
-- The counter is incremented atomically; the MVar is only held briefly
-- when adding a new topic to the map.
nextPartition :: KafkaProducer -> TopicName -> Int32 -> IO Int32
nextPartition producer topic count = do
  ref <- modifyMVar (kpCounters producer) $ \m ->
    case Map.lookup topic m of
      Just ref -> pure (m, ref)
      Nothing -> do
        ref <- newIORef 0
        pure (Map.insert topic ref m, ref)
  atomicModifyIORef' ref $ \n ->
    let n' = if n + 1 >= fromIntegral count then 0 else n + 1
    in (n', fromIntegral n)
