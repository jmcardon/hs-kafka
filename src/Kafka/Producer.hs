{-# language
    BangPatterns
  , LambdaCase
  , OverloadedStrings
  #-}

-- | High-performance Kafka producer with delivery reports.
--
-- Three delivery report mechanisms (can coexist):
--
-- 1. __Global callback__: set via 'ProducerConfig'. Called from the broker
--    thread for every delivered/failed message. Fast but blocks the broker
--    thread — keep it short.
--
-- 2. __Per-message callback__: use 'produceWithCallback'. The callback is
--    invoked from the broker thread for that specific message.
--
-- 3. __Poll-based__: call 'pollEvents' from any thread. Delivery reports
--    accumulate in a bounded queue; pollEvents drains it with a timeout.
--    Best for "produce in thread A, process reports in thread B."
--
-- All three are thread-safe. You can produce from multiple threads and
-- poll from another — no locks, pure STM.
module Kafka.Producer
  ( -- * Producer handle
    KafkaProducer(..)
  , ProducerConfig(..)
  , defaultProducerConfig
    -- * Lifecycle
  , newProducer
  , closeProducer
  , flushProducer
    -- * Producing
  , produce
  , produceAsync
  , produceWithCallback
    -- * Delivery reports
  , pollEvents
    -- * Types (re-export)
  , module Kafka.Producer.Types
  ) where

import Control.Concurrent.MVar (MVar, newMVar, modifyMVar)
import Control.Concurrent.STM
import Data.ByteString (ByteString)
import Data.Int (Int32, Int64, Int16)
import Data.IORef (IORef, newIORef, atomicModifyIORef')
import Data.Map.Strict (Map)

import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as Map

import Kafka.Common
import Kafka.Client
import Kafka.Internal.Broker
import Kafka.Internal.Config
import Kafka.Internal.InitProducerId.Request (initProducerIdRequest)
import Kafka.Internal.InitProducerId.Response (InitProducerIdResponse(..),
  parseInitProducerIdResponseV4)
import Kafka.Producer.Types
import qualified Kafka.Internal.Wire as Wire

------------------------------------------------------------------------
-- Configuration
------------------------------------------------------------------------

-- | Producer configuration, including delivery report settings.
data ProducerConfig = ProducerConfig
  { pcClientConfig  :: !ClientConfig
  , pcDeliveryCallback :: !(Maybe (DeliveryReport -> IO ()))
    -- ^ Global delivery report callback. Called from the broker thread
    -- for every message. Keep it fast — it blocks the broker thread.
  , pcDeliveryQueueSize :: {-# UNPACK #-} !Int
    -- ^ Size of the delivery report queue for pollEvents. Default: 10000.
  }

defaultProducerConfig :: ClientConfig -> ProducerConfig
defaultProducerConfig cfg = ProducerConfig
  { pcClientConfig = cfg
  , pcDeliveryCallback = Nothing
  , pcDeliveryQueueSize = 10000
  }

------------------------------------------------------------------------
-- Producer handle
------------------------------------------------------------------------

data KafkaProducer = KafkaProducer
  { kpClient         :: !KafkaClient
  , kpCounters       :: !(MVar (Map TopicName (IORef Int)))
  , kpConfig         :: !ClientConfig
  , kpIdempotent     :: !(Maybe IdempotentState)
  , kpDeliveryQueue  :: !(TBQueue DeliveryReport)
    -- ^ Bounded queue for pollEvents-based delivery reports.
  , kpDeliveryCallback :: !(Maybe (DeliveryReport -> IO ()))
    -- ^ Global callback, invoked from broker thread.
  }

-- | Idempotent producer state.
data IdempotentState = IdempotentState
  { isProducerId    :: {-# UNPACK #-} !Int64
  , isProducerEpoch :: {-# UNPACK #-} !Int16
  , isSequences     :: !(MVar (Map (TopicName, Int32) Int32))
  }

------------------------------------------------------------------------
-- Lifecycle
------------------------------------------------------------------------

-- | Create a new producer.
newProducer :: KafkaClient -> ProducerConfig -> IO (Either KafkaException KafkaProducer)
newProducer client pCfg = do
  let cfg = pcClientConfig pCfg
  counters <- newMVar Map.empty
  drQueue <- newTBQueueIO (fromIntegral (pcDeliveryQueueSize pCfg))
  if ccIdempotent cfg
    then do
      idempResult <- initIdempotentState client
      case idempResult of
        Left err -> pure (Left err)
        Right idemState -> do
          brokers <- readTVarIO (kcBrokers client)
          mapM_ (\env -> setIdempotentState env
            (isProducerId idemState) (isProducerEpoch idemState) (isSequences idemState))
            (IM.elems brokers)
          pure $ Right KafkaProducer
            { kpClient = client, kpCounters = counters, kpConfig = cfg
            , kpIdempotent = Just idemState, kpDeliveryQueue = drQueue
            , kpDeliveryCallback = pcDeliveryCallback pCfg }
    else pure $ Right KafkaProducer
      { kpClient = client, kpCounters = counters, kpConfig = cfg
      , kpIdempotent = Nothing, kpDeliveryQueue = drQueue
      , kpDeliveryCallback = pcDeliveryCallback pCfg }

-- | Obtain a ProducerId via InitProducerId API.
initIdempotentState :: KafkaClient -> IO (Either KafkaException IdempotentState)
initIdempotentState client = do
  mBroker <- anyBroker client
  case mBroker of
    Nothing -> pure (Left (KafkaException "no broker available for InitProducerId"))
    Just env -> do
      let reqBytes = initProducerIdRequest Nothing 30000
      respVar <- enqueueRequest env reqBytes
      response <- atomically $ readTMVar respVar
      case response of
        Left err -> pure (Left err)
        Right bytes -> case Wire.runWire parseInitProducerIdResponseV4 bytes of
          Nothing -> pure (Left (KafkaParseException "failed to parse InitProducerIdResponse"))
          Just resp
            | ipErrorCode resp /= 0 ->
                pure (Left (KafkaUnexpectedErrorCodeException (ipErrorCode resp)))
            | otherwise -> do
                seqs <- newMVar Map.empty
                pure $ Right IdempotentState
                  { isProducerId = ipProducerId resp
                  , isProducerEpoch = ipProducerEpoch resp
                  , isSequences = seqs }

closeProducer :: KafkaProducer -> IO ()
closeProducer _ = pure ()

------------------------------------------------------------------------
-- Producing
------------------------------------------------------------------------

-- | Produce a message synchronously. Blocks until delivery report.
produce :: KafkaProducer -> ProducerRecord -> IO (Either KafkaException DeliveryReport)
produce producer record = do
  var <- newEmptyTMVarIO
  result <- sendRecord producer record (Just (\dr -> atomically $ putTMVar var dr)) Nothing
  case result of
    Left err -> pure (Left err)
    Right () -> Right <$> atomically (readTMVar var)

-- | Produce a message asynchronously. The delivery report will be
-- available via 'pollEvents' or the global callback.
-- Returns immediately (unless backpressure blocks the TBQueue).
produceAsync :: KafkaProducer -> ProducerRecord -> IO (Either KafkaException ())
produceAsync producer record =
  sendRecord producer record Nothing Nothing

-- | Produce a message with a per-message callback. The callback is
-- invoked from the broker thread when delivery is confirmed or fails.
produceWithCallback :: KafkaProducer -> ProducerRecord
                   -> (DeliveryReport -> IO ()) -> IO (Either KafkaException ())
produceWithCallback producer record cb =
  sendRecord producer record (Just cb) Nothing

-- | Internal: resolve partition, find broker, enqueue.
sendRecord :: KafkaProducer -> ProducerRecord
           -> Maybe (DeliveryReport -> IO ())  -- per-message callback
           -> Maybe ByteString                  -- override value (unused for now)
           -> IO (Either KafkaException ())
sendRecord producer record mCb _mOverride = do
  let topic = prTopic record
  partCount <- ensureTopicMetadata producer topic
  case partCount of
    Left err -> pure (Left err)
    Right count -> do
      part <- selectPartition producer record count
      mBroker <- leaderBrokerFor (kpClient producer) topic part
      case mBroker of
        Nothing -> pure (Left (KafkaException "no broker available"))
        Just env -> do
          let pm = PendingMessage
                { pmRecord = record
                , pmPayload = case prValue record of
                    Just v  -> v
                    Nothing -> ""
                , pmKey = prKey record
                , pmHeaders = prHeaders record
                , pmCallback = mCb
                , pmGlobalCallback = kpDeliveryCallback producer
                , pmDeliveryQueue = kpDeliveryQueue producer
                , pmRetriesLeft = ccRetries (kpConfig producer)
                }
          atomically $ writeTBQueue (beOps env) (BrokerProduce topic part pm)
          pure (Right ())

------------------------------------------------------------------------
-- Delivery report polling
------------------------------------------------------------------------

-- | Poll for delivery reports, blocking up to @timeoutMs@ milliseconds.
--
-- Returns all delivery reports that are ready. If none are ready and
-- @timeoutMs > 0@, blocks until at least one arrives or timeout expires.
-- With @timeoutMs = 0@, returns immediately (non-blocking drain).
-- With @timeoutMs = -1@, blocks indefinitely until at least one report.
--
-- Thread-safe: can be called from a different thread than the producer.
pollEvents :: KafkaProducer -> Int -> IO [DeliveryReport]
pollEvents producer timeoutMs = do
  -- First try a non-blocking drain
  immediate <- atomically $ flushTBQueue (kpDeliveryQueue producer)
  if not (null immediate)
    then pure immediate
    else if timeoutMs == 0
      then pure []
      else do
        -- Block with timeout
        timer <- if timeoutMs < 0
          then newTVarIO False  -- never fires → block indefinitely
          else registerDelay (timeoutMs * 1000)
        atomically $ do
          timedOut <- readTVar timer
          if timedOut
            then pure []
            else do
              -- Wait for at least one report
              first <- readTBQueue (kpDeliveryQueue producer)
              rest <- flushTBQueue (kpDeliveryQueue producer)
              pure (first : rest)

------------------------------------------------------------------------
-- Flush
------------------------------------------------------------------------

-- | Flush all pending messages, blocking until delivery reports are in.
flushProducer :: KafkaProducer -> IO ()
flushProducer producer = do
  brokers <- readTVarIO (kcBrokers (kpClient producer))
  doneVars <- mapM flushOne (IM.elems brokers)
  atomically $ mapM_ readTMVar doneVars
  where
    flushOne env = do
      done <- newEmptyTMVarIO
      atomically $ writeTBQueue (beOps env) (BrokerFlush done)
      pure done

------------------------------------------------------------------------
-- Internal: metadata + partitioning
------------------------------------------------------------------------

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
            Nothing -> pure (Left (KafkaException "topic not found after metadata refresh"))

-- | Select partition based on the ProducerRecord's partition strategy.
selectPartition :: KafkaProducer -> ProducerRecord -> Int32 -> IO Int32
selectPartition producer record count = case prPartition record of
  SpecifiedPartition p -> pure p
  UnassignedPartition -> case prKey record of
    -- TODO: key-based partitioning via murmur2 hash
    Just _key -> nextPartitionRR producer (prTopic record) count
    Nothing   -> nextPartitionRR producer (prTopic record) count

-- | Round-robin partition selection.
nextPartitionRR :: KafkaProducer -> TopicName -> Int32 -> IO Int32
nextPartitionRR producer topic count = do
  ref <- modifyMVar (kpCounters producer) $ \m ->
    case Map.lookup topic m of
      Just ref -> pure (m, ref)
      Nothing -> do
        ref <- newIORef 0
        pure (Map.insert topic ref m, ref)
  atomicModifyIORef' ref $ \n ->
    let n' = if n + 1 >= fromIntegral count then 0 else n + 1
    in (n', fromIntegral n)
