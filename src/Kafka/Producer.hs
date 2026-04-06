{-# language
    BangPatterns
  , LambdaCase
  , OverloadedStrings
  #-}

-- | High-performance Kafka producer with delivery reports.
--
-- Delivery report model (matching librdkafka):
--
-- * Broker thread produces → gets ProduceResponse → pushes
--   'DeliveryEntry' items to a 'TBQueue'. Never invokes callbacks.
--
-- * Application thread calls 'pollEvents' → drains the queue,
--   invokes per-message and global callbacks, returns reports.
--
-- This means: produce in thread A, poll in thread B. The broker
-- thread is never blocked by user callback code.
--
-- Queue forwarding: call 'forwardDeliveryQueue' to route delivery
-- reports to a custom queue instead of the default one.
module Kafka.Producer
  ( -- * Producer handle
    KafkaProducer(..)
  , ProducerConfig(..)
  , defaultProducerConfig
    -- * Lifecycle
  , newProducer
  , closeProducer
  , flushProducer
  , rotatePartitions
    -- * Producing
  , produce
  , produceAsync
  , produceWithCallback
    -- * Delivery reports
  , pollEvents
    -- * Queue forwarding
  , forwardDeliveryQueue
  , newDeliveryQueue
    -- * Types (re-export)
  , module Kafka.Producer.Types
  ) where

import Control.Concurrent.MVar (MVar, newMVar, modifyMVar, readMVar)
import Control.Monad (void, forM_)
import GHC.Clock (getMonotonicTimeNSec)
import Control.Concurrent.STM
import Data.Int (Int32, Int64, Int16)
import Data.IORef (IORef, newIORef, readIORef, atomicModifyIORef')
import Data.Map.Strict (Map)
import Numeric.Natural (Natural)

import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as Map

import Kafka.Common
import Kafka.Client
import Kafka.Internal.Broker
import Kafka.Internal.Config
import Kafka.Internal.InitProducerId.Request (initProducerIdRequest)
import Kafka.Internal.InitProducerId.Response (InitProducerIdResponse(..),
  parseInitProducerIdResponseV4)
import Kafka.Internal.Murmur2 (murmur2)
import Kafka.Producer.Types
import qualified Kafka.Internal.Wire as Wire

------------------------------------------------------------------------
-- Configuration
------------------------------------------------------------------------

data ProducerConfig = ProducerConfig
  { pcClientConfig     :: !ClientConfig
  , pcDeliveryCallback :: !(Maybe (DeliveryReport -> IO ()))
    -- ^ Global delivery report callback. Invoked by 'pollEvents'
    -- in the polling thread (NOT the broker thread).
  , pcDeliveryQueueSize :: {-# UNPACK #-} !Int
    -- ^ Size of the delivery report queue. Default: 10000.
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
  { kpClient           :: !KafkaClient
  , kpCounters         :: !(MVar (Map TopicName (IORef Int)))
  , kpConfig           :: !ClientConfig
  , kpIdempotent       :: !(Maybe IdempotentState)
  , kpDeliveryQueue    :: !(TBQueue DeliveryEntry)
    -- ^ Default delivery queue. Broker threads push here.
  , kpForwardQueue     :: !(TVar (Maybe (TBQueue DeliveryEntry)))
    -- ^ If set, new messages use this queue instead of kpDeliveryQueue.
    -- Enables queue forwarding (like librdkafka's rkq_fwdq).
  , kpGlobalCallback   :: !(Maybe (DeliveryReport -> IO ()))
    -- ^ Global callback, invoked by pollEvents.
  }

data IdempotentState = IdempotentState
  { isProducerId    :: {-# UNPACK #-} !Int64
  , isProducerEpoch :: {-# UNPACK #-} !Int16
  , isSequences     :: !(MVar (Map (TopicName, Int32) Int32))
  }

------------------------------------------------------------------------
-- Lifecycle
------------------------------------------------------------------------

newProducer :: KafkaClient -> ProducerConfig -> IO (Either KafkaException KafkaProducer)
newProducer client pCfg = do
  let cfg = pcClientConfig pCfg
  counters <- newMVar Map.empty
  drQueue <- newTBQueueIO (fromIntegral (pcDeliveryQueueSize pCfg) :: Natural)
  fwdVar <- newTVarIO Nothing
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
            , kpForwardQueue = fwdVar, kpGlobalCallback = pcDeliveryCallback pCfg }
    else pure $ Right KafkaProducer
      { kpClient = client, kpCounters = counters, kpConfig = cfg
      , kpIdempotent = Nothing, kpDeliveryQueue = drQueue
      , kpForwardQueue = fwdVar, kpGlobalCallback = pcDeliveryCallback pCfg }

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
-- Queue forwarding
------------------------------------------------------------------------

-- | Create a new delivery queue that can be used with 'forwardDeliveryQueue'.
newDeliveryQueue :: Int -> IO (TBQueue DeliveryEntry)
newDeliveryQueue size = newTBQueueIO (fromIntegral size :: Natural)

-- | Forward delivery reports to a custom queue. New messages produced
-- after this call will have their delivery reports sent to @q@ instead
-- of the producer's default queue. Pass 'Nothing' to revert to default.
--
-- This is like librdkafka's @rd_kafka_queue_forward()@.
forwardDeliveryQueue :: KafkaProducer -> Maybe (TBQueue DeliveryEntry) -> IO ()
forwardDeliveryQueue producer mq =
  atomically $ writeTVar (kpForwardQueue producer) mq

------------------------------------------------------------------------
-- Producing
------------------------------------------------------------------------

-- | Produce a message synchronously. Blocks until the broker confirms
-- delivery. Does NOT require a separate polling thread — the broker
-- thread writes the result directly to a TMVar (lightweight STM write,
-- not a user callback).
produce :: KafkaProducer -> ProducerRecord -> IO (Either KafkaException DeliveryReport)
produce producer record = do
  var <- newEmptyTMVarIO
  -- Use a callback that just fills the TMVar. This callback is stored
  -- in DeliveryEntry and invoked by pollEvents OR read directly by the
  -- broker thread via the TMVar mechanism below. For sync produce, we
  -- also install it as a direct STM write so it works without polling.
  result <- sendRecordSync producer record var
  case result of
    Left err -> pure (Left err)
    Right () -> Right <$> atomically (readTMVar var)

-- | Produce asynchronously. Returns immediately.
-- Delivery reports available via 'pollEvents'.
produceAsync :: KafkaProducer -> ProducerRecord -> IO (Either KafkaException ())
produceAsync producer record =
  sendRecord producer record Nothing

-- | Produce with a per-message callback. The callback is invoked
-- by 'pollEvents' in the polling thread (NOT the broker thread).
produceWithCallback :: KafkaProducer -> ProducerRecord
                   -> (DeliveryReport -> IO ()) -> IO (Either KafkaException ())
produceWithCallback producer record cb =
  sendRecord producer record (Just cb)

-- | Internal: sync produce. Broker thread fills the TMVar directly.
sendRecordSync :: KafkaProducer -> ProducerRecord -> TMVar DeliveryReport
               -> IO (Either KafkaException ())
sendRecordSync producer record var = do
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
          now <- getMonotonicTimeNSec
          drQueue <- resolveDeliveryQueue producer
          let pm = PendingMessage
                { pmEnqueueTime = fromIntegral (now `div` 1000)  -- ns → μs
                , pmRecord = record
                , pmPayload = case prValue record of { Just v -> v; Nothing -> "" }
                , pmKey = prKey record
                , pmHeaders = prHeaders record
                , pmCallback = Nothing
                , pmSyncVar = Just var
                , pmDeliveryQueue = drQueue
                , pmRetriesLeft = ccRetries (kpConfig producer)
                }
          atomically $ writeTBQueue (beOps env) (BrokerProduce topic part pm)
          pure (Right ())

-- | Internal: async produce.
sendRecord :: KafkaProducer -> ProducerRecord
           -> Maybe (DeliveryReport -> IO ())
           -> IO (Either KafkaException ())
sendRecord producer record mCb = do
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
          now <- getMonotonicTimeNSec
          drQueue <- resolveDeliveryQueue producer
          let pm = PendingMessage
                { pmEnqueueTime = fromIntegral (now `div` 1000)
                , pmRecord = record
                , pmPayload = case prValue record of { Just v -> v; Nothing -> "" }
                , pmKey = prKey record
                , pmHeaders = prHeaders record
                , pmCallback = mCb
                , pmSyncVar = Nothing
                , pmDeliveryQueue = drQueue
                , pmRetriesLeft = ccRetries (kpConfig producer)
                }
          atomically $ writeTBQueue (beOps env) (BrokerProduce topic part pm)
          pure (Right ())

-- | Resolve which delivery queue to use: forwarded or default.
resolveDeliveryQueue :: KafkaProducer -> IO (TBQueue DeliveryEntry)
resolveDeliveryQueue producer = do
  mFwd <- readTVarIO (kpForwardQueue producer)
  pure $ case mFwd of
    Just q  -> q
    Nothing -> kpDeliveryQueue producer

------------------------------------------------------------------------
-- Delivery report polling
------------------------------------------------------------------------

-- | Poll for delivery reports, blocking up to @timeoutMs@ milliseconds.
--
-- Drains the delivery queue, invokes per-message callbacks and the
-- global callback for each report, then returns all reports.
--
-- Semantics match @rd_kafka_poll@:
--   * @timeoutMs = 0@: non-blocking drain (return immediately)
--   * @timeoutMs > 0@: block until at least one report or timeout
--   * @timeoutMs = -1@: block indefinitely until at least one report
--
-- Thread-safe. Designed to be called from a dedicated polling thread.
pollEvents :: KafkaProducer -> Int -> IO [DeliveryReport]
pollEvents producer timeoutMs = do
  entries <- drainWithTimeout (kpDeliveryQueue producer) timeoutMs
  -- Invoke callbacks in the polling thread (never in broker thread)
  mapM (invokeCallbacks (kpGlobalCallback producer)) entries

-- | Drain the queue with timeout semantics.
drainWithTimeout :: TBQueue DeliveryEntry -> Int -> IO [DeliveryEntry]
drainWithTimeout q timeoutMs = do
  -- Non-blocking drain first
  immediate <- atomically $ flushTBQueue q
  if not (null immediate)
    then pure immediate
    else if timeoutMs == 0
      then pure []
      else do
        -- Block with timeout
        timer <- if timeoutMs < 0
          then newTVarIO False  -- never fires
          else registerDelay (timeoutMs * 1000)
        atomically $ do
          timedOut <- readTVar timer
          if timedOut
            then pure []
            else do
              first <- readTBQueue q
              rest <- flushTBQueue q
              pure (first : rest)

-- | Invoke per-message callback + global callback, return the report.
invokeCallbacks :: Maybe (DeliveryReport -> IO ()) -> DeliveryEntry -> IO DeliveryReport
invokeCallbacks globalCb (DeliveryEntry dr mCb) = do
  -- Per-message callback first
  case mCb of
    Just cb -> cb dr
    Nothing -> pure ()
  -- Global callback
  case globalCb of
    Just cb -> cb dr
    Nothing -> pure ()
  pure dr

------------------------------------------------------------------------
-- Flush
------------------------------------------------------------------------

flushProducer :: KafkaProducer -> IO ()
flushProducer producer = do
  brokers <- readTVarIO (kcBrokers (kpClient producer))
  doneVars <- mapM flushOne (IM.elems brokers)
  atomically $ mapM_ readTMVar doneVars
  -- Rotate sticky partitions for next batch
  rotatePartitions producer
  where
    flushOne env = do
      done <- newEmptyTMVarIO
      atomically $ writeTBQueue (beControlOps env) (BrokerFlush done)
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

selectPartition :: KafkaProducer -> ProducerRecord -> Int32 -> IO Int32
selectPartition producer record count = case prPartition record of
  SpecifiedPartition p -> pure p
  UnassignedPartition -> case prKey record of
    Just key -> pure (fromIntegral (murmur2 key `mod` count))
    Nothing  -> stickyPartition producer (prTopic record) count

-- | Sticky partition: returns the same partition for a topic until
-- rotatePartitions is called (on batch flush). Improves batching.
stickyPartition :: KafkaProducer -> TopicName -> Int32 -> IO Int32
stickyPartition producer topic _count = do
  ref <- modifyMVar (kpCounters producer) $ \m ->
    case Map.lookup topic m of
      Just ref -> pure (m, ref)
      Nothing -> do
        ref <- newIORef 0
        pure (Map.insert topic ref m, ref)
  fromIntegral <$> readIORef ref

-- | Rotate sticky partition counters. Called after flush.
rotatePartitions :: KafkaProducer -> IO ()
rotatePartitions producer = do
  counters <- readMVar (kpCounters producer)
  forM_ (Map.toList counters) $ \(topic, ref) -> do
    mCount <- partitionCountFor (kpClient producer) topic
    case mCount of
      Nothing -> pure ()
      Just count ->
        atomicModifyIORef' ref $ \n ->
          let n' = if n + 1 >= fromIntegral count then 0 else n + 1
          in (n', ())
