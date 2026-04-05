{-# language
    BangPatterns
  , DerivingStrategies
  , LambdaCase
  , OverloadedStrings
  , ScopedTypeVariables
  #-}

-- | Per-broker green thread with sender, receiver, batching, and correlation.
--
-- Architecture modeled on librdkafka (rdkafka_broker.c):
--   - One forkIO green thread per broker (replaces OS thread)
--   - TBQueue for incoming ops (replaces rdkafka_q_t with pipe wakeup)
--   - IntMap for inflight request dispatch (correlation ID → TMVar)
--   - Two sub-threads per connection: sender + receiver
--   - Batching in the sender thread (linger + size + count thresholds)
--   - Reconnection with exponential backoff + jitter
--   - ProduceResponse error parsing with retry on retriable errors
--   - In-flight request limit (max.in.flight.requests.per.connection)
--
-- Key librdkafka references:
--   rdkafka_broker.c:4505   rd_kafka_broker_thread_main
--   rdkafka_broker.c:4226   rd_kafka_broker_producer_serve
--   rdkafka_broker.c:3872   rd_kafka_toppar_producer_serve
--   rdkafka_msg.c:1779      rd_kafka_msgq_allow_wakeup_at
module Kafka.Internal.Broker
  ( BrokerEnv(..)
  , BrokerState(..)
  , BrokerOp(..)
  , PendingMessage(..)
  , IdempotentRef(..)
  , newBrokerEnv
  , startBrokerThread
  , stopBroker
  , enqueueRequest
  , setIdempotentState
  ) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.Async (race_)
import Control.Concurrent.MVar (MVar, modifyMVar)
import Control.Concurrent.STM
import Control.Exception (SomeException, IOException, try)
import Control.Monad (unless, void, forM_)
import Data.Bits (shiftR)
import Data.IntMap.Strict (IntMap)
import Data.IORef
import Data.Int (Int16, Int32, Int64)
import Data.Map.Strict (Map)
import Data.ByteString (ByteString)
import Numeric.Natural (Natural)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy as BSL
import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as Map
import qualified Network.Socket.ByteString as NBS
import qualified Network.Socket.ByteString.Lazy as NBSL

import Kafka.Common
import Kafka.Producer.Types
import Kafka.Internal.ApiVersions.Request (apiVersionsRequest)
import Kafka.Internal.ApiVersions.Response (ApiVersionsResponse(..), ApiVersionEntry(..),
  parseApiVersionsResponse)
import Kafka.Internal.Config
import Kafka.Internal.Produce.Request (buildProduceRequest)
import Kafka.Internal.Produce.Response (ProduceResponse(..), ProduceResponseMessage(..),
  ProducePartitionResponse(..), parseProduceResponseV9)
import Kafka.Internal.Reconnect
import Kafka.Internal.Response (getKafkaResponse)
import qualified Kafka.Internal.Wire as Wire

------------------------------------------------------------------------
-- Types
------------------------------------------------------------------------

data BrokerState = BrokerInit | BrokerDown | BrokerConnecting | BrokerUp
  deriving stock (Eq, Show)

-- | A message waiting to be batched and sent.
data PendingMessage = PendingMessage
  { pmRecord         :: !ProducerRecord
    -- ^ Original record (for delivery reports).
  , pmPayload        :: !ByteString
    -- ^ Value bytes to encode in the record batch.
  , pmKey            :: !(Maybe ByteString)
    -- ^ Key bytes (for record batch encoding).
  , pmHeaders        :: !Headers
    -- ^ Record headers (for record batch encoding).
  , pmCallback       :: !(Maybe (DeliveryReport -> IO ()))
    -- ^ Per-message callback. Stored in DeliveryEntry, invoked by the poller.
  , pmSyncVar        :: !(Maybe (TMVar DeliveryReport))
    -- ^ For sync produce: broker thread writes here directly (lightweight STM).
    -- This is NOT a user callback — just a TMVar put.
  , pmDeliveryQueue  :: !(TBQueue DeliveryEntry)
    -- ^ Shared delivery queue. Broker thread pushes DeliveryEntry here.
  , pmRetriesLeft    :: {-# UNPACK #-} !Int
  }

-- | Operations enqueued to a broker thread.
data BrokerOp
  = BrokerProduce
      !TopicName
      {-# UNPACK #-} !Int32                         -- partition
      !PendingMessage
  | BrokerSendRaw
      !BSL.ByteString                                -- pre-encoded request
      !(TMVar (Either KafkaException ByteString))    -- raw response slot
  | BrokerFlush
      !(TMVar ())                                  -- signal when flush is done
  | BrokerShutdown

-- | Inflight request entry — how to dispatch the response.
data InflightEntry
  = InflightRaw    !(TMVar (Either KafkaException ByteString))
  | InflightBatch  !TopicName ![(Int32, [PendingMessage])]
    -- ^ topic, [(partition, [messages with retry info])]

-- | Batch accumulator for produce messages.
-- Messages are accumulated in reverse order per (topic, partition).
data Batch = Batch
  { batchGroups :: !(Map (TopicName, Int32) [PendingMessage])
  , batchCount  :: {-# UNPACK #-} !Int
  , batchBytes  :: {-# UNPACK #-} !Int
  }

emptyBatch :: Batch
emptyBatch = Batch Map.empty 0 0

batchIsEmpty :: Batch -> Bool
batchIsEmpty b = batchCount b == 0
{-# INLINE batchIsEmpty #-}

{-# INLINE batchAdd #-}
batchAdd :: Batch -> TopicName -> Int32 -> PendingMessage -> Batch
batchAdd (Batch groups cnt bytes) topic part msg = Batch
  { batchGroups = Map.alter addMsg (topic, part) groups
  , batchCount  = cnt + 1
  , batchBytes  = bytes + BS.length (pmPayload msg)
  }
  where
    addMsg Nothing    = Just [msg]
    addMsg (Just old) = Just (msg : old)

-- | Check if batch meets send thresholds (size or count).
-- Linger timeout is handled separately via STM.
batchReadyByThreshold :: ClientConfig -> Batch -> Bool
batchReadyByThreshold cfg b =
  batchCount b >= ccBatchNumMessages cfg || batchBytes b >= ccBatchSize cfg

-- | Per-broker thread environment.
data BrokerEnv = BrokerEnv
  { beNodeId         :: {-# UNPACK #-} !Int32
  , beBrokerAddress  :: !BrokerAddress
  , beOps            :: !(TBQueue BrokerOp)
  , beState          :: !(TVar BrokerState)
  , beInflight       :: !(TVar (IntMap InflightEntry))
  , beInflightCount  :: !(TVar Int)
    -- ^ Current number of inflight produce requests (for max.in.flight limit).
  , beCorrCounter    :: !(IORef Int32)
  , beReconnect      :: !(IORef ReconnectState)
  , beConfig         :: !ClientConfig
  , beShutdown       :: !(TVar Bool)
  , beApiVersions    :: !(TVar (Maybe [ApiVersionEntry]))
  , beIdempotent     :: !(IORef (Maybe IdempotentRef))
    -- ^ When set, enables idempotent produce with PID/epoch/sequences.
    -- Mutable so Producer can set it after InitProducerId without
    -- replacing the BrokerEnv in the client's IntMap.
  }

-- | Shared idempotent producer state, set from the Producer layer.
data IdempotentRef = IdempotentRef
  { irProducerId    :: {-# UNPACK #-} !Int64
  , irProducerEpoch :: {-# UNPACK #-} !Int16
  , irSequences     :: !(MVar (Map (TopicName, Int32) Int32))
  }

------------------------------------------------------------------------
-- Construction
------------------------------------------------------------------------

newBrokerEnv :: ClientConfig -> Int32 -> BrokerAddress -> IO BrokerEnv
newBrokerEnv cfg nodeId peer = do
  ops       <- newTBQueueIO (fromIntegral (ccQueueSize cfg) :: Natural)
  state     <- newTVarIO BrokerInit
  inflight  <- newTVarIO IM.empty
  inflightC <- newTVarIO 0
  corrId    <- newIORef 0
  reconn    <- newIORef (newReconnectState (ccReconnectMs cfg)
                                           (ccReconnectMaxMs cfg)
                                           (fromIntegral nodeId + 12345))
  shutdown  <- newTVarIO False
  apiVers   <- newTVarIO Nothing
  idempRef  <- newIORef Nothing
  pure BrokerEnv
    { beNodeId         = nodeId
    , beBrokerAddress  = peer
    , beOps            = ops
    , beState          = state
    , beInflight       = inflight
    , beInflightCount  = inflightC
    , beCorrCounter    = corrId
    , beReconnect      = reconn
    , beConfig         = cfg
    , beShutdown       = shutdown
    , beApiVersions    = apiVers
    , beIdempotent     = idempRef
    }

-- | Set idempotent state on a broker env (called from Producer after InitProducerId).
setIdempotentState :: BrokerEnv -> Int64 -> Int16 -> MVar (Map (TopicName, Int32) Int32) -> IO ()
setIdempotentState env pid epoch seqs =
  writeIORef (beIdempotent env) (Just (IdempotentRef pid epoch seqs))

------------------------------------------------------------------------
-- Lifecycle
------------------------------------------------------------------------

-- | Start the broker's green thread.
startBrokerThread :: BrokerEnv -> IO ()
startBrokerThread env = void $ forkIO (brokerThreadMain env)

-- | Request clean shutdown of the broker thread.
stopBroker :: BrokerEnv -> IO ()
stopBroker env = atomically $ do
  writeTVar (beShutdown env) True
  writeTBQueue (beOps env) BrokerShutdown

------------------------------------------------------------------------
-- Public: enqueue operations
------------------------------------------------------------------------

-- | Enqueue a pre-encoded request (metadata, fetch, etc.).
-- Returns a TMVar that will be filled with the raw response bytes.
enqueueRequest :: BrokerEnv -> BSL.ByteString
              -> IO (TMVar (Either KafkaException ByteString))
enqueueRequest env reqBytes = do
  result <- newEmptyTMVarIO
  atomically $ writeTBQueue (beOps env) (BrokerSendRaw reqBytes result)
  pure result

------------------------------------------------------------------------
-- Broker thread internals
------------------------------------------------------------------------

-- | Main broker thread loop. Connects, runs session, reconnects with
-- backoff on failure. Modeled on rdkafka_broker.c:rd_kafka_broker_thread_main.
brokerThreadMain :: BrokerEnv -> IO ()
brokerThreadMain env = do
  done <- readTVarIO (beShutdown env)
  unless done $ do
    atomically $ writeTVar (beState env) BrokerConnecting
    let BrokerAddress host port = beBrokerAddress env
    _ <- try @SomeException $ withKafka host (show port) $ \kafka -> do
      atomically $ writeTVar (beState env) BrokerUp
      resetBackoff (beReconnect env)
      runBrokerSession env kafka
    -- Session ended
    atomically $ writeTVar (beState env) BrokerDown
    failAllInflight env
    -- Reconnect with backoff (unless shutting down)
    done' <- readTVarIO (beShutdown env)
    unless done' $ do
      delayUs <- nextBackoffDelay (beReconnect env)
      threadDelay delayUs
      brokerThreadMain env

-- | Run a single broker session (one TCP connection lifetime).
-- Races sender and receiver — when either exits (normally or via
-- exception), the other is cancelled. This mirrors librdkafka's
-- single-thread poll() loop, but uses two green threads since GHC's
-- IO manager makes blocking send/recv transparent.
runBrokerSession :: BrokerEnv -> Kafka -> IO ()
runBrokerSession env kafka = do
  performHandshake env kafka
  race_ (senderLoop env kafka) (receiverLoop env kafka)

------------------------------------------------------------------------
-- Connection handshake
------------------------------------------------------------------------

-- | Send ApiVersions request and store the broker's supported versions.
-- Non-fatal: if the handshake fails, we proceed without version info.
performHandshake :: BrokerEnv -> Kafka -> IO ()
performHandshake env kafka = do
  let reqBytes = apiVersionsRequest
  corrId <- nextCorrId (beCorrCounter env)
  let patched = patchCorrelationIdLBS corrId reqBytes
  result <- try @IOException $ NBSL.sendAll (getSocket kafka) patched
  case result of
    Left _ -> pure ()  -- handshake failed, proceed without version info
    Right () -> do
      interrupt <- registerDelay (ccRequestTimeoutMs (beConfig env) * 1000)
      resp <- getKafkaResponse kafka interrupt
      case resp of
        Left _ -> pure ()
        Right responseBytes ->
          case Wire.runWire parseApiVersionsResponse responseBytes of
            Nothing -> pure ()
            Just avResp ->
              atomically $ writeTVar (beApiVersions env)
                (Just (avApiVersions avResp))

------------------------------------------------------------------------
-- Sender thread
------------------------------------------------------------------------

-- | Sender loop with batching. Modeled on:
--   rdkafka_broker.c:4226 (rd_kafka_broker_producer_serve)
--   rdkafka_msg.c:1779    (rd_kafka_msgq_allow_wakeup_at)
--
-- Strategy: wait for ops from TBQueue. For produce ops, accumulate
-- into a batch. Send the batch when ANY of:
--   1. Message count >= batchNumMessages
--   2. Total bytes >= batchSize
--   3. Linger timer fires (registerDelay)
--   4. Shutdown requested
--
-- Non-produce ops (BrokerSendRaw) are sent immediately.
--
-- The linger timer is implemented via registerDelay (STM TVar Bool),
-- which replaces librdkafka's condvar + pipe-fd wakeup mechanism.
-- STM orElse naturally handles the "wake on either new message or
-- timer" pattern — no explicit wakeup management needed (Phase 5 free).
senderLoop :: BrokerEnv -> Kafka -> IO ()
senderLoop env kafka = do
  lingerTimer <- registerDelay (ccLingerMs (beConfig env) * 1000)
  go lingerTimer emptyBatch
  where
    cfg = beConfig env
    ops = beOps env

    go :: TVar Bool -> Batch -> IO ()
    go !lingerTimer !batch = do
      event <- if batchIsEmpty batch
        -- No pending batch: just block on the next op
        then OpEvent <$> atomically (readTBQueue ops)
        -- Pending batch: race queue read vs linger timer
        else atomically $
              (OpEvent <$> readTBQueue ops)
          `orElse`
              (do fired <- readTVar lingerTimer
                  check fired
                  pure LingerFired)

      case event of
        OpEvent (BrokerProduce topic part msg) -> do
          let !batch' = batchAdd batch topic part msg
          if batchReadyByThreshold cfg batch'
            then do
              flushBatch env kafka batch'
              newTimer <- registerDelay (ccLingerMs cfg * 1000)
              go newTimer emptyBatch
            else go lingerTimer batch'

        OpEvent (BrokerSendRaw reqBytes respVar) -> do
          sendRawRequest env kafka reqBytes respVar
          go lingerTimer batch

        OpEvent (BrokerFlush done) -> do
          unless (batchIsEmpty batch) (flushBatch env kafka batch)
          atomically $ putTMVar done ()
          newTimer <- registerDelay (ccLingerMs cfg * 1000)
          go newTimer emptyBatch

        OpEvent BrokerShutdown -> do
          unless (batchIsEmpty batch) (flushBatch env kafka batch)

        LingerFired -> do
          flushBatch env kafka batch
          newTimer <- registerDelay (ccLingerMs cfg * 1000)
          go newTimer emptyBatch

data SenderEvent = OpEvent !BrokerOp | LingerFired

------------------------------------------------------------------------
-- Batch flush
------------------------------------------------------------------------

-- | Encode and send all accumulated produce messages.
-- Groups messages by (topic, partition), builds a ProduceRequest for
-- each group, assigns correlation IDs, and sends.
flushBatch :: BrokerEnv -> Kafka -> Batch -> IO ()
flushBatch env kafka batch =
  Map.foldlWithKey' (\act k v -> act >> sendPartitionBatch env kafka (k, v))
    (pure ()) (batchGroups batch)

-- | Send a batch for a single (topic, partition) group.
-- Blocks via STM if in-flight request count is at the limit.
-- When idempotent state is set, uses produceRequestIdempotent with
-- per-partition sequence numbers.
sendPartitionBatch :: BrokerEnv -> Kafka -> ((TopicName, Int32), [PendingMessage]) -> IO ()
sendPartitionBatch env kafka ((topic, part), msgsRev) = do
  let msgs = reverse msgsRev
      cfg = beConfig env
      msgCount = length msgs

  -- Determine idempotent state (PID/epoch/sequence) if set
  mIdemp <- readIORef (beIdempotent env)
  (pid, epoch, baseSeq) <- case mIdemp of
    Nothing -> pure (-1, -1, -1)
    Just (IdempotentRef p e seqsVar) -> do
      s <- modifyMVar seqsVar $ \seqMap ->
        let key = (topic, part)
            curSeq = Map.findWithDefault 0 key seqMap
            nextSeq = curSeq + fromIntegral msgCount
        in pure (Map.insert key nextSeq seqMap, curSeq)
      pure (p, e, s)

  -- Wait for in-flight slot (blocks if at ccMaxInFlight)
  atomically $ do
    count <- readTVar (beInflightCount env)
    check (count < ccMaxInFlight cfg)
    writeTVar (beInflightCount env) (count + 1)

  -- Get corrId and build the entire request as a strict ByteString
  corrId <- nextCorrId (beCorrCounter env)
  let !reqBytes = buildProduceRequest
        corrId
        (acksToInt16 (ccAcks cfg))
        (ccClientId cfg)
        (ccRequestTimeoutMs cfg)
        topic part pid epoch baseSeq
        (ccCompression cfg)
        (map pmPayload msgs)

  let callbacks = [(part, msgs)]
  atomically $ modifyTVar' (beInflight env) $
    IM.insert (fromIntegral corrId) (InflightBatch topic callbacks)

  -- Send strict ByteString — single send() syscall
  result <- try @IOException $ NBS.sendAll (getSocket kafka) reqBytes
  case result of
    Left err -> do
      let errBS = BS8.pack (show err)
      forM_ msgs $ \m ->
        deliverReportIO m (DeliveryFailure (pmRecord m) errBS)
      atomically $ do
        modifyTVar' (beInflight env) $ IM.delete (fromIntegral corrId)
        modifyTVar' (beInflightCount env) (subtract 1)
    Right () -> pure ()

------------------------------------------------------------------------
-- Raw request send
------------------------------------------------------------------------

sendRawRequest :: BrokerEnv -> Kafka -> BSL.ByteString
              -> TMVar (Either KafkaException ByteString) -> IO ()
sendRawRequest env kafka reqBytes respVar = do
  corrId <- nextCorrId (beCorrCounter env)
  let patched = patchCorrelationIdLBS corrId reqBytes

  atomically $ modifyTVar' (beInflight env) $
    IM.insert (fromIntegral corrId) (InflightRaw respVar)

  result <- try @IOException $ NBSL.sendAll (getSocket kafka) patched
  case result of
    Left err -> atomically $ do
      void $ tryPutTMVar respVar (Left (KafkaIOError (show err)))
      modifyTVar' (beInflight env) $ IM.delete (fromIntegral corrId)
    Right () -> pure ()

------------------------------------------------------------------------
-- Receiver thread
------------------------------------------------------------------------

-- | Reads responses from the socket and dispatches by correlation ID.
-- Modeled on rdkafka_broker.c broker thread response handling.
receiverLoop :: BrokerEnv -> Kafka -> IO ()
receiverLoop env kafka = do
  done <- readTVarIO (beShutdown env)
  unless done $ do
    -- Use a long timeout for response reading
    interrupt <- registerDelay (ccRequestTimeoutMs (beConfig env) * 1000)
    result <- getKafkaResponse kafka interrupt
    case result of
      Left _err -> pure ()  -- socket error → session will end
      Right responseBytes -> do
        dispatchResponse env responseBytes
        receiverLoop env kafka

-- | Extract correlation ID from response and dispatch to the waiting caller.
-- For produce responses, parses error codes per partition and retries
-- on retriable errors if retries remain.
dispatchResponse :: BrokerEnv -> ByteString -> IO ()
dispatchResponse env responseBytes = do
  let corrId = extractCorrelationId responseBytes
  mEntry <- atomically $ do
    m <- readTVar (beInflight env)
    case IM.lookup corrId m of
      Nothing -> pure Nothing
      Just entry -> do
        writeTVar (beInflight env) (IM.delete corrId m)
        pure (Just entry)
  case mEntry of
    Nothing -> pure ()  -- orphaned response, ignore
    Just (InflightRaw respVar) ->
      void $ atomically $ tryPutTMVar respVar (Right responseBytes)
    Just (InflightBatch topic callbacks) -> do
      -- Decrement in-flight count
      atomically $ modifyTVar' (beInflightCount env) (subtract 1)
      -- Parse the ProduceResponse to get per-partition error codes
      case Wire.runWire parseProduceResponseV9 responseBytes of
        Nothing ->
          forM_ callbacks $ \(_part, msgs) ->
            forM_ msgs $ \m ->
              deliverReportIO m (DeliveryFailure (pmRecord m) "failed to parse ProduceResponse")
        Just prodResp ->
          dispatchProduceResponse env topic callbacks prodResp

-- | Dispatch a parsed ProduceResponse to the waiting callbacks.
-- Matches partition responses to callbacks and handles errors.
dispatchProduceResponse :: BrokerEnv
                       -> TopicName
                       -> [(Int32, [PendingMessage])]
                       -> ProduceResponse
                       -> IO ()
dispatchProduceResponse env topic callbacks prodResp = do
  -- Build a map of partition → (errorCode, baseOffset)
  let partResults = Map.fromList
        [ (prResponsePartition pr, (prResponseErrorCode pr, prResponseBaseOffset pr))
        | msg <- produceResponseMessages prodResp
        , pr  <- prPartitionResponses msg
        ]
  forM_ callbacks $ \(part, msgs) ->
    case Map.lookup part partResults of
      Nothing ->
        deliverAll msgs (Offset 0)  -- no entry → treat as success
      Just (0, baseOff) ->
        deliverAll msgs (Offset baseOff)
      Just (errCode, _) -> case fromErrorCode errCode of
        Nothing ->
          failAll msgs (BS8.pack ("unknown error code: " ++ show errCode))
        Just protoErr
          | isRetriable protoErr ->
              forM_ msgs $ \m ->
                if pmRetriesLeft m > 0
                  then atomically $ writeTBQueue (beOps env)
                    (BrokerProduce topic part m { pmRetriesLeft = pmRetriesLeft m - 1 })
                  else deliverReport m (DeliveryFailure (pmRecord m) (BS8.pack (show protoErr)))
          | otherwise ->
              forM_ msgs $ \m ->
                deliverReport m (DeliveryFailure (pmRecord m) (BS8.pack (show protoErr)))
  where
    -- Deliver success reports with sequential offsets starting at baseOffset.
    deliverAll [] _ = pure ()
    deliverAll (m:rest) !off = do
      deliverReport m (DeliverySuccess (pmRecord m) off)
      deliverAll rest (Offset (unOffset off + 1))

    failAll msgs errBS = forM_ msgs $ \m ->
      deliverReport m (DeliveryFailure (pmRecord m) errBS)

    -- Push delivery entry to queue + fill sync TMVar. No user callbacks.
    deliverReport :: PendingMessage -> DeliveryReport -> IO ()
    deliverReport m dr = atomically $ do
      -- Fill sync TMVar if present (for sync produce)
      case pmSyncVar m of
        Just var -> void $ tryPutTMVar var dr
        Nothing  -> pure ()
      -- Push to delivery queue (for pollEvents)
      let !entry = DeliveryEntry dr (pmCallback m)
      full <- isFullTBQueue (pmDeliveryQueue m)
      unless full $ writeTBQueue (pmDeliveryQueue m) entry

------------------------------------------------------------------------
-- Failure handling
------------------------------------------------------------------------

-- | Fail all inflight requests (called when connection drops).
failAllInflight :: BrokerEnv -> IO ()
failAllInflight env = do
  entries <- atomically $ do
    m <- readTVar (beInflight env)
    writeTVar (beInflight env) IM.empty
    writeTVar (beInflightCount env) 0
    pure m
  let err = Left (KafkaException "broker connection lost")
  forM_ (IM.elems entries) $ \case
    InflightRaw respVar ->
      atomically $ void $ tryPutTMVar respVar err
    InflightBatch _ callbacks ->
      forM_ callbacks $ \(_, msgs) ->
        forM_ msgs $ \m ->
          deliverReportIO m (DeliveryFailure (pmRecord m) "broker connection lost")

-- | Push a delivery entry to the queue (IO version for use outside dispatch).
deliverReportIO :: PendingMessage -> DeliveryReport -> IO ()
deliverReportIO m dr = atomically $ do
  case pmSyncVar m of
    Just var -> void $ tryPutTMVar var dr
    Nothing  -> pure ()
  let !entry = DeliveryEntry dr (pmCallback m)
  full <- isFullTBQueue (pmDeliveryQueue m)
  unless full $ writeTBQueue (pmDeliveryQueue m) entry

------------------------------------------------------------------------
-- Correlation ID
------------------------------------------------------------------------

nextCorrId :: IORef Int32 -> IO Int32
nextCorrId ref = atomicModifyIORef' ref $ \n -> (n + 1, n)

-- | Extract correlation ID from the first 4 bytes of a response body.
-- Kafka response format: [4-byte size (already consumed)] [4-byte corrId] [...]
-- getKafkaResponse returns the body (starting with corrId).
extractCorrelationId :: ByteString -> Int
extractCorrelationId bs
  | BS.length bs >= 4 =
      let b0 = fromIntegral (BS.index bs 0) :: Int
          b1 = fromIntegral (BS.index bs 1) :: Int
          b2 = fromIntegral (BS.index bs 2) :: Int
          b3 = fromIntegral (BS.index bs 3) :: Int
      in b0 * 16777216 + b1 * 65536 + b2 * 256 + b3
  | otherwise = -1

------------------------------------------------------------------------
-- Correlation ID patching
------------------------------------------------------------------------

-- | Patch correlation ID in a BSL.ByteString Kafka request.
-- The correlation ID sits at byte offset 4 within the request body
-- (after apiKey:2 + apiVersion:2). buildRequest produces:
--   chunk[0] = 4-byte size prefix
--   chunk[1] = body starting with apiKey(2) + apiVersion(2) + corrId(4) + ...
-- So we patch at byte 4 of chunk[1], or byte 8 if it's a single chunk.
patchCorrelationIdLBS :: Int32 -> BSL.ByteString -> BSL.ByteString
patchCorrelationIdLBS corrId lbs =
  let corrBytes = BS.pack
        [ fromIntegral (shiftR corrId 24)
        , fromIntegral (shiftR corrId 16)
        , fromIntegral (shiftR corrId 8)
        , fromIntegral corrId
        ]
  in case BSL.toChunks lbs of
    [] -> lbs
    -- Two-chunk layout (buildRequest): size(4) | body(apiKey+apiVersion+corrId+...)
    -- corrId at byte 4 of chunk[1]
    (sizeChunk : bodyChunk : rest)
      | BS.length sizeChunk == 4 && BS.length bodyChunk >= 8 ->
          let before = BS.take 4 bodyChunk
              after  = BS.drop 8 bodyChunk
          in BSL.fromChunks (sizeChunk : (before <> corrBytes <> after) : rest)
    -- Single-chunk layout: corrId at byte 8
    (firstChunk : rest)
      | BS.length firstChunk >= 12 ->
          let before = BS.take 8 firstChunk
              after  = BS.drop 12 firstChunk
          in BSL.fromChunks ((before <> corrBytes <> after) : rest)
    _ -> lbs  -- malformed
