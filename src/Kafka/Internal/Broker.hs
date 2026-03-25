{-# language
    BangPatterns
  , DerivingStrategies
  , LambdaCase
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
  , newBrokerEnv
  , startBrokerThread
  , stopBroker
  , enqueueProduce
  , enqueueRequest
  ) where

import Control.Concurrent (forkIO, ThreadId, killThread, threadDelay, myThreadId)
import Control.Concurrent.STM
import Control.Exception (SomeException, IOException, try, throwTo)
import Control.Monad (unless, void)
import Data.Bits (shiftR)
import Data.ByteString (ByteString)
import Data.IntMap.Strict (IntMap)
import Data.IORef
import Data.Int (Int32)
import Data.Map.Strict (Map)
import Control.Monad.ST (runST)
import Data.Primitive.ByteArray (ByteArray, sizeofByteArray, indexByteArray,
  newByteArray, writeByteArray, unsafeFreezeByteArray, copyByteArray)
import Data.Primitive.Unlifted.Array
import Data.Word (Word8, Word32, byteSwap32)
import Numeric.Natural (Natural)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as Map
import qualified Network.Socket.ByteString.Lazy as NBSL

import Kafka.Common
import Kafka.Internal.ApiVersions.Request (apiVersionsRequest)
import Kafka.Internal.ApiVersions.Response (ApiVersionsResponse(..), ApiVersionEntry(..),
  parseApiVersionsResponse)
import Kafka.Internal.Config
import Kafka.Internal.Produce.Request (produceRequest)
import Kafka.Internal.Reconnect
import Kafka.Internal.Response (getKafkaResponse)

import qualified Data.Bytes.Parser as Smith

------------------------------------------------------------------------
-- Types
------------------------------------------------------------------------

data BrokerState = BrokerInit | BrokerDown | BrokerConnecting | BrokerUp
  deriving stock (Eq, Show)

-- | A message waiting to be batched and sent.
data PendingMessage = PendingMessage
  { pmPayload  :: !ByteArray
  , pmResult   :: !(TMVar (Either KafkaException ()))
  }

-- | Operations enqueued to a broker thread.
data BrokerOp
  = BrokerProduce
      !TopicName
      {-# UNPACK #-} !Int32                         -- partition
      !PendingMessage
  | BrokerSendRaw
      !BSL.ByteString                                -- pre-encoded request
      !(TMVar (Either KafkaException ByteArray))     -- raw response slot
  | BrokerFlush
      !(TMVar ())                                  -- signal when flush is done
  | BrokerShutdown

-- | Inflight request entry — how to dispatch the response.
data InflightEntry
  = InflightRaw    !(TMVar (Either KafkaException ByteArray))
  | InflightBatch  !TopicName ![(Int32, [TMVar (Either KafkaException ())])]
    -- ^ topic, [(partition, [callbacks])]

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

batchAdd :: Batch -> TopicName -> Int32 -> PendingMessage -> Batch
batchAdd (Batch groups cnt bytes) topic part msg = Batch
  { batchGroups = Map.alter addMsg (topic, part) groups
  , batchCount  = cnt + 1
  , batchBytes  = bytes + sizeofByteArray (pmPayload msg)
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
  { beNodeId      :: {-# UNPACK #-} !Int32
  , beBrokerAddress        :: !BrokerAddress
  , beOps         :: !(TBQueue BrokerOp)
  , beState       :: !(TVar BrokerState)
  , beInflight    :: !(TVar (IntMap InflightEntry))
  , beCorrCounter :: !(IORef Int32)
  , beReconnect   :: !(IORef ReconnectState)
  , beConfig      :: !ClientConfig
  , beShutdown    :: !(TVar Bool)
  , beThread      :: !(IORef (Maybe ThreadId))
  , beApiVersions :: !(TVar (Maybe [ApiVersionEntry]))
  }

------------------------------------------------------------------------
-- Construction
------------------------------------------------------------------------

newBrokerEnv :: ClientConfig -> Int32 -> BrokerAddress -> IO BrokerEnv
newBrokerEnv cfg nodeId peer = do
  ops       <- newTBQueueIO (fromIntegral (ccQueueSize cfg) :: Natural)
  state     <- newTVarIO BrokerInit
  inflight  <- newTVarIO IM.empty
  corrId    <- newIORef 0
  reconn    <- newIORef (newReconnectState (ccReconnectMs cfg)
                                           (ccReconnectMaxMs cfg)
                                           (fromIntegral nodeId + 12345))
  shutdown  <- newTVarIO False
  threadRef <- newIORef Nothing
  apiVers   <- newTVarIO Nothing
  pure BrokerEnv
    { beNodeId      = nodeId
    , beBrokerAddress        = peer
    , beOps         = ops
    , beState       = state
    , beInflight    = inflight
    , beCorrCounter = corrId
    , beReconnect   = reconn
    , beConfig      = cfg
    , beShutdown    = shutdown
    , beThread      = threadRef
    , beApiVersions = apiVers
    }

------------------------------------------------------------------------
-- Lifecycle
------------------------------------------------------------------------

-- | Start the broker's green thread.
startBrokerThread :: BrokerEnv -> IO ()
startBrokerThread env = do
  tid <- forkIO (brokerThreadMain env)
  writeIORef (beThread env) (Just tid)

-- | Request clean shutdown of the broker thread.
stopBroker :: BrokerEnv -> IO ()
stopBroker env = do
  atomically $ writeTVar (beShutdown env) True
  atomically $ writeTBQueue (beOps env) BrokerShutdown

------------------------------------------------------------------------
-- Public: enqueue operations
------------------------------------------------------------------------

-- | Enqueue a message for batched produce. Returns a TMVar that will
-- be filled with the delivery result.
enqueueProduce :: BrokerEnv -> TopicName -> Int32 -> ByteArray
              -> IO (TMVar (Either KafkaException ()))
enqueueProduce env topic part payload = do
  result <- newEmptyTMVarIO
  let msg = PendingMessage payload result
  atomically $ writeTBQueue (beOps env) (BrokerProduce topic part msg)
  pure result

-- | Enqueue a pre-encoded request (metadata, fetch, etc.).
-- Returns a TMVar that will be filled with the raw response bytes.
enqueueRequest :: BrokerEnv -> BSL.ByteString
              -> IO (TMVar (Either KafkaException ByteArray))
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
-- Spawns a receiver thread and runs the sender loop.
-- When either fails, the session ends and we reconnect.
runBrokerSession :: BrokerEnv -> Kafka -> IO ()
runBrokerSession env kafka = do
  -- Handshake: send ApiVersions to discover broker capabilities
  performHandshake env kafka

  -- Main loop: sender + receiver threads
  senderTid <- myThreadId
  recvTid <- forkIO $ do
    r <- try (receiverLoop env kafka)
    case r of
      Left (e :: SomeException) -> throwTo senderTid e
      Right _ -> pure ()
  r <- try (senderLoop env kafka)
  killThread recvTid
  case r of
    Left (e :: SomeException) -> throwTo senderTid e >> error "unreachable"
    Right a -> pure a

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
          case Smith.parseByteArray parseApiVersionsResponse responseBytes of
            Smith.Failure _ -> pure ()
            Smith.Success (Smith.Slice _ _ avResp) ->
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
flushBatch env kafka batch = do
  let groups = Map.toList (batchGroups batch)
  mapM_ (sendPartitionBatch env kafka) groups

-- | Send a batch for a single (topic, partition) group.
sendPartitionBatch :: BrokerEnv -> Kafka -> ((TopicName, Int32), [PendingMessage]) -> IO ()
sendPartitionBatch env kafka ((topic, part), msgsRev) = do
  let msgs = reverse msgsRev
      payloads = messagesToPayloadArray msgs
      timeoutMs = ccRequestTimeoutMs (beConfig env)
      cfg = beConfig env
      reqBytes = produceRequest
        (acksToInt16 (ccAcks cfg))
        (ccClientId cfg)
        timeoutMs topic part payloads

  corrId <- nextCorrId (beCorrCounter env)
  let patched = patchCorrelationIdLBS corrId reqBytes

  let callbacks = [(part, fmap pmResult msgs)]
  atomically $ modifyTVar' (beInflight env) $
    IM.insert (fromIntegral corrId) (InflightBatch topic callbacks)

  result <- try @IOException $ NBSL.sendAll (getSocket kafka) patched
  case result of
    Left err -> do
      let kafkaErr = Left (KafkaIOError (show err))
      mapM_ (\m -> atomically $ void $ tryPutTMVar (pmResult m) kafkaErr) msgs
      atomically $ modifyTVar' (beInflight env) $ IM.delete (fromIntegral corrId)
    Right () -> pure ()

-- | Build an UnliftedArray ByteArray from pending message payloads.
messagesToPayloadArray :: [PendingMessage] -> UnliftedArray ByteArray
messagesToPayloadArray msgs = runUnliftedArray $ do
  let n = length msgs
  arr <- newUnliftedArray n mempty
  go arr 0 msgs
  pure arr
  where
    go _ _ [] = pure ()
    go arr !i (m:ms) = do
      writeUnliftedArray arr i (pmPayload m)
      go arr (i + 1) ms

------------------------------------------------------------------------
-- Raw request send
------------------------------------------------------------------------

sendRawRequest :: BrokerEnv -> Kafka -> BSL.ByteString
              -> TMVar (Either KafkaException ByteArray) -> IO ()
sendRawRequest env kafka reqBytes respVar = do
  corrId <- nextCorrId (beCorrCounter env)
  let patched = patchCorrelationIdLBS corrId reqBytes

  atomically $ modifyTVar' (beInflight env) $
    IM.insert (fromIntegral corrId) (InflightRaw respVar)

  result <- try @IOException $ NBSL.sendAll (getSocket kafka) patched
  case result of
    Left err -> do
      atomically $ void $ tryPutTMVar respVar (Left (KafkaIOError (show err)))
      atomically $ modifyTVar' (beInflight env) $ IM.delete (fromIntegral corrId)
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
dispatchResponse :: BrokerEnv -> ByteArray -> IO ()
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
    Just (InflightBatch _topic callbacks) ->
      -- For now, assume success if we got a response.
      -- TODO: parse ProduceResponse and check per-partition error codes
      mapM_ (\(_part, tmvars) ->
        mapM_ (\tv -> atomically $ tryPutTMVar tv (Right ())) tmvars
      ) callbacks

------------------------------------------------------------------------
-- Failure handling
------------------------------------------------------------------------

-- | Fail all inflight requests (called when connection drops).
failAllInflight :: BrokerEnv -> IO ()
failAllInflight env = do
  entries <- atomically $ do
    m <- readTVar (beInflight env)
    writeTVar (beInflight env) IM.empty
    pure m
  let err = KafkaException "broker connection lost"
  mapM_ (failEntry err) (IM.elems entries)
  where
    failEntry err (InflightRaw respVar) =
      void $ atomically $ tryPutTMVar respVar (Left err)
    failEntry err (InflightBatch _ callbacks) =
      mapM_ (\(_, tmvars) ->
        mapM_ (\tv -> void $ atomically $ tryPutTMVar tv (Left err)) tmvars
      ) callbacks

------------------------------------------------------------------------
-- Correlation ID
------------------------------------------------------------------------

nextCorrId :: IORef Int32 -> IO Int32
nextCorrId ref = atomicModifyIORef' ref $ \n -> (n + 1, n)

-- | Extract correlation ID from the first 4 bytes of a response body.
-- Kafka response format: [4-byte size (already consumed)] [4-byte corrId] [...]
-- getKafkaResponse returns the body (starting with corrId).
extractCorrelationId :: ByteArray -> Int
extractCorrelationId ba =
  fromIntegral (byteSwap32 (indexByteArray ba 0 :: Word32))

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
