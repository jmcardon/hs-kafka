{-# language
    BangPatterns
  , LambdaCase
  , OverloadedStrings
  , ScopedTypeVariables
  #-}

-- | Multi-broker Kafka client with metadata management.
--
-- Manages broker threads, metadata cache, and request routing.
-- Modeled on librdkafka's main thread (rdkafka.c:rd_kafka_thread_main)
-- which coordinates metadata refresh and broker management.
module Kafka.Client
  ( KafkaClient(..)
  , MetadataCache(..)
  , newClient
  , closeClient
  , anyBroker
  , refreshTopicMetadata
  , partitionCountFor
  , leaderBrokerFor
  ) where

import Control.Concurrent.STM
import Control.Monad (forM_, unless)
import Data.Int (Int32)
import Data.ByteString (ByteString)
import Data.IntMap.Strict (IntMap)
import Data.Map.Strict (Map)

import qualified Data.ByteString.Char8 as BS8
import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as Map

import Kafka.Common
import Kafka.Internal.Broker
import Kafka.Internal.Config
import Kafka.Internal.Metadata.Request (metadataRequest)
import Kafka.Internal.Metadata.Response (MetadataResponse(..), MetadataBroker(..), MetadataTopic(..), MetadataPartition(..), parseMetadataResponseV12)
import qualified Kafka.Internal.Metadata.Response as M
import qualified Kafka.Internal.Wire as Wire

------------------------------------------------------------------------
-- Types
------------------------------------------------------------------------

data KafkaClient = KafkaClient
  { kcBrokers  :: !(TVar (IntMap BrokerEnv))
    -- ^ Active broker threads, keyed by node ID.
  , kcMetadata :: !(TVar MetadataCache)
    -- ^ Cached topic metadata (partition counts + leaders).
  , kcConfig   :: !ClientConfig
  , kcShutdown :: !(TVar Bool)
  }

data MetadataCache = MetadataCache
  { mcPartitionLeaders :: !(Map (TopicName, Int32) Int32)
    -- ^ (topic, partition) → leader node ID
  , mcPartitionCounts  :: !(Map TopicName Int32)
    -- ^ topic → number of partitions
  }

emptyMetadata :: MetadataCache
emptyMetadata = MetadataCache Map.empty Map.empty

------------------------------------------------------------------------
-- Client lifecycle
------------------------------------------------------------------------

-- | Create a new multi-broker Kafka client.
--
-- Connects to all bootstrap servers (one green thread per broker),
-- waits for at least one to come up, then returns the client.
--
-- The caller should call 'closeClient' when done.
newClient :: ClientConfig -> IO (Either KafkaException KafkaClient)
newClient cfg = case ccBootstrap cfg of
  [] -> pure (Left (KafkaException "no bootstrap servers configured"))
  peers -> do
    brokersVar <- newTVarIO IM.empty
    metaVar    <- newTVarIO emptyMetadata
    shutdownV  <- newTVarIO False

    let client = KafkaClient brokersVar metaVar cfg shutdownV

    -- Create a broker thread for each bootstrap server.
    -- Assign temporary node IDs (0, 1, 2, ...) since we don't know
    -- real node IDs until metadata response.
    envs <- sequence
      [ do env <- newBrokerEnv cfg (fromIntegral i) peer
           startBrokerThread env
           pure (fromIntegral i :: Int, env)
      | (i, peer) <- zip [(0::Int)..] peers
      ]

    atomically $ writeTVar brokersVar (IM.fromList envs)

    -- Wait for at least one broker to reach BrokerUp (with timeout)
    connected <- waitForAnyBroker client 10000000 -- 10s timeout
    if connected
      then pure (Right client)
      else do
        closeClient client
        pure (Left (KafkaException "could not connect to any bootstrap server"))

-- | Shut down all broker threads and clean up.
closeClient :: KafkaClient -> IO ()
closeClient client = do
  brokers <- atomically $ do
    writeTVar (kcShutdown client) True
    readTVar (kcBrokers client)
  mapM_ stopBroker (IM.elems brokers)

------------------------------------------------------------------------
-- Broker selection
------------------------------------------------------------------------

-- | Pick any broker in BrokerUp state. Returns Nothing if none available.
anyBroker :: KafkaClient -> IO (Maybe BrokerEnv)
anyBroker client = atomically $ do
  brokers <- readTVar (kcBrokers client)
  findUp (IM.elems brokers)
  where
    findUp [] = pure Nothing
    findUp (env:rest) = readTVar (beState env) >>= \case
      BrokerUp -> pure (Just env)
      _        -> findUp rest

-- | Find the broker for a topic-partition's leader.
-- Falls back to any UP broker if leader is unknown or unavailable.
leaderBrokerFor :: KafkaClient -> TopicName -> Int32 -> IO (Maybe BrokerEnv)
leaderBrokerFor client topic partition = atomically $ do
  meta <- readTVar (kcMetadata client)
  brokers <- readTVar (kcBrokers client)
  case Map.lookup (topic, partition) (mcPartitionLeaders meta)
       >>= \nodeId -> IM.lookup (fromIntegral nodeId) brokers of
    Just env -> readTVar (beState env) >>= \case
      BrokerUp -> pure (Just env)
      _        -> findUp (IM.elems brokers)
    Nothing -> findUp (IM.elems brokers)
  where
    findUp [] = pure Nothing
    findUp (env:rest) = readTVar (beState env) >>= \case
      BrokerUp -> pure (Just env)
      _        -> findUp rest

------------------------------------------------------------------------
-- Metadata
------------------------------------------------------------------------

-- | Get the partition count for a topic from the metadata cache.
partitionCountFor :: KafkaClient -> TopicName -> IO (Maybe Int32)
partitionCountFor client topic = do
  meta <- readTVarIO (kcMetadata client)
  pure (Map.lookup topic (mcPartitionCounts meta))

-- | Refresh metadata for a specific topic.
-- Sends a Metadata request through any available broker and updates
-- the metadata cache with the response.
refreshTopicMetadata :: KafkaClient -> TopicName -> IO (Either KafkaException ())
refreshTopicMetadata client topic = do
  mBroker <- anyBroker client
  case mBroker of
    Nothing -> pure (Left (KafkaException "no broker available for metadata request"))
    Just env -> do
      -- Encode metadata request using existing encoder
      let reqChunks = metadataRequest topic NeverCreate

      -- Send through broker thread and wait for response
      respVar <- enqueueRequest env reqChunks
      response <- atomically $ readTMVar respVar

      case response of
        Left err -> pure (Left err)
        Right bytes -> case parseMetadata bytes of
          Left parseErr -> pure (Left (KafkaParseException parseErr))
          Right metaResp -> do
            updateMetadataCache client metaResp
            pure (Right ())

-- | Parse raw response bytes into MetadataResponse.
parseMetadata :: ByteString -> Either String MetadataResponse
parseMetadata bytes = case Wire.runWire parseMetadataResponseV12 bytes of
  Nothing -> Left "failed to parse MetadataResponse v12"
  Just a  -> Right a

-- | Update the metadata cache and broker map from a MetadataResponse.
-- Re-keys the broker IntMap from temporary bootstrap IDs to real node IDs
-- discovered via metadata, so partition leader lookups find the right broker.
updateMetadataCache :: KafkaClient -> MetadataResponse -> IO ()
updateMetadataCache client resp = do
  let newLeaders = Map.fromList
        [ ((name t, M.partitionIndex p), leaderId p)
        | t <- topics resp
        , p <- partitions t
        , errorCode t == 0
        , partitionErrorCode p == 0
        ]
      newCounts = Map.fromList
        [ (name t, fromIntegral (length (partitions t)))
        | t <- topics resp
        , errorCode t == 0
        ]
  -- Re-key broker map: match existing BrokerEnvs by address to real node IDs
  let metaBrokers = brokers resp
  atomically $ do
    modifyTVar' (kcMetadata client) $ \old -> MetadataCache
      { mcPartitionLeaders = Map.union newLeaders (mcPartitionLeaders old)
      , mcPartitionCounts  = Map.union newCounts  (mcPartitionCounts  old)
      }
    oldMap <- readTVar (kcBrokers client)
    let addrMap = Map.fromList
          [ (brokerAddrKey (beBrokerAddress env), env)
          | env <- IM.elems oldMap
          ]
        rekeyed = IM.fromList
          [ (fromIntegral (M.nodeId mb), env { beNodeId = M.nodeId mb })
          | mb <- metaBrokers
          , Just env <- [Map.lookup (metaBrokerAddrKey mb) addrMap]
          ]
    -- Only update if we found matches (don't lose brokers)
    if not (IM.null rekeyed)
      then writeTVar (kcBrokers client) rekeyed
      else pure ()
  where
    brokerAddrKey :: BrokerAddress -> (String, Int)
    brokerAddrKey (BrokerAddress h p) = (h, fromIntegral p)

    metaBrokerAddrKey :: MetadataBroker -> (String, Int)
    metaBrokerAddrKey mb = (BS8.unpack (M.host mb), fromIntegral (M.port mb))

------------------------------------------------------------------------
-- Internal helpers
------------------------------------------------------------------------

-- | Wait until at least one broker reaches BrokerUp state, or timeout.
-- Returns True if a broker connected, False on timeout.
waitForAnyBroker :: KafkaClient -> Int -> IO Bool
waitForAnyBroker client timeoutUs = do
  timer <- registerDelay timeoutUs
  atomically $ do
    -- Check if any broker is up
    brokers <- readTVar (kcBrokers client)
    anyUp <- checkAnyUp (IM.elems brokers)
    if anyUp
      then pure True
      else do
        -- Check timeout
        timedOut <- readTVar timer
        if timedOut
          then pure False
          else retry
  where
    checkAnyUp [] = pure False
    checkAnyUp (env:rest) = readTVar (beState env) >>= \case
      BrokerUp -> pure True
      _        -> checkAnyUp rest
