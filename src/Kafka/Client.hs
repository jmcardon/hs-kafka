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

import Control.Concurrent (threadDelay)
import Control.Concurrent.STM
import Control.Monad (forM_, unless)
import Data.Int (Int32, Int16)
import Data.IntMap.Strict (IntMap)
import Data.IORef
import Data.Map.Strict (Map)
import Data.Primitive.ByteArray (ByteArray)

import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as Map
import qualified Data.Bytes.Parser as Smith

import Kafka.Common
import Kafka.Internal.Broker
import Kafka.Internal.Config
import Kafka.Internal.Metadata.Request (metadataRequest)
import qualified Kafka.Internal.Metadata.Response as M
import Kafka.Internal.Metadata.Response (MetadataResponse(..), MetadataBroker(..), MetadataTopic(..), MetadataPartition(..), getMetadataResponse, parseMetadataResponse)

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
  atomically $ writeTVar (kcShutdown client) True
  brokers <- readTVarIO (kcBrokers client)
  forM_ (IM.elems brokers) stopBroker

------------------------------------------------------------------------
-- Broker selection
------------------------------------------------------------------------

-- | Pick any broker in BrokerUp state. Returns Nothing if none available.
anyBroker :: KafkaClient -> IO (Maybe BrokerEnv)
anyBroker client = do
  brokers <- readTVarIO (kcBrokers client)
  findUp (IM.elems brokers)
  where
    findUp [] = pure Nothing
    findUp (env:rest) = do
      st <- readTVarIO (beState env)
      if st == BrokerUp
        then pure (Just env)
        else findUp rest

-- | Get the broker thread for a specific node ID.
brokerById :: KafkaClient -> Int32 -> IO (Maybe BrokerEnv)
brokerById client nodeId = do
  brokers <- readTVarIO (kcBrokers client)
  pure (IM.lookup (fromIntegral nodeId) brokers)

-- | Find the broker for a topic-partition's leader.
-- Falls back to any UP broker if leader is unknown or unavailable.
leaderBrokerFor :: KafkaClient -> TopicName -> Int32 -> IO (Maybe BrokerEnv)
leaderBrokerFor client topic partition = do
  meta <- readTVarIO (kcMetadata client)
  case Map.lookup (topic, partition) (mcPartitionLeaders meta) of
    Just nodeId -> do
      mBroker <- brokerById client nodeId
      case mBroker of
        Just env -> do
          st <- readTVarIO (beState env)
          if st == BrokerUp then pure (Just env) else anyBroker client
        Nothing -> anyBroker client
    Nothing -> anyBroker client

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
parseMetadata :: ByteArray -> Either String MetadataResponse
parseMetadata bytes =
  case Smith.parseByteArray parseMetadataResponse bytes of
    Smith.Failure e          -> Left e
    Smith.Success (Smith.Slice _ _ a) -> Right a

-- | Update the metadata cache from a MetadataResponse.
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
  atomically $ modifyTVar' (kcMetadata client) $ \old -> MetadataCache
    { mcPartitionLeaders = Map.union newLeaders (mcPartitionLeaders old)
    , mcPartitionCounts  = Map.union newCounts  (mcPartitionCounts  old)
    }

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
    checkAnyUp (env:rest) = do
      st <- readTVar (beState env)
      if st == BrokerUp then pure True else checkAnyUp rest
