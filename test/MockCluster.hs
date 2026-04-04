{-# LANGUAGE ForeignFunctionInterface #-}

-- | FFI bindings to librdkafka's mock cluster API for integration testing.
--
-- Uses the same approach as hw-kafka-client: link to librdkafka dynamically
-- via extra-libraries: rdkafka. The mock cluster provides real Kafka protocol
-- endpoints without requiring a real Kafka broker.
--
-- Pattern: create a minimal rd_kafka_t harness, then create a mock cluster
-- from it. The mock cluster listens on localhost with random ports.
-- Test code connects to it using the bootstrap addresses.
module MockCluster
  ( MockCluster(..)
  , withMockCluster
  , createMockCluster
  , destroyMockCluster
  , mockCreateTopic
  , mockBrokerDown
  , mockBrokerUp
  , mockPushRequestErrors
  , mockClearRequestErrors
  , mockPartitionSetLeader
  , mockBrokerSetRtt
  , parseBootstraps
  ) where

import Control.Exception (bracket)
import Data.Int (Int16, Int32)
import Foreign.C.String (CString, withCString, peekCString)
import Foreign.C.Types (CInt(..), CSize(..))
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Marshal.Array (withArrayLen)
import Foreign.Ptr (Ptr, nullPtr)

import Kafka.Internal.Config (BrokerAddress(..))

------------------------------------------------------------------------
-- Opaque C types
------------------------------------------------------------------------

data RdKafkaConf
data RdKafka
data RdKafkaMockCluster

------------------------------------------------------------------------
-- FFI imports
------------------------------------------------------------------------

-- rd_kafka_type_t: RD_KAFKA_PRODUCER = 0, RD_KAFKA_CONSUMER = 1
rdKafkaProducer :: CInt
rdKafkaProducer = 0

foreign import ccall unsafe "rdkafka.h rd_kafka_conf_new"
  c_rd_kafka_conf_new :: IO (Ptr RdKafkaConf)

foreign import ccall unsafe "rdkafka.h rd_kafka_conf_set"
  c_rd_kafka_conf_set :: Ptr RdKafkaConf -> CString -> CString -> CString -> CSize -> IO CInt

foreign import ccall unsafe "rdkafka.h rd_kafka_new"
  c_rd_kafka_new :: CInt -> Ptr RdKafkaConf -> CString -> CSize -> IO (Ptr RdKafka)

foreign import ccall unsafe "rdkafka.h rd_kafka_destroy"
  c_rd_kafka_destroy :: Ptr RdKafka -> IO ()

foreign import ccall unsafe "rdkafka_mock.h rd_kafka_mock_cluster_new"
  c_rd_kafka_mock_cluster_new :: Ptr RdKafka -> CInt -> IO (Ptr RdKafkaMockCluster)

foreign import ccall unsafe "rdkafka_mock.h rd_kafka_mock_cluster_destroy"
  c_rd_kafka_mock_cluster_destroy :: Ptr RdKafkaMockCluster -> IO ()

foreign import ccall unsafe "rdkafka_mock.h rd_kafka_mock_cluster_bootstraps"
  c_rd_kafka_mock_cluster_bootstraps :: Ptr RdKafkaMockCluster -> IO CString

foreign import ccall unsafe "rdkafka_mock.h rd_kafka_mock_topic_create"
  c_rd_kafka_mock_topic_create :: Ptr RdKafkaMockCluster -> CString -> CInt -> CInt -> IO CInt

foreign import ccall unsafe "rdkafka_mock.h rd_kafka_mock_broker_set_down"
  c_rd_kafka_mock_broker_set_down :: Ptr RdKafkaMockCluster -> Int32 -> IO CInt

foreign import ccall unsafe "rdkafka_mock.h rd_kafka_mock_broker_set_up"
  c_rd_kafka_mock_broker_set_up :: Ptr RdKafkaMockCluster -> Int32 -> IO CInt

foreign import ccall unsafe "rdkafka_mock.h rd_kafka_mock_push_request_errors_array"
  c_rd_kafka_mock_push_request_errors_array
    :: Ptr RdKafkaMockCluster -> Int16 -> CSize -> Ptr CInt -> IO ()

foreign import ccall unsafe "rdkafka_mock.h rd_kafka_mock_clear_request_errors"
  c_rd_kafka_mock_clear_request_errors
    :: Ptr RdKafkaMockCluster -> Int16 -> IO ()

foreign import ccall unsafe "rdkafka_mock.h rd_kafka_mock_partition_set_leader"
  c_rd_kafka_mock_partition_set_leader
    :: Ptr RdKafkaMockCluster -> CString -> Int32 -> Int32 -> IO CInt

foreign import ccall unsafe "rdkafka_mock.h rd_kafka_mock_broker_set_rtt"
  c_rd_kafka_mock_broker_set_rtt
    :: Ptr RdKafkaMockCluster -> Int32 -> CInt -> IO CInt

------------------------------------------------------------------------
-- High-level wrapper
------------------------------------------------------------------------

data MockCluster = MockCluster
  { mcRdKafka    :: !(Ptr RdKafka)
  , mcCluster    :: !(Ptr RdKafkaMockCluster)
  , mcBootstraps :: !String
  }

-- | Create a mock Kafka cluster with the given number of brokers.
-- The cluster is destroyed when the action completes.
withMockCluster :: Int -> (MockCluster -> IO a) -> IO a
withMockCluster brokerCount = bracket (createMockCluster brokerCount) destroyMockCluster

-- | Create a mock cluster (caller must call 'destroyMockCluster').
createMockCluster :: Int -> IO MockCluster
createMockCluster brokerCount = do
  conf <- c_rd_kafka_conf_new
  withCString "client.id" $ \k ->
    withCString "mock-harness" $ \v ->
      allocaBytes 512 $ \errBuf ->
        c_rd_kafka_conf_set conf k v errBuf 512 >> pure ()

  rk <- allocaBytes 512 $ \errBuf ->
    c_rd_kafka_new rdKafkaProducer conf errBuf 512
  if rk == nullPtr
    then error "failed to create rd_kafka_t harness"
    else pure ()

  cluster <- c_rd_kafka_mock_cluster_new rk (fromIntegral brokerCount)
  if cluster == nullPtr
    then do
      c_rd_kafka_destroy rk
      error "failed to create mock cluster"
    else pure ()

  bootstrapsCStr <- c_rd_kafka_mock_cluster_bootstraps cluster
  bootstraps <- peekCString bootstrapsCStr

  pure MockCluster
    { mcRdKafka    = rk
    , mcCluster    = cluster
    , mcBootstraps = bootstraps
    }

-- | Destroy a mock cluster.
destroyMockCluster :: MockCluster -> IO ()
destroyMockCluster mc = do
  c_rd_kafka_mock_cluster_destroy (mcCluster mc)
  c_rd_kafka_destroy (mcRdKafka mc)

-- | Create a topic on the mock cluster.
mockCreateTopic :: MockCluster -> String -> Int -> Int -> IO ()
mockCreateTopic mc topic partitions replication =
  withCString topic $ \cTopic -> do
    err <- c_rd_kafka_mock_topic_create
      (mcCluster mc) cTopic
      (fromIntegral partitions) (fromIntegral replication)
    if err /= 0
      then error $ "mock topic create failed with error code " ++ show err
      else pure ()

-- | Bring a mock broker down (simulates network partition).
mockBrokerDown :: MockCluster -> Int32 -> IO ()
mockBrokerDown mc brokerId = do
  _ <- c_rd_kafka_mock_broker_set_down (mcCluster mc) brokerId
  pure ()

-- | Bring a mock broker back up.
mockBrokerUp :: MockCluster -> Int32 -> IO ()
mockBrokerUp mc brokerId = do
  _ <- c_rd_kafka_mock_broker_set_up (mcCluster mc) brokerId
  pure ()

-- | Push error codes onto the mock cluster's error stack for a given API key.
-- The next N requests for that API key will return these errors in order.
-- API keys: 0 = Produce, 1 = Fetch, 3 = Metadata, etc.
mockPushRequestErrors :: MockCluster -> Int16 -> [CInt] -> IO ()
mockPushRequestErrors mc apiKey errs =
  withArrayLen errs $ \len ptr ->
    c_rd_kafka_mock_push_request_errors_array
      (mcCluster mc) apiKey (fromIntegral len) ptr

-- | Clear the error stack for a given API key.
mockClearRequestErrors :: MockCluster -> Int16 -> IO ()
mockClearRequestErrors mc apiKey =
  c_rd_kafka_mock_clear_request_errors (mcCluster mc) apiKey

-- | Set the leader broker for a topic-partition.
-- broker_id = -1 makes the partition leader-less.
mockPartitionSetLeader :: MockCluster -> String -> Int32 -> Int32 -> IO ()
mockPartitionSetLeader mc topic partition brokerId =
  withCString topic $ \cTopic -> do
    _ <- c_rd_kafka_mock_partition_set_leader (mcCluster mc) cTopic partition brokerId
    pure ()

-- | Set artificial RTT (latency) on a broker in milliseconds.
mockBrokerSetRtt :: MockCluster -> Int32 -> Int -> IO ()
mockBrokerSetRtt mc brokerId rttMs = do
  _ <- c_rd_kafka_mock_broker_set_rtt (mcCluster mc) brokerId (fromIntegral rttMs)
  pure ()

------------------------------------------------------------------------
-- Utilities
------------------------------------------------------------------------

-- | Parse a bootstrap string like "localhost:12345,localhost:12346"
-- into a list of BrokerAddress.
parseBootstraps :: String -> [BrokerAddress]
parseBootstraps = map parseOne . splitOn ','
  where
    parseOne s =
      let (host, rest) = break (== ':') s
          port = case rest of
            ':':p -> read p
            _     -> 9092
      in BrokerAddress host (fromIntegral (port :: Int))

    splitOn :: Char -> String -> [String]
    splitOn _ [] = []
    splitOn c s =
      let (before, after) = break (== c) s
      in before : case after of
           [] -> []
           _:rest -> splitOn c rest
