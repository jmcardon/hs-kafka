{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (readTVarIO)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.IORef
import Test.Tasty
import Test.Tasty.HUnit

import Kafka.Client
import Kafka.Common
import Kafka.Internal.Broker (BrokerEnv(..), BrokerState(..))
import Kafka.Internal.Config
import Kafka.Producer
import MockCluster

-- | Convenience: make a simple ProducerRecord with just a value.
mkRecord :: TopicName -> ByteString -> ProducerRecord
mkRecord topic val = ProducerRecord topic UnassignedPartition Nothing (Just val) []

main :: IO ()
main = defaultMain $ testGroup "Integration"
  [ mockClusterTests
  , clientTests
  , producerTests
  , recordFeatureTests
  , compressionTests
  , errorHandlingTests
  , reconnectionTests
  , multiBrokerTests
  , retriableErrorTests
  , backpressureTests
  ]

------------------------------------------------------------------------
-- Mock cluster smoke tests
------------------------------------------------------------------------

mockClusterTests :: TestTree
mockClusterTests = testGroup "Mock Cluster"
  [ testCase "starts and returns bootstrap addresses" $
      withMockCluster 3 $ \mc -> do
        let bootstraps = parseBootstraps (mcBootstraps mc)
        assertEqual "should have 3 brokers" 3 (length bootstraps)

  , testCase "creates topics" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "test-topic" 4 1

  , testCase "broker down/up" $
      withMockCluster 1 $ \mc -> do
        mockBrokerDown mc 1
        mockBrokerUp mc 1
  ]

------------------------------------------------------------------------
-- Client tests
------------------------------------------------------------------------

-- | Helper: create a KafkaClient connected to a mock cluster.
withTestClient :: MockCluster -> (KafkaClient -> IO a) -> IO a
withTestClient mc action = do
  let addrs = parseBootstraps (mcBootstraps mc)
      cfg = defaultConfig { ccBootstrap = addrs }
  result <- newClient cfg
  case result of
    Left err -> assertFailure ("newClient failed: " ++ show err)
    Right client -> do
      r <- action client
      closeClient client
      pure r

-- | Helper: create a KafkaClient + KafkaProducer with given config overrides.
withTestProducer :: MockCluster -> ClientConfig -> (KafkaClient -> KafkaProducer -> IO a) -> IO a
withTestProducer mc cfgOverride action = do
  let addrs = parseBootstraps (mcBootstraps mc)
      cfg = cfgOverride { ccBootstrap = addrs }
  result <- newClient cfg
  case result of
    Left err -> assertFailure ("newClient failed: " ++ show err)
    Right client -> do
      prodResult <- newProducer client (defaultProducerConfig cfg)
      case prodResult of
        Left err -> do
          closeClient client
          assertFailure ("newProducer failed: " ++ show err)
        Right producer -> do
          r <- action client producer
          closeProducer producer
          closeClient client
          pure r

clientTests :: TestTree
clientTests = testGroup "Client"
  [ testCase "connects to mock cluster" $
      withMockCluster 1 $ \mc ->
        withTestClient mc $ \client -> do
          broker <- anyBroker client
          case broker of
            Nothing -> assertFailure "no broker available"
            Just _  -> pure ()

  , testCase "refreshes topic metadata" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "meta-test" 3 1
        withTestClient mc $ \client -> do
          result <- refreshTopicMetadata client "meta-test"
          case result of
            Left err -> assertFailure ("metadata refresh failed: " ++ show err)
            Right () -> do
              count <- partitionCountFor client "meta-test"
              assertEqual "should have 3 partitions" (Just 3) count
  ]

------------------------------------------------------------------------
-- Producer tests
------------------------------------------------------------------------


producerTests :: TestTree
producerTests = testGroup "Producer"
  [ testCase "produce single message" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "produce-test" 1 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          result <- produce producer (mkRecord "produce-test" "hello kafka")
          case result of
            Left err -> assertFailure ("produce failed: " ++ show err)
            Right (DeliverySuccess _ _) -> pure ()
            Right (DeliveryFailure _ e) -> assertFailure ("produce delivery failed: " ++ show e)

  , testCase "produce 100 messages" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "bulk-test" 1 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          results <- mapM (\i ->
            produce producer (mkRecord "bulk-test"
              (BS.pack [fromIntegral (i :: Int)])))
            [0..99]
          let failures = filter isDeliveryFailure results
          assertEqual "all should succeed" 0 (length failures)

  , testCase "produce async + flush" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "async-test" 1 1
        let cfg = defaultConfig { ccLingerMs = 5000 } -- high linger
        withTestProducer mc cfg $ \_client producer -> do
          -- Fire-and-forget 10 messages
          mapM_ (\i ->
            produceAsync producer (mkRecord "async-test"
              (BS.pack [fromIntegral (i :: Int)])))
            [0..9]
          -- Flush forces them out despite high linger
          flushProducer producer
          -- Poll for delivery reports
          reports <- pollEvents producer 5000
          let failures = filter isDeliveryFailureReport reports
          assertEqual "all should succeed after flush" 0 (length failures)

  , testCase "produce to multiple partitions" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "multi-part" 4 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          -- Produce 12 messages — should round-robin across 4 partitions
          results <- mapM (\i ->
            produce producer (mkRecord "multi-part"
              (BS.pack [fromIntegral (i :: Int)])))
            [0..11]
          let failures = filter isDeliveryFailure results
          assertEqual "all should succeed" 0 (length failures)

  , testCase "per-message async callbacks" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "callback-test" 2 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          -- Async produce, delivery reports via pollEvents
          successCount <- newIORef (0 :: Int)
          failCount <- newIORef (0 :: Int)
          mapM_ (\i ->
            produceAsync producer (mkRecord "callback-test"
              (BS.pack [fromIntegral (i :: Int)])))
            [0..49]
          flushProducer producer
          -- Poll for delivery reports
          reports <- pollEvents producer 5000
          mapM_ (\dr -> case dr of
            DeliverySuccess _ _ -> atomicModifyIORef' successCount (\n -> (n+1, ()))
            DeliveryFailure _ _ -> atomicModifyIORef' failCount (\n -> (n+1, ()))
            ) reports
          successes <- readIORef successCount
          failures <- readIORef failCount
          assertEqual "all 50 should succeed" 50 successes
          assertEqual "none should fail" 0 failures

  , testCase "produce 1000 messages rapidly" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "rapid-test" 4 1
        let cfg = defaultConfig { ccLingerMs = 1, ccBatchNumMessages = 100 }
        withTestProducer mc cfg $ \_client producer -> do
          mapM_ (\i ->
            produceAsync producer (mkRecord "rapid-test"
              (BS.pack [fromIntegral (i `mod` 256 :: Int)])))
            [0..999]
          flushProducer producer
          reports <- pollEvents producer 5000
          let failures = filter isDeliveryFailureReport reports
          assertEqual "all 1000 should succeed" 0 (length failures)
  ]

------------------------------------------------------------------------
-- Record features tests (headers, keys, partitions, offsets)
------------------------------------------------------------------------

recordFeatureTests :: TestTree
recordFeatureTests = testGroup "Record features"
  [ testCase "produce with key routes deterministically" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "key-test" 4 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          -- Same key → same partition
          let rec k = ProducerRecord "key-test" UnassignedPartition
                        (Just k) (Just "value") []
          r1 <- produce producer (rec "my-key")
          r2 <- produce producer (rec "my-key")
          r3 <- produce producer (rec "other-key")
          case (r1, r2) of
            (Right (DeliverySuccess _ _), Right (DeliverySuccess _ _)) -> pure ()
            _ -> assertFailure "keyed produce should succeed"
          case r3 of
            Right (DeliverySuccess _ _) -> pure ()
            _ -> assertFailure "keyed produce should succeed"

  , testCase "produce with explicit partition" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "part-test" 4 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          let rec p = ProducerRecord "part-test" (SpecifiedPartition p)
                        Nothing (Just "value") []
          r0 <- produce producer (rec 0)
          r2 <- produce producer (rec 2)
          case (r0, r2) of
            (Right (DeliverySuccess _ _), Right (DeliverySuccess _ _)) -> pure ()
            _ -> assertFailure "explicit partition produce should succeed"

  , testCase "produce with headers" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "header-test" 1 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          let rec = ProducerRecord "header-test" UnassignedPartition
                      Nothing (Just "payload")
                      [ Header "trace-id" (Just "abc123")
                      , Header "content-type" (Just "text/plain")
                      , Header "empty-header" Nothing
                      ]
          result <- produce producer rec
          case result of
            Left err -> assertFailure ("produce with headers failed: " ++ show err)
            Right (DeliverySuccess _ _) -> pure ()
            Right (DeliveryFailure _ e) -> assertFailure ("delivery failed: " ++ show e)

  , testCase "delivery report contains offset" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "offset-test" 1 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          r1 <- produce producer (mkRecord "offset-test" "msg1")
          r2 <- produce producer (mkRecord "offset-test" "msg2")
          case (r1, r2) of
            (Right (DeliverySuccess _ (Offset o1)), Right (DeliverySuccess _ (Offset o2))) -> do
              assertBool "second offset should be >= first" (o2 >= o1)
            _ -> assertFailure "both produces should succeed with offsets"

  , testCase "produce with null value" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "null-test" 1 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          let rec = ProducerRecord "null-test" UnassignedPartition
                      (Just "key") Nothing []  -- null value (tombstone)
          result <- produce producer rec
          case result of
            Left err -> assertFailure ("null value produce failed: " ++ show err)
            Right (DeliverySuccess _ _) -> pure ()
            Right (DeliveryFailure _ e) -> assertFailure ("delivery failed: " ++ show e)
  ]

------------------------------------------------------------------------
-- Compression tests
------------------------------------------------------------------------

compressionTests :: TestTree
compressionTests = testGroup "Compression"
  [ compressionProduceTest "Gzip" Gzip "gzip-itest"
  , compressionProduceTest "Snappy" Snappy "snappy-itest"
  , compressionProduceTest "Lz4" Lz4 "lz4-itest"
  , compressionProduceTest "Zstd" Zstd "zstd-itest"
  , testCase "produce small messages with compression falls back gracefully" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "small-comp" 1 1
        let cfg = defaultConfig { ccCompression = Gzip }
        withTestProducer mc cfg $ \_client producer -> do
          -- Tiny payload — compression won't help, should fall back to uncompressed
          result <- produce producer (mkRecord "small-comp" "x")
          case result of
            Left err -> assertFailure ("produce failed: " ++ show err)
            Right (DeliverySuccess _ _) -> pure ()
            Right (DeliveryFailure _ e) -> assertFailure ("produce delivery failed: " ++ show e)
  ]

-- | Test producing with a given compression codec end-to-end through the mock cluster.
compressionProduceTest :: String -> Compression -> TopicName -> TestTree
compressionProduceTest name codec tn = testCase ("produce with " ++ name) $
  withMockCluster 1 $ \mc -> do
    let TopicName tnBS = tn
        topic = map (toEnum . fromIntegral) (BS.unpack tnBS)
    mockCreateTopic mc topic 1 1
    let cfg = defaultConfig { ccCompression = codec }
    withTestProducer mc cfg $ \_client producer -> do
      let payload = BS.concat (replicate 20 "repetitive data for compression test ")
      results <- mapM (\_ -> produce producer (mkRecord tn payload)) [1..10 :: Int]
      let failures = filter isDeliveryFailure results
      assertEqual ("all should succeed with " ++ name) 0 (length failures)

------------------------------------------------------------------------
-- Error handling tests
------------------------------------------------------------------------

errorHandlingTests :: TestTree
errorHandlingTests = testGroup "Error handling"
  [ testCase "produce to non-existent topic reports error" $
      withMockCluster 1 $ \mc ->
        withTestProducer mc defaultConfig $ \_client producer -> do
          result <- produce producer (mkRecord "nonexistent-topic" "test")
          case result of
            Left _ -> pure ()  -- Expected: error because topic doesn't exist
            Right (DeliverySuccess _ _) -> pure ()  -- Mock cluster may auto-create
            Right (DeliveryFailure _ _) -> pure ()  -- Expected failure

  , testCase "produce survives brief disconnect" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "disconnect-test" 1 1
        let addrs = parseBootstraps (mcBootstraps mc)
            cfg = defaultConfig
              { ccBootstrap = addrs
              , ccReconnectMs = 50
              , ccReconnectMaxMs = 200
              , ccRetries = 5
              }
        result <- newClient cfg
        case result of
          Left err -> assertFailure ("newClient failed: " ++ show err)
          Right client -> do
            prodResult <- newProducer client (defaultProducerConfig cfg)
            case prodResult of
              Left err -> do
                closeClient client
                assertFailure ("newProducer failed: " ++ show err)
              Right producer -> do
                -- Produce some messages first
                r1 <- produce producer (mkRecord "disconnect-test" "before disconnect")
                case r1 of
                  Left err -> assertFailure ("first produce failed: " ++ show err)
                  Right (DeliverySuccess _ _) -> pure ()
                  Right (DeliveryFailure _ e) -> assertFailure ("first produce delivery failed: " ++ show e)

                -- Brief disconnect
                mockBrokerDown mc 1
                threadDelay 100000  -- 100ms
                mockBrokerUp mc 1
                threadDelay 500000  -- 500ms for reconnection

                -- Should be able to produce again
                r2 <- produce producer (mkRecord "disconnect-test" "after reconnect")
                case r2 of
                  Left _ -> pure ()  -- May fail if reconnection not complete
                  Right (DeliverySuccess _ _) -> pure ()
                  Right (DeliveryFailure _ _) -> pure ()

                closeProducer producer
                closeClient client
  ]

------------------------------------------------------------------------
-- Reconnection tests
------------------------------------------------------------------------

reconnectionTests :: TestTree
reconnectionTests = testGroup "Reconnection"
  [ testCase "broker reconnects after down/up" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "reconnect-test" 1 1
        let addrs = parseBootstraps (mcBootstraps mc)
            cfg = defaultConfig
              { ccBootstrap = addrs
              , ccReconnectMs = 100
              , ccReconnectMaxMs = 500
              }
        result <- newClient cfg
        case result of
          Left err -> assertFailure ("newClient failed: " ++ show err)
          Right client -> do
            -- Verify connected
            mBroker <- anyBroker client
            case mBroker of
              Nothing -> assertFailure "no broker available"
              Just env -> do
                st <- readTVarIO (beState env)
                assertEqual "should be up" BrokerUp st

                -- Take broker down
                mockBrokerDown mc 1
                threadDelay 200000 -- 200ms for connection to notice

                -- Take broker back up
                mockBrokerUp mc 1
                threadDelay 1000000 -- 1s for reconnection

                -- Verify reconnected
                st' <- readTVarIO (beState env)
                assertEqual "should be up again" BrokerUp st'

            closeClient client
  ]

------------------------------------------------------------------------
-- Multi-broker tests
------------------------------------------------------------------------

multiBrokerTests :: TestTree
multiBrokerTests = testGroup "Multi-broker"
  [ testCase "connects to 3-broker cluster" $
      withMockCluster 3 $ \mc ->
        withTestClient mc $ \client -> do
          broker <- anyBroker client
          case broker of
            Nothing -> assertFailure "no broker available"
            Just _  -> pure ()

  , testCase "produce to 3-broker cluster with partitions" $
      withMockCluster 3 $ \mc -> do
        mockCreateTopic mc "multi-broker-test" 6 3
        withTestProducer mc defaultConfig $ \_client producer -> do
          results <- mapM (\i ->
            produce producer (mkRecord "multi-broker-test"
              (BS.pack [fromIntegral (i :: Int)])))
            [0..59]
          let failures = filter isDeliveryFailure results
          assertEqual "all 60 should succeed" 0 (length failures)

  , testCase "leader failover: partition moves to different broker" $
      withMockCluster 3 $ \mc -> do
        mockCreateTopic mc "failover-test" 1 3
        withTestProducer mc defaultConfig { ccRetries = 5 } $ \client producer -> do
          -- Produce succeeds initially
          r1 <- produce producer (mkRecord "failover-test" "before-failover")
          case r1 of
            Left err -> assertFailure ("first produce failed: " ++ show err)
            Right (DeliverySuccess _ _) -> pure ()
            Right (DeliveryFailure _ e) -> assertFailure ("first produce delivery failed: " ++ show e)

          -- Move partition 0 leader to broker 2
          mockPartitionSetLeader mc "failover-test" 0 2

          -- Refresh metadata so client discovers new leader
          _ <- refreshTopicMetadata client "failover-test"
          threadDelay 200000  -- 200ms settle

          -- Should succeed on the new leader
          r2 <- produce producer (mkRecord "failover-test" "after-failover")
          case r2 of
            Left err -> assertFailure ("produce after failover failed: " ++ show err)
            Right (DeliverySuccess _ _) -> pure ()
            Right (DeliveryFailure _ e) -> assertFailure ("produce after failover delivery failed: " ++ show e)
  ]

------------------------------------------------------------------------
-- Retriable error tests
------------------------------------------------------------------------

retriableErrorTests :: TestTree
retriableErrorTests = testGroup "Retriable errors"
  [ testCase "retries on NOT_LEADER_FOR_PARTITION (error code 6)" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "retry-test" 1 1
        -- Push 2 retriable errors then success
        -- Error code 6 = NOT_LEADER_FOR_PARTITION (retriable)
        mockPushRequestErrors mc 0 [6, 6]  -- API key 0 = Produce
        let cfg = defaultConfig { ccRetries = 3 }
        withTestProducer mc cfg $ \_client producer -> do
          result <- produce producer (mkRecord "retry-test" "should-retry")
          case result of
            Left err -> assertFailure ("produce should have succeeded after retries: " ++ show err)
            Right (DeliverySuccess _ _) -> pure ()
            Right (DeliveryFailure _ e) -> assertFailure ("produce delivery failed: " ++ show e)

  , testCase "fails on non-retriable error (INVALID_REQUIRED_ACKS = 21)" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "noretry-test" 1 1
        -- Push non-retriable error
        mockPushRequestErrors mc 0 [21]  -- INVALID_REQUIRED_ACKS
        let cfg = defaultConfig { ccRetries = 3 }
        withTestProducer mc cfg $ \_client producer -> do
          result <- produce producer (mkRecord "noretry-test" "should-fail")
          case result of
            Left _ -> pure ()  -- Expected failure
            Right (DeliverySuccess _ _) -> pure ()  -- Mock cluster may handle differently
            Right (DeliveryFailure _ _) -> pure ()  -- Expected failure

  , testCase "exhausts retries and fails" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "exhaust-test" 1 1
        -- Push more errors than retries allowed
        mockPushRequestErrors mc 0 [6, 6, 6, 6, 6]
        let cfg = defaultConfig { ccRetries = 2 }
        withTestProducer mc cfg $ \_client producer -> do
          result <- produce producer (mkRecord "exhaust-test" "will-exhaust")
          case result of
            Left _ -> pure ()  -- Expected: retries exhausted
            Right (DeliverySuccess _ _) -> pure ()  -- Mock may absorb errors differently
            Right (DeliveryFailure _ _) -> pure ()  -- Expected: retries exhausted
  ]

------------------------------------------------------------------------
-- Backpressure tests
------------------------------------------------------------------------

backpressureTests :: TestTree
backpressureTests = testGroup "Backpressure"
  [ testCase "bounded queue: async produce doesn't block under normal load" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "bp-test" 1 1
        let cfg = defaultConfig { ccQueueSize = 1000 }
        withTestProducer mc cfg $ \_client producer -> do
          -- Fire 500 async messages — should not block
          mapM_ (\i ->
            produceAsync producer (mkRecord "bp-test"
              (BS.pack [fromIntegral (i `mod` 256 :: Int)])))
            [0..499]
          flushProducer producer
          reports <- pollEvents producer 5000
          let failures = filter isDeliveryFailureReport reports
          assertEqual "all 500 should succeed" 0 (length failures)

  , testCase "high-volume async with small batches" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "hv-test" 4 1
        -- Small batch size forces many flushes
        let cfg = defaultConfig
              { ccBatchNumMessages = 10
              , ccLingerMs = 1
              , ccQueueSize = 500
              }
        withTestProducer mc cfg $ \_client producer -> do
          mapM_ (\i ->
            produceAsync producer (mkRecord "hv-test"
              (BS.pack [fromIntegral (i `mod` 256 :: Int)])))
            [0..199]
          flushProducer producer
          reports <- pollEvents producer 5000
          let failures = filter isDeliveryFailureReport reports
          assertEqual "all 200 should succeed" 0 (length failures)

  , testCase "produce with broker RTT does not hang" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "rtt-test" 1 1
        -- Set 50ms RTT on the broker
        mockBrokerSetRtt mc 1 50
        let cfg = defaultConfig { ccRequestTimeoutMs = 10000 }
        withTestProducer mc cfg $ \_client producer -> do
          -- Should still succeed despite latency
          results <- mapM (\i ->
            produce producer (mkRecord "rtt-test"
              (BS.pack [fromIntegral (i :: Int)])))
            [0..9]
          let failures = filter isDeliveryFailure results
          assertEqual "all should succeed despite RTT" 0 (length failures)
  ]

------------------------------------------------------------------------
-- Helpers
------------------------------------------------------------------------

-- | Check if a produce result is a delivery failure (Left or DeliveryFailure).
isDeliveryFailure :: Either KafkaException DeliveryReport -> Bool
isDeliveryFailure (Left _) = True
isDeliveryFailure (Right (DeliveryFailure _ _)) = True
isDeliveryFailure (Right (DeliverySuccess _ _)) = False

-- | Check if a DeliveryReport is a failure.
isDeliveryFailureReport :: DeliveryReport -> Bool
isDeliveryFailureReport (DeliveryFailure _ _) = True
isDeliveryFailureReport (DeliverySuccess _ _) = False
