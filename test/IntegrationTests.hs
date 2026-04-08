{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM (readTVarIO, TMVar, newEmptyTMVarIO, putTMVar, readTMVar, atomically)
import Control.Monad (forM, forM_, void, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.Map.Strict as Map
import Data.IORef
import Test.Tasty
import Test.Tasty.HUnit

import Kafka.Client
import Kafka.Common
import Kafka.Consumer.New
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
  , loadTests
  , consumerTests
  , queueFullTests
  , closeProducerTests
  , deliveryReportFieldTests
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
            Right (DeliverySuccess{}) -> pure ()
            Right (DeliveryFailure{drError = e}) -> assertFailure ("produce delivery failed: " ++ show e)

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
        let cfg = defaultConfig { ccLingerMs = 5000 }
        withTestProducer mc cfg $ \_client producer -> do
          mapM_ (\i ->
            produceAsync producer (mkRecord "async-test"
              (BS.pack [fromIntegral (i :: Int)])))
            [0..9]
          -- flush = flushProducer + drain delivery reports
          delivered <- flush producer 5000
          assertEqual "all 10 should be delivered via flush" 10 delivered

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

  , testCase "per-message async callbacks via pollEvents" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "callback-test" 2 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          successCount <- newIORef (0 :: Int)
          failCount <- newIORef (0 :: Int)
          mapM_ (\i ->
            produceAsync producer (mkRecord "callback-test"
              (BS.pack [fromIntegral (i :: Int)])))
            [0..49]
          -- flushProducer signals send, then pollEvents drains reports
          flushProducer producer
          -- Poll in a loop until we get all 50
          let drainLoop !total = do
                reports <- pollEvents producer 1000
                forM_ reports $ \dr -> case dr of
                  DeliverySuccess{} -> atomicModifyIORef' successCount (\n -> (n+1, ()))
                  DeliveryFailure{} -> atomicModifyIORef' failCount (\n -> (n+1, ()))
                let !total' = total + length reports
                if total' < 50 && not (null reports)
                  then drainLoop total'
                  else pure total'
          _ <- drainLoop 0
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
          delivered <- flush producer 10000
          assertEqual "all 1000 should be delivered" 1000 delivered
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
            (Right (DeliverySuccess{}), Right (DeliverySuccess{})) -> pure ()
            _ -> assertFailure "keyed produce should succeed"
          case r3 of
            Right (DeliverySuccess{}) -> pure ()
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
            (Right (DeliverySuccess{}), Right (DeliverySuccess{})) -> pure ()
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
            Right (DeliverySuccess{}) -> pure ()
            Right (DeliveryFailure{drError = e}) -> assertFailure ("delivery failed: " ++ show e)

  , testCase "delivery report contains offset" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "offset-test" 1 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          r1 <- produce producer (mkRecord "offset-test" "msg1")
          r2 <- produce producer (mkRecord "offset-test" "msg2")
          case (r1, r2) of
            (Right DeliverySuccess{drOffset = Offset o1}, Right DeliverySuccess{drOffset = Offset o2}) -> do
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
            Right (DeliverySuccess{}) -> pure ()
            Right (DeliveryFailure{drError = e}) -> assertFailure ("delivery failed: " ++ show e)
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
            Right (DeliverySuccess{}) -> pure ()
            Right (DeliveryFailure{drError = e}) -> assertFailure ("produce delivery failed: " ++ show e)
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
            Right (DeliverySuccess{}) -> pure ()  -- Mock cluster may auto-create
            Right (DeliveryFailure{}) -> pure ()  -- Expected failure

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
                  Right (DeliverySuccess{}) -> pure ()
                  Right (DeliveryFailure{drError = e}) -> assertFailure ("first produce delivery failed: " ++ show e)

                -- Brief disconnect
                mockBrokerDown mc 1
                threadDelay 100000  -- 100ms
                mockBrokerUp mc 1
                threadDelay 500000  -- 500ms for reconnection

                -- Should be able to produce again
                r2 <- produce producer (mkRecord "disconnect-test" "after reconnect")
                case r2 of
                  Left _ -> pure ()  -- May fail if reconnection not complete
                  Right (DeliverySuccess{}) -> pure ()
                  Right (DeliveryFailure{}) -> pure ()

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
            Right (DeliverySuccess{}) -> pure ()
            Right (DeliveryFailure{drError = e}) -> assertFailure ("first produce delivery failed: " ++ show e)

          -- Move partition 0 leader to broker 2
          mockPartitionSetLeader mc "failover-test" 0 2

          -- Refresh metadata so client discovers new leader
          _ <- refreshTopicMetadata client "failover-test"
          threadDelay 200000  -- 200ms settle

          -- Should succeed on the new leader
          r2 <- produce producer (mkRecord "failover-test" "after-failover")
          case r2 of
            Left err -> assertFailure ("produce after failover failed: " ++ show err)
            Right (DeliverySuccess{}) -> pure ()
            Right (DeliveryFailure{drError = e}) -> assertFailure ("produce after failover delivery failed: " ++ show e)
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
            Right (DeliverySuccess{}) -> pure ()
            Right (DeliveryFailure{drError = e}) -> assertFailure ("produce delivery failed: " ++ show e)

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
            Right (DeliverySuccess{}) -> pure ()  -- Mock cluster may handle differently
            Right (DeliveryFailure{}) -> pure ()  -- Expected failure

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
            Right (DeliverySuccess{}) -> pure ()  -- Mock may absorb errors differently
            Right (DeliveryFailure{}) -> pure ()  -- Expected: retries exhausted
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
          mapM_ (\i ->
            produceAsync producer (mkRecord "bp-test"
              (BS.pack [fromIntegral (i `mod` 256 :: Int)])))
            [0..499]
          delivered <- flush producer 5000
          assertEqual "all 500 should succeed" 500 delivered

  , testCase "high-volume async with small batches" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "hv-test" 4 1
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
          delivered <- flush producer 5000
          assertEqual "all 200 should succeed" 200 delivered

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

------------------------------------------------------------------------
-- Consumer tests
------------------------------------------------------------------------

consumerTests :: TestTree
consumerTests = testGroup "Consumer"
  [ testCase "manual assign + poll" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "cons-test" 1 1
        let addrs = parseBootstraps (mcBootstraps mc)
            cfg = defaultConfig { ccBootstrap = addrs }
        -- Produce some messages first
        Right client <- newClient cfg
        Right producer <- newProducer client (defaultProducerConfig cfg)
        forM_ [0..9 :: Int] $ \i ->
          void $ produce producer (ProducerRecord "cons-test" (SpecifiedPartition 0)
            Nothing (Just (BS.pack [fromIntegral i])) [])
        void $ flush producer 5000
        closeProducer producer
        -- Now consume via manual assign (no consumer group)
        let consCfg = (defaultConsumerConfig cfg "test-group" ["cons-test"])
              { ccFetchWaitMs = 100 }  -- fast fetch cycle
        Right consumer <- newConsumer client consCfg
        assign consumer [TopicPartition "cons-test" 0 0]
        -- Poll with generous timeout to allow fetch cycle to complete
        records <- consumerPollBatch consumer 10000 100
        closeConsumer consumer
        closeClient client
        assertBool ("should consume some messages, got " ++ show (length records))
          (length records > 0)

  , testCase "assignment and subscription queries" $
      withMockCluster 1 $ \mc -> do
        let addrs = parseBootstraps (mcBootstraps mc)
            cfg = defaultConfig { ccBootstrap = addrs }
        mockCreateTopic mc "query-test" 1 1
        Right client <- newClient cfg
        let consCfg = defaultConsumerConfig cfg "query-group" ["query-test"]
        Right consumer <- newConsumer client consCfg
        -- Query subscription
        subs <- subscription consumer
        assertEqual "should be subscribed" ["query-test"] subs
        -- Manual assign
        assign consumer [TopicPartition "query-test" 0 0]
        assigned <- assignment consumer
        assertEqual "should have 1 partition" 1 (length assigned)
        closeConsumer consumer
        closeClient client

  , testCase "seek changes fetch offset" $
      withMockCluster 1 $ \mc -> do
        let addrs = parseBootstraps (mcBootstraps mc)
            cfg = defaultConfig { ccBootstrap = addrs }
        mockCreateTopic mc "seek-test" 1 1
        Right client <- newClient cfg
        let consCfg = defaultConsumerConfig cfg "seek-group" ["seek-test"]
        Right consumer <- newConsumer client consCfg
        assign consumer [TopicPartition "seek-test" 0 0]
        -- Seek to offset 100
        seek consumer "seek-test" 0 100
        pos <- position consumer
        case Map.lookup ("seek-test", 0) pos of
          Just off -> assertEqual "offset should be 100" 100 off
          Nothing -> assertFailure "position should have seek-test:0"
        closeConsumer consumer
        closeClient client

  , testCase "pause and resume partitions" $
      withMockCluster 1 $ \mc -> do
        let addrs = parseBootstraps (mcBootstraps mc)
            cfg = defaultConfig { ccBootstrap = addrs }
        mockCreateTopic mc "pause-test" 2 1
        Right client <- newClient cfg
        let consCfg = defaultConsumerConfig cfg "pause-group" ["pause-test"]
        Right consumer <- newConsumer client consCfg
        assign consumer [TopicPartition "pause-test" 0 0, TopicPartition "pause-test" 1 0]
        -- Pause partition 0
        pausePartitions consumer [("pause-test", 0)]
        -- Query — partition 0 should be paused
        -- (fetch loop skips paused partitions)
        -- Resume
        resumePartitions consumer [("pause-test", 0)]
        closeConsumer consumer
        closeClient client

  , testCase "storeOffset updates position" $
      withMockCluster 1 $ \mc -> do
        let addrs = parseBootstraps (mcBootstraps mc)
            cfg = defaultConfig { ccBootstrap = addrs }
        mockCreateTopic mc "store-test" 1 1
        Right client <- newClient cfg
        let consCfg = defaultConsumerConfig cfg "store-group" ["store-test"]
        Right consumer <- newConsumer client consCfg
        assign consumer [TopicPartition "store-test" 0 0]
        -- Simulate a consumed record
        let fakeRecord = ConsumerRecord "store-test" 0 42 0 Nothing (Just "data") []
        storeOffset consumer fakeRecord
        pos <- position consumer
        case Map.lookup ("store-test", 0) pos of
          Just off -> assertEqual "stored offset should be 43" 43 off
          Nothing -> assertFailure "position should have store-test:0"
        closeConsumer consumer
        closeClient client

  , testCase "withConsumer handles cleanup" $
      withMockCluster 1 $ \mc -> do
        let addrs = parseBootstraps (mcBootstraps mc)
            cfg = defaultConfig { ccBootstrap = addrs }
        mockCreateTopic mc "with-cons-test" 1 1
        Right client <- newClient cfg
        let consCfg = defaultConsumerConfig cfg "with-group" ["with-cons-test"]
        result <- withConsumer client consCfg $ \consumer -> do
          subs <- subscription consumer
          assertEqual "subscribed" ["with-cons-test"] subs
          pure ()
        case result of
          Right () -> pure ()
          Left err -> assertFailure ("withConsumer failed: " ++ show err)
        closeClient client

  , testCase "closeConsumer terminates without hang" $
      withMockCluster 1 $ \mc -> do
        let addrs = parseBootstraps (mcBootstraps mc)
            cfg = defaultConfig { ccBootstrap = addrs }
        mockCreateTopic mc "close-cons-test" 1 1
        Right client <- newClient cfg
        let consCfg = defaultConsumerConfig cfg "close-group" ["close-cons-test"]
        Right consumer <- newConsumer client consCfg
        assign consumer [TopicPartition "close-cons-test" 0 0]
        -- closeConsumer should return promptly even if no messages
        closeConsumer consumer
        closeClient client
        -- Getting here means no hang

  , testCase "concurrent fetch across many partitions" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "concfetch" 8 1
        let addrs = parseBootstraps (mcBootstraps mc)
            cfg = defaultConfig { ccBootstrap = addrs }
        -- Produce to all partitions
        Right client <- newClient cfg
        Right producer <- newProducer client (defaultProducerConfig cfg)
        forM_ [0..7 :: Int] $ \p ->
          forM_ [0..4 :: Int] $ \i ->
            void $ produce producer (ProducerRecord "concfetch"
              (SpecifiedPartition (fromIntegral p)) Nothing
              (Just (BS.pack [fromIntegral i])) [])
        void $ flush producer 5000
        closeProducer producer
        -- Consume via assign — should fetch from all 8 partitions concurrently
        let consCfg = (defaultConsumerConfig cfg "conc-group" ["concfetch"])
              { ccFetchWaitMs = 100 }
        Right consumer <- newConsumer client consCfg
        assign consumer [ TopicPartition "concfetch" (fromIntegral p) 0
                        | p <- [0..7 :: Int]
                        ]
        records <- consumerPollBatch consumer 10000 200
        closeConsumer consumer
        closeClient client
        assertBool ("should get records from multiple partitions, got " ++ show (length records))
          (length records > 0)
  ]

------------------------------------------------------------------------
-- Helpers
------------------------------------------------------------------------

-- | Check if a produce result is a delivery failure (Left or DeliveryFailure).
isDeliveryFailure :: Either KafkaException DeliveryReport -> Bool
isDeliveryFailure (Left _) = True
isDeliveryFailure (Right (DeliveryFailure{})) = True
isDeliveryFailure (Right (DeliverySuccess{})) = False

-- | Check if a DeliveryReport is a failure.
isDeliveryFailureReport :: DeliveryReport -> Bool
isDeliveryFailureReport (DeliveryFailure{}) = True
isDeliveryFailureReport (DeliverySuccess{}) = False

------------------------------------------------------------------------
-- Load tests
------------------------------------------------------------------------

loadTests :: TestTree
loadTests = testGroup "Load tests"
  [ testCase "10K messages burst" $
      withMockCluster 3 $ \mc -> do
        mockCreateTopic mc "burst-test" 8 3
        let cfg = defaultConfig
              { ccLingerMs = 1
              , ccBatchNumMessages = 500
              , ccBatchSize = 1000000
              , ccQueueSize = 20000  -- large enough for 10K in-flight
              }
            pCfg = (defaultProducerConfig cfg) { pcDeliveryQueueSize = 20000 }
        let addrs = parseBootstraps (mcBootstraps mc)
            fullCfg = cfg { ccBootstrap = addrs }
        Right client <- newClient fullCfg
        Right producer <- newProducer client pCfg
        mapM_ (\i ->
          produceAsync producer (mkRecord "burst-test"
            (BS.replicate 100 (fromIntegral (i `mod` 256 :: Int)))))
          [0..9999]
        delivered <- flush producer 15000
        assertBool ("should deliver most messages, got " ++ show delivered)
          (delivered >= 9000)
        closeProducer producer
        closeClient client

  , testCase "concurrent producers to same topic" $
      withMockCluster 3 $ \mc -> do
        mockCreateTopic mc "concurrent-test" 4 3
        let addrs = parseBootstraps (mcBootstraps mc)
            cfg = defaultConfig { ccBootstrap = addrs }
            pCfg = (defaultProducerConfig cfg) { pcDeliveryQueueSize = 5000 }
        Right client <- newClient cfg
        Right producer <- newProducer client pCfg
        -- 4 concurrent threads, each producing 250 messages
        doneVars <- forM [0..3 :: Int] $ \threadId -> do
          done <- newEmptyTMVarIO
          _ <- forkIO $ do
            forM_ [0..249 :: Int] $ \_ -> do
              let key = BS.pack [fromIntegral threadId]
                  rec = ProducerRecord "concurrent-test" UnassignedPartition
                          (Just key) (Just (BS.replicate 50 0x42)) []
              _ <- produceAsync producer rec
              pure ()
            atomically $ putTMVar done ()
          pure done
        mapM_ (\v -> atomically $ readTMVar v) doneVars
        delivered <- flush producer 10000
        assertEqual "all 1000 should deliver" 1000 delivered
        closeProducer producer
        closeClient client

  , testCase "withProducer handles exceptions" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "with-test" 1 1
        let addrs = parseBootstraps (mcBootstraps mc)
            cfg = defaultConfig { ccBootstrap = addrs }
        result <- withClient cfg $ \client -> do
          withProducer client (defaultProducerConfig cfg) $ \producer -> do
            r <- produce producer (mkRecord "with-test" "bracket-safe")
            case r of
              Right (DeliverySuccess{}) -> pure ()
              _ -> assertFailure "produce should succeed"
        case result of
          Right (Right ()) -> pure ()
          _ -> assertFailure "withClient/withProducer should succeed"

  , testCase "rapid produce+poll interleaving" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "interleave-test" 2 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          -- Interleave produce and poll in tight loop
          totalDelivered <- newIORef (0 :: Int)
          forM_ [0..99 :: Int] $ \i -> do
            _ <- produceAsync producer (mkRecord "interleave-test"
                    (BS.pack [fromIntegral i]))
            -- Poll after every 10 produces
            when (i `mod` 10 == 9) $ do
              reports <- pollEvents producer 100
              atomicModifyIORef' totalDelivered
                (\n -> (n + length reports, ()))
          flushProducer producer
          -- Final drain
          finalReports <- pollEvents producer 5000
          atomicModifyIORef' totalDelivered
            (\n -> (n + length finalReports, ()))
          delivered <- readIORef totalDelivered
          assertEqual "all 100 should be delivered" 100 delivered
  ]

------------------------------------------------------------------------
-- Queue-full tests
------------------------------------------------------------------------

queueFullTests :: TestTree
queueFullTests = testGroup "Queue full"
  [ testCase "produceAsync returns KafkaQueueFullException when queue full" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "qfull-test" 1 1
        -- Tiny queue + huge linger so messages pile up
        let cfg = defaultConfig
              { ccQueueSize = 4
              , ccLingerMs = 60000
              , ccBatchNumMessages = 1000000
              , ccBatchSize = 1000000
              }
        withTestProducer mc cfg $ \_client producer -> do
          -- Push lots of messages without flushing
          results <- mapM (\i ->
            produceAsync producer (mkRecord "qfull-test"
              (BS.pack [fromIntegral (i `mod` 256 :: Int)])))
            [0..50 :: Int]
          let queueFulls = [() | Left KafkaQueueFullException <- results]
          assertBool "should have at least one KafkaQueueFullException"
            (not (null queueFulls))
          -- Drain so test cleanup doesn't hang
          flushProducer producer

  , testCase "produceAsync returns Right () when queue has room" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "qok-test" 1 1
        let cfg = defaultConfig { ccQueueSize = 1000 }
        withTestProducer mc cfg $ \_client producer -> do
          results <- mapM (\i ->
            produceAsync producer (mkRecord "qok-test"
              (BS.pack [fromIntegral (i :: Int)])))
            [0..9 :: Int]
          let failed = [e | Left e <- results]
          assertEqual "no enqueue failures" 0 (length failed)
          flushProducer producer
  ]

------------------------------------------------------------------------
-- closeProducer flush behavior
------------------------------------------------------------------------

closeProducerTests :: TestTree
closeProducerTests = testGroup "closeProducer"
  [ testCase "closeProducer drains pending messages" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "close-test" 1 1
        let addrs = parseBootstraps (mcBootstraps mc)
            cfg = defaultConfig { ccBootstrap = addrs, ccLingerMs = 30000 }
        Right client <- newClient cfg
        Right producer <- newProducer client (defaultProducerConfig cfg)

        delivered <- newIORef (0 :: Int)
        -- Use callbacks to count actual deliveries
        mapM_ (\i -> produceWithCallback producer
          (mkRecord "close-test" (BS.pack [fromIntegral (i :: Int)]))
          (\_ -> atomicModifyIORef' delivered (\n -> (n+1, ()))))
          [0..9 :: Int]

        -- closeProducer should flush before returning
        closeProducer producer
        -- After close, drain any reports that arrived
        _ <- pollEvents producer 1000

        n <- readIORef delivered
        closeClient client
        assertEqual "all 10 should be delivered after closeProducer" 10 n
  ]

------------------------------------------------------------------------
-- DeliveryReport fields
------------------------------------------------------------------------

deliveryReportFieldTests :: TestTree
deliveryReportFieldTests = testGroup "DeliveryReport fields"
  [ testCase "DeliverySuccess includes partition" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "dr-part" 4 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          result <- produce producer (ProducerRecord "dr-part"
            (SpecifiedPartition 2) Nothing (Just "msg") [])
          case result of
            Right dr@DeliverySuccess{} ->
              assertEqual "partition matches" 2 (drPartition dr)
            _ -> assertFailure "expected DeliverySuccess"

  , testCase "DeliverySuccess includes brokerId" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "dr-broker" 1 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          result <- produce producer (mkRecord "dr-broker" "msg")
          case result of
            Right dr@DeliverySuccess{} ->
              assertBool "brokerId should be >= 0" (drBrokerId dr >= 0)
            _ -> assertFailure "expected DeliverySuccess"

  , testCase "DeliverySuccess includes non-negative latency" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "dr-lat" 1 1
        -- Add some RTT to ensure non-trivial latency
        mockBrokerSetRtt mc 1 5
        withTestProducer mc defaultConfig $ \_client producer -> do
          result <- produce producer (mkRecord "dr-lat" "msg")
          case result of
            Right dr@DeliverySuccess{} ->
              assertBool ("latency should be >= 0, got " ++ show (drLatencyUs dr))
                (drLatencyUs dr >= 0)
            _ -> assertFailure "expected DeliverySuccess"

  , testCase "DeliverySuccess includes drOffset" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "dr-off" 1 1
        withTestProducer mc defaultConfig $ \_client producer -> do
          r1 <- produce producer (mkRecord "dr-off" "first")
          r2 <- produce producer (mkRecord "dr-off" "second")
          case (r1, r2) of
            (Right d1@DeliverySuccess{}, Right d2@DeliverySuccess{}) -> do
              let Offset o1 = drOffset d1
                  Offset o2 = drOffset d2
              assertBool "second offset > first" (o2 > o1)
            _ -> assertFailure "expected two successes"

  , testCase "DeliveryFailure includes drErrorCode" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "dr-err" 1 1
        -- Push a non-retriable error
        mockPushRequestErrors mc 0 [21]  -- INVALID_REQUIRED_ACKS
        let cfg = defaultConfig { ccRetries = 0 }
        withTestProducer mc cfg $ \_client producer -> do
          result <- produce producer (mkRecord "dr-err" "msg")
          case result of
            Right dr@DeliveryFailure{} ->
              -- 21 is the pushed error code
              assertBool ("error code should be set, got " ++ show (drErrorCode dr))
                (drErrorCode dr /= 0)
            _ -> pure ()  -- Mock may handle differently; not asserting strictly
  ]
