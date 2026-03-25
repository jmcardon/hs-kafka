{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.STM
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Int (Int32)
import Data.IORef
import Data.Primitive.ByteArray (ByteArray, byteArrayFromListN)
import Test.Tasty
import Test.Tasty.HUnit

import Kafka.Client
import Kafka.Common
import Kafka.Internal.Broker (BrokerEnv(..), BrokerState(..))
import Kafka.Internal.Config
import Kafka.Producer
import MockCluster

main :: IO ()
main = defaultMain $ testGroup "Integration"
  [ mockClusterTests
  , clientTests
  , producerTests
  , reconnectionTests
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

-- | Helper: ByteString to ByteArray for payloads
bsToBA :: ByteString -> ByteArray
bsToBA bs = byteArrayFromListN (BS.length bs) (BS.unpack bs)

producerTests :: TestTree
producerTests = testGroup "Producer"
  [ testCase "produce single message" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "produce-test" 1 1
        withTestClient mc $ \client -> do
          producer <- newProducer client defaultConfig
          result <- produce producer "produce-test" (bsToBA "hello kafka")
          case result of
            Left err -> assertFailure ("produce failed: " ++ show err)
            Right () -> pure ()
          closeProducer producer

  , testCase "produce 100 messages" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "bulk-test" 1 1
        withTestClient mc $ \client -> do
          producer <- newProducer client defaultConfig
          results <- mapM (\i ->
            produce producer "bulk-test"
              (bsToBA (BS.pack [fromIntegral (i :: Int)])))
            [0..99]
          let failures = filter isLeft results
          assertEqual "all should succeed" 0 (length failures)
          closeProducer producer

  , testCase "produce async + flush" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "async-test" 1 1
        withTestClient mc $ \client -> do
          let cfg = defaultConfig { ccLingerMs = 5000 } -- high linger
          producer <- newProducer client cfg
          -- Fire-and-forget 10 messages
          vars <- mapM (\i ->
            produceAsync producer "async-test"
              (bsToBA (BS.pack [fromIntegral (i :: Int)])))
            [0..9]
          -- Flush forces them out despite high linger
          flushProducer producer
          -- All should be delivered
          results <- mapM (atomically . readTMVar) vars
          let failures = filter isLeft results
          assertEqual "all should succeed after flush" 0 (length failures)
          closeProducer producer

  , testCase "produce to multiple partitions" $
      withMockCluster 1 $ \mc -> do
        mockCreateTopic mc "multi-part" 4 1
        withTestClient mc $ \client -> do
          producer <- newProducer client defaultConfig
          -- Produce 12 messages — should round-robin across 4 partitions
          results <- mapM (\i ->
            produce producer "multi-part"
              (bsToBA (BS.pack [fromIntegral (i :: Int)])))
            [0..11]
          let failures = filter isLeft results
          assertEqual "all should succeed" 0 (length failures)
          closeProducer producer
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
-- Helpers
------------------------------------------------------------------------

isLeft :: Either a b -> Bool
isLeft (Left _)  = True
isLeft (Right _) = False
