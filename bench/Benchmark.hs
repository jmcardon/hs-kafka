{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PackageImports #-}
{-# LANGUAGE DeriveGeneric #-}

-- | Throughput benchmark: kafka-native vs hw-kafka-client.
-- Both produce to a librdkafka mock cluster via criterion.
module Main (main) where

import Control.Concurrent.STM
import Control.DeepSeq (NFData(..))
import Criterion.Main
import qualified Data.ByteString as BS
import Data.Primitive.ByteArray (ByteArray, byteArrayFromListN)
import qualified Data.Text as Text

-- kafka-native
import qualified "kafka" Kafka.Client as Native
import qualified "kafka" Kafka.Common as Native
import qualified "kafka" Kafka.Internal.Config as Native
import qualified "kafka" Kafka.Producer as Native

-- hw-kafka-client
import qualified "hw-kafka-client" Kafka.Producer as HW
import qualified "hw-kafka-client" Kafka.Types as HW

-- mock cluster (shared FFI to librdkafka)
import MockCluster

------------------------------------------------------------------------
-- Helpers
------------------------------------------------------------------------

bsToBA :: BS.ByteString -> ByteArray
bsToBA bs = byteArrayFromListN (BS.length bs) (BS.unpack bs)

------------------------------------------------------------------------
-- Environment setup
------------------------------------------------------------------------

data NativeEnv = NativeEnv
  { neProducer :: !Native.KafkaProducer
  , neClient   :: !Native.KafkaClient
  , neCluster  :: !MockCluster
  }

-- Opaque handles — NFData is trivial (they're pointers/TVars, fully evaluated)
instance NFData NativeEnv where rnf _ = ()

data HwEnv = HwEnv
  { heProducer :: !HW.KafkaProducer
  , heCluster  :: !MockCluster
  }

instance NFData HwEnv where rnf _ = ()

setupNative :: IO NativeEnv
setupNative = do
  mc <- createMockCluster 3
  mockCreateTopic mc "native-bench" 4 1
  let addrs = parseBootstraps (mcBootstraps mc)
      cfg = Native.defaultConfig
        { Native.ccBootstrap = addrs
        , Native.ccLingerMs = 1
        , Native.ccBatchNumMessages = 1000
        , Native.ccBatchSize = 1000000
        , Native.ccAcks = Native.AcksAll
        }
  Right client <- Native.newClient cfg
  Right producer <- Native.newProducer client cfg
  -- warm up
  _ <- Native.produce producer "native-bench" (bsToBA "warmup")
  Native.flushProducer producer
  pure (NativeEnv producer client mc)

teardownNative :: NativeEnv -> IO ()
teardownNative env = do
  Native.closeProducer (neProducer env)
  Native.closeClient (neClient env)
  destroyMockCluster (neCluster env)

setupHw :: IO HwEnv
setupHw = do
  mc <- createMockCluster 3
  mockCreateTopic mc "hw-bench" 4 1
  let bootstrapStr = mcBootstraps mc
      props = HW.brokersList [HW.BrokerAddress (Text.pack bootstrapStr)]
           <> HW.extraProp "linger.ms" "1"
           <> HW.extraProp "batch.num.messages" "1000"
           <> HW.extraProp "queue.buffering.max.messages" "200000"
  Right producer <- HW.newProducer props
  -- warm up
  _ <- HW.produceMessage producer (mkHwRecord "hw-bench" 100)
  HW.flushProducer producer
  pure (HwEnv producer mc)

teardownHw :: HwEnv -> IO ()
teardownHw env = do
  HW.closeProducer (heProducer env)
  destroyMockCluster (heCluster env)

mkHwRecord :: Text.Text -> Int -> HW.ProducerRecord
mkHwRecord topic size = HW.ProducerRecord
  { HW.prTopic = HW.TopicName topic
  , HW.prPartition = HW.UnassignedPartition
  , HW.prKey = Nothing
  , HW.prValue = Just (BS.replicate size 0x42)
  , HW.prHeaders = mempty
  }

------------------------------------------------------------------------
-- Benchmark actions
------------------------------------------------------------------------

-- | Produce N messages with kafka-native, flush, wait for all callbacks.
nativeProduce :: Native.KafkaProducer -> Int -> Int -> IO ()
nativeProduce producer n size = do
  let payload = bsToBA (BS.replicate size 0x41)
  vars <- mapM (\_ -> Native.produceAsync producer "native-bench" payload) [1..n]
  Native.flushProducer producer
  mapM_ (\v -> atomically $ readTMVar v) vars

-- | Produce N messages with hw-kafka-client, flush.
hwProduce :: HW.KafkaProducer -> Int -> Int -> IO ()
hwProduce producer n size = do
  let record = HW.ProducerRecord
        { HW.prTopic = "hw-bench"
        , HW.prPartition = HW.UnassignedPartition
        , HW.prKey = Nothing
        , HW.prValue = Just (BS.replicate size 0x42)
        , HW.prHeaders = mempty
        }
  mapM_ (\_ -> HW.produceMessage producer record) [1..n]
  HW.flushProducer producer

------------------------------------------------------------------------
-- Main
------------------------------------------------------------------------

main :: IO ()
main = defaultMain
  [ bgroup "kafka-native"
      [ envWithCleanup setupNative teardownNative $ \ ~env ->
          bgroup "produce+flush+callback"
            [ bench "1000 × 100B"  $ whnfIO (nativeProduce (neProducer env) 1000 100)
            , bench "10000 × 100B" $ whnfIO (nativeProduce (neProducer env) 10000 100)
            , bench "1000 × 1KB"   $ whnfIO (nativeProduce (neProducer env) 1000 1024)
            , bench "1000 × 10KB"  $ whnfIO (nativeProduce (neProducer env) 1000 10240)
            ]
      ]
  , bgroup "hw-kafka-client"
      [ envWithCleanup setupHw teardownHw $ \ ~env ->
          bgroup "produce+flush"
            [ bench "1000 × 100B"  $ whnfIO (hwProduce (heProducer env) 1000 100)
            , bench "10000 × 100B" $ whnfIO (hwProduce (heProducer env) 10000 100)
            , bench "1000 × 1KB"   $ whnfIO (hwProduce (heProducer env) 1000 1024)
            , bench "1000 × 10KB"  $ whnfIO (hwProduce (heProducer env) 1000 10240)
            ]
      ]
  ]
