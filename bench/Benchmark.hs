{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE PackageImports #-}

-- | Comprehensive throughput benchmark: kafka-native vs hw-kafka-client.
--
-- Tests multiple patterns:
--   1. Async produce + flush (the standard pattern)
--   2. Sync produce (blocking per-message)
--   3. Produce with keys (murmur2 partitioning)
--   4. Produce with headers
--   5. Produce with compression (gzip, snappy, lz4, zstd)
--   6. Produce+poll (separate threads)
module Main (main) where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM (atomically, newEmptyTMVarIO, putTMVar, readTMVar)
import Control.DeepSeq (NFData(..))
import Control.Monad (forM_, void, replicateM_)
import Criterion.Main
import qualified Data.ByteString as BS
import qualified Data.Text as Text

-- kafka-native
import qualified "kafka" Kafka.Client as Native
import qualified "kafka" Kafka.Internal.Config as Native
import qualified "kafka" Kafka.Producer as Native
import "kafka" Kafka.Common (TopicName(..))

-- hw-kafka-client
import qualified "hw-kafka-client" Kafka.Producer as HW

-- mock cluster (shared FFI to librdkafka)
import MockCluster

------------------------------------------------------------------------
-- Helpers
------------------------------------------------------------------------

data NativeEnv = NativeEnv
  { neProducer :: !Native.KafkaProducer
  , neClient   :: !Native.KafkaClient
  , neCluster  :: !MockCluster
  }
instance NFData NativeEnv where rnf !_ = ()

data HwEnv = HwEnv
  { heProducer :: !HW.KafkaProducer
  , heCluster  :: !MockCluster
  }
instance NFData HwEnv where rnf !_ = ()

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
  Right producer <- Native.newProducer client (Native.defaultProducerConfig cfg)
  -- warm up
  _ <- Native.produce producer (Native.ProducerRecord "native-bench" Native.UnassignedPartition Nothing (Just "warmup") [])
  Native.flushProducer producer
  pure (NativeEnv producer client mc)

setupNativeCompressed :: Native.Compression -> IO NativeEnv
setupNativeCompressed codec = do
  mc <- createMockCluster 3
  mockCreateTopic mc "native-comp" 4 1
  let addrs = parseBootstraps (mcBootstraps mc)
      cfg = Native.defaultConfig
        { Native.ccBootstrap = addrs
        , Native.ccLingerMs = 1
        , Native.ccBatchNumMessages = 1000
        , Native.ccCompression = codec
        }
  Right client <- Native.newClient cfg
  Right producer <- Native.newProducer client (Native.defaultProducerConfig cfg)
  _ <- Native.produce producer (Native.ProducerRecord "native-comp" Native.UnassignedPartition Nothing (Just "warmup") [])
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
  let bootstraps = mcBootstraps mc
  let props = HW.brokersList [HW.BrokerAddress (Text.pack bootstraps)]
           <> HW.logLevel HW.KafkaLogErr
  Right producer <- HW.newProducer props
  pure (HwEnv producer mc)

teardownHw :: HwEnv -> IO ()
teardownHw env = do
  HW.closeProducer (heProducer env)
  destroyMockCluster (heCluster env)

------------------------------------------------------------------------
-- Benchmark functions
------------------------------------------------------------------------

-- | Async produce N messages + flush (standard pattern)
nativeAsyncFlush :: Native.KafkaProducer -> TopicName -> Int -> BS.ByteString -> IO ()
nativeAsyncFlush producer topic n payload = do
  let record = Native.ProducerRecord topic Native.UnassignedPartition
                 Nothing (Just payload) []
  replicateM_ n $ Native.produceAsync producer record
  Native.flushProducer producer

-- | Async produce with keys (murmur2 partitioning)
nativeAsyncWithKeys :: Native.KafkaProducer -> Int -> BS.ByteString -> IO ()
nativeAsyncWithKeys producer n payload = do
  forM_ [0..n-1] $ \i -> do
    let key = BS.pack [fromIntegral (i `mod` 256)]
        record = Native.ProducerRecord "native-bench" Native.UnassignedPartition
                   (Just key) (Just payload) []
    void $ Native.produceAsync producer record
  Native.flushProducer producer

-- | Async produce with headers
nativeAsyncWithHeaders :: Native.KafkaProducer -> Int -> BS.ByteString -> IO ()
nativeAsyncWithHeaders producer n payload = do
  let hdrs = [ Native.Header "trace-id" (Just "abc123")
             , Native.Header "content-type" (Just "application/octet-stream")
             ]
  forM_ [0..n-1] $ \_ -> do
    let record = Native.ProducerRecord "native-bench" Native.UnassignedPartition
                   Nothing (Just payload) hdrs
    void $ Native.produceAsync producer record
  Native.flushProducer producer

-- | Sync produce (blocking per-message)
nativeSync :: Native.KafkaProducer -> Int -> BS.ByteString -> IO ()
nativeSync producer n payload = do
  let record = Native.ProducerRecord "native-bench" Native.UnassignedPartition
                 Nothing (Just payload) []
  forM_ [0..n-1] $ \_ -> void $ Native.produce producer record

-- | Produce in one thread, poll in another
nativeProducePoll :: Native.KafkaProducer -> Int -> BS.ByteString -> IO ()
nativeProducePoll producer n payload = do
  done <- newEmptyTMVarIO
  -- Poller thread
  _ <- forkIO $ do
    let go !count
          | count >= n = atomically $ putTMVar done ()
          | otherwise = do
              reports <- Native.pollEvents producer 100
              go (count + length reports)
    go 0
  -- Producer thread
  let record = Native.ProducerRecord "native-bench" Native.UnassignedPartition
                 Nothing (Just payload) []
  replicateM_ n $ Native.produceAsync producer record
  Native.flushProducer producer
  atomically $ readTMVar done

-- | hw-kafka-client produce
hwProduce :: HW.KafkaProducer -> Int -> BS.ByteString -> IO ()
hwProduce producer n payload = do
  let record = HW.ProducerRecord
        { HW.prTopic = "hw-bench"
        , HW.prPartition = HW.UnassignedPartition
        , HW.prKey = Nothing
        , HW.prValue = Just payload
        , HW.prHeaders = mempty
        }
  replicateM_ n $ HW.produceMessage producer record
  HW.flushProducer producer

------------------------------------------------------------------------
-- Main
------------------------------------------------------------------------

main :: IO ()
main = defaultMain
  [ bgroup "kafka-native"
    [ envWithCleanup setupNative teardownNative $ \ ~env -> bgroup "async+flush"
        [ bench "1K × 100B"  $ nfIO $ nativeAsyncFlush (neProducer env) "native-bench" 1000 (BS.replicate 100 0x41)
        , bench "10K × 100B" $ nfIO $ nativeAsyncFlush (neProducer env) "native-bench" 10000 (BS.replicate 100 0x41)
        , bench "1K × 1KB"   $ nfIO $ nativeAsyncFlush (neProducer env) "native-bench" 1000 (BS.replicate 1000 0x41)
        , bench "1K × 10KB"  $ nfIO $ nativeAsyncFlush (neProducer env) "native-bench" 1000 (BS.replicate 10000 0x41)
        ]
    , envWithCleanup setupNative teardownNative $ \ ~env -> bgroup "with-keys"
        [ bench "1K × 100B" $ nfIO $ nativeAsyncWithKeys (neProducer env) 1000 (BS.replicate 100 0x41)
        ]
    , envWithCleanup setupNative teardownNative $ \ ~env -> bgroup "with-headers"
        [ bench "1K × 100B" $ nfIO $ nativeAsyncWithHeaders (neProducer env) 1000 (BS.replicate 100 0x41)
        ]
    , envWithCleanup setupNative teardownNative $ \ ~env -> bgroup "sync"
        [ bench "100 × 100B" $ nfIO $ nativeSync (neProducer env) 100 (BS.replicate 100 0x41)
        ]
    , envWithCleanup setupNative teardownNative $ \ ~env -> bgroup "produce+poll"
        [ bench "1K × 100B" $ nfIO $ nativeProducePoll (neProducer env) 1000 (BS.replicate 100 0x41)
        ]
    , envWithCleanup (setupNativeCompressed Native.Gzip) teardownNative $ \ ~env -> bgroup "compression"
        [ bench "gzip 1K × 100B" $ nfIO $ nativeAsyncFlush (neProducer env) "native-comp" 1000 (BS.replicate 100 0x41)
        ]
    , envWithCleanup (setupNativeCompressed Native.Snappy) teardownNative $ \ ~env ->
        bench "snappy 1K × 100B" $ nfIO $ nativeAsyncFlush (neProducer env) "native-comp" 1000 (BS.replicate 100 0x41)
    , envWithCleanup (setupNativeCompressed Native.Lz4) teardownNative $ \ ~env ->
        bench "lz4 1K × 100B" $ nfIO $ nativeAsyncFlush (neProducer env) "native-comp" 1000 (BS.replicate 100 0x41)
    , envWithCleanup (setupNativeCompressed Native.Zstd) teardownNative $ \ ~env ->
        bench "zstd 1K × 100B" $ nfIO $ nativeAsyncFlush (neProducer env) "native-comp" 1000 (BS.replicate 100 0x41)
    ]
  , bgroup "hw-kafka-client"
    [ envWithCleanup setupHw teardownHw $ \ ~env -> bgroup "async+flush"
        [ bench "1K × 100B"  $ nfIO $ hwProduce (heProducer env) 1000 (BS.replicate 100 0x42)
        , bench "10K × 100B" $ nfIO $ hwProduce (heProducer env) 10000 (BS.replicate 100 0x42)
        , bench "1K × 1KB"   $ nfIO $ hwProduce (heProducer env) 1000 (BS.replicate 1000 0x42)
        , bench "1K × 10KB"  $ nfIO $ hwProduce (heProducer env) 1000 (BS.replicate 10000 0x42)
        ]
    ]
  ]
