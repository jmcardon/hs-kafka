{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE PackageImports #-}

-- | Apples-to-apples throughput benchmark: kafka-native vs hw-kafka-client.
--
-- Both sides are configured identically:
--   - Same acks, linger.ms, batch.num.messages, batch.size
--   - Same mock cluster (3 brokers, 4 partitions, replication 1)
--   - Same payload sizes, same message counts
--   - Warmup message on both sides before benchmarking
--
-- Two config profiles:
--   "low-latency":  linger.ms=1,  batch.num.messages=1000,  acks=1
--   "throughput":   linger.ms=5,  batch.num.messages=10000, acks=1
module Main (main) where

import Control.Monad (void, replicateM_)
import Control.DeepSeq (NFData(..))
import Criterion.Main
import qualified Data.ByteString as BS
import qualified Data.Map.Strict as M
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
-- Environment types
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

------------------------------------------------------------------------
-- Setup/teardown: low-latency profile
-- linger.ms=1, batch.num.messages=1000, batch.size=1MB, acks=1
------------------------------------------------------------------------

setupNativeLowLat :: IO NativeEnv
setupNativeLowLat = do
  mc <- createMockCluster 3
  mockCreateTopic mc "native-bench" 4 1
  let addrs = parseBootstraps (mcBootstraps mc)
      cfg = Native.defaultConfig
        { Native.ccBootstrap        = addrs
        , Native.ccLingerMs          = 1
        , Native.ccBatchNumMessages  = 1000
        , Native.ccBatchSize         = 1000000
        , Native.ccAcks              = Native.AcksLeader
        }
  Right client <- Native.newClient cfg
  Right producer <- Native.newProducer client (Native.defaultProducerConfig cfg)
  _ <- Native.produce producer (Native.ProducerRecord "native-bench" Native.UnassignedPartition Nothing (Just "warmup") [])
  Native.flushProducer producer
  pure (NativeEnv producer client mc)

setupHwLowLat :: IO HwEnv
setupHwLowLat = do
  mc <- createMockCluster 3
  mockCreateTopic mc "hw-bench" 4 1
  let props = HW.brokersList [HW.BrokerAddress (Text.pack (mcBootstraps mc))]
           <> HW.logLevel HW.KafkaLogErr
           <> HW.extraProps (M.fromList
                [ ("linger.ms",           "1")
                , ("batch.num.messages",  "1000")
                , ("batch.size",          "1000000")
                , ("acks",                "1")
                ])
  Right producer <- HW.newProducer props
  _ <- HW.produceMessage producer (hwRecord "hw-bench" Nothing (Just "warmup"))
  HW.flushProducer producer
  pure (HwEnv producer mc)

------------------------------------------------------------------------
-- Setup/teardown: throughput profile
-- linger.ms=5, batch.num.messages=10000, batch.size=1MB, acks=1
------------------------------------------------------------------------

setupNativeThroughput :: IO NativeEnv
setupNativeThroughput = do
  mc <- createMockCluster 3
  mockCreateTopic mc "native-bench" 4 1
  let addrs = parseBootstraps (mcBootstraps mc)
      cfg = Native.defaultConfig
        { Native.ccBootstrap        = addrs
        , Native.ccLingerMs          = 5
        , Native.ccBatchNumMessages  = 10000
        , Native.ccBatchSize         = 1000000
        , Native.ccAcks              = Native.AcksLeader
        }
  Right client <- Native.newClient cfg
  Right producer <- Native.newProducer client (Native.defaultProducerConfig cfg)
  _ <- Native.produce producer (Native.ProducerRecord "native-bench" Native.UnassignedPartition Nothing (Just "warmup") [])
  Native.flushProducer producer
  pure (NativeEnv producer client mc)

setupHwThroughput :: IO HwEnv
setupHwThroughput = do
  mc <- createMockCluster 3
  mockCreateTopic mc "hw-bench" 4 1
  let props = HW.brokersList [HW.BrokerAddress (Text.pack (mcBootstraps mc))]
           <> HW.logLevel HW.KafkaLogErr
           <> HW.extraProps (M.fromList
                [ ("linger.ms",           "5")
                , ("batch.num.messages",  "10000")
                , ("batch.size",          "1000000")
                , ("acks",                "1")
                ])
  Right producer <- HW.newProducer props
  _ <- HW.produceMessage producer (hwRecord "hw-bench" Nothing (Just "warmup"))
  HW.flushProducer producer
  pure (HwEnv producer mc)

------------------------------------------------------------------------
-- Setup/teardown: compression variants (throughput base + codec)
------------------------------------------------------------------------

setupNativeCompressed :: Native.Compression -> IO NativeEnv
setupNativeCompressed codec = do
  mc <- createMockCluster 3
  mockCreateTopic mc "native-comp" 4 1
  let addrs = parseBootstraps (mcBootstraps mc)
      cfg = Native.defaultConfig
        { Native.ccBootstrap        = addrs
        , Native.ccLingerMs          = 5
        , Native.ccBatchNumMessages  = 10000
        , Native.ccBatchSize         = 1000000
        , Native.ccAcks              = Native.AcksLeader
        , Native.ccCompression       = codec
        }
  Right client <- Native.newClient cfg
  Right producer <- Native.newProducer client (Native.defaultProducerConfig cfg)
  _ <- Native.produce producer (Native.ProducerRecord "native-comp" Native.UnassignedPartition Nothing (Just "warmup") [])
  Native.flushProducer producer
  pure (NativeEnv producer client mc)

setupHwCompressed :: String -> IO HwEnv
setupHwCompressed codec = do
  mc <- createMockCluster 3
  mockCreateTopic mc "hw-comp" 4 1
  let props = HW.brokersList [HW.BrokerAddress (Text.pack (mcBootstraps mc))]
           <> HW.logLevel HW.KafkaLogErr
           <> HW.extraProps (M.fromList
                [ ("linger.ms",           "5")
                , ("batch.num.messages",  "10000")
                , ("batch.size",          "1000000")
                , ("acks",                "1")
                , ("compression.codec",   Text.pack codec)
                ])
  Right producer <- HW.newProducer props
  _ <- HW.produceMessage producer (hwRecord "hw-comp" Nothing (Just "warmup"))
  HW.flushProducer producer
  pure (HwEnv producer mc)

teardownNative :: NativeEnv -> IO ()
teardownNative env = do
  Native.closeProducer (neProducer env)
  Native.closeClient (neClient env)
  destroyMockCluster (neCluster env)

teardownHw :: HwEnv -> IO ()
teardownHw env = do
  HW.closeProducer (heProducer env)
  destroyMockCluster (heCluster env)

------------------------------------------------------------------------
-- Benchmark functions
------------------------------------------------------------------------

-- kafka-native: async produce N messages + flush
nativeAsyncFlush :: Native.KafkaProducer -> TopicName -> Int -> BS.ByteString -> IO ()
nativeAsyncFlush producer topic n payload = do
  let record = Native.ProducerRecord topic Native.UnassignedPartition
                 Nothing (Just payload) []
  replicateM_ n $ Native.produceAsync producer record
  Native.flushProducer producer

-- kafka-native: async produce with keys
nativeAsyncWithKeys :: Native.KafkaProducer -> Int -> BS.ByteString -> IO ()
nativeAsyncWithKeys producer n payload = do
  let go !i
        | i >= n = pure ()
        | otherwise = do
            let key = BS.pack [fromIntegral (i `mod` 256)]
                record = Native.ProducerRecord "native-bench" Native.UnassignedPartition
                           (Just key) (Just payload) []
            void $ Native.produceAsync producer record
            go (i + 1)
  go 0
  Native.flushProducer producer

-- kafka-native: async produce with headers
nativeAsyncWithHeaders :: Native.KafkaProducer -> Int -> BS.ByteString -> IO ()
nativeAsyncWithHeaders producer n payload = do
  let hdrs = [ Native.Header "trace-id" (Just "abc123")
             , Native.Header "content-type" (Just "application/octet-stream")
             ]
      record = Native.ProducerRecord "native-bench" Native.UnassignedPartition
                 Nothing (Just payload) hdrs
  replicateM_ n $ Native.produceAsync producer record
  Native.flushProducer producer

-- hw-kafka-client: produce N messages + flush
hwProduce :: HW.KafkaProducer -> HW.TopicName -> Int -> BS.ByteString -> IO ()
hwProduce producer topic n payload = do
  let record = HW.ProducerRecord
        { HW.prTopic = topic
        , HW.prPartition = HW.UnassignedPartition
        , HW.prKey = Nothing
        , HW.prValue = Just payload
        , HW.prHeaders = mempty
        }
  replicateM_ n $ HW.produceMessage producer record
  HW.flushProducer producer

-- hw-kafka-client: produce with keys
hwProduceWithKeys :: HW.KafkaProducer -> Int -> BS.ByteString -> IO ()
hwProduceWithKeys producer n payload = do
  let go !i
        | i >= n = pure ()
        | otherwise = do
            let key = BS.pack [fromIntegral (i `mod` 256)]
                record = HW.ProducerRecord
                  { HW.prTopic = "hw-bench"
                  , HW.prPartition = HW.UnassignedPartition
                  , HW.prKey = Just key
                  , HW.prValue = Just payload
                  , HW.prHeaders = mempty
                  }
            void $ HW.produceMessage producer record
            go (i + 1)
  go 0
  HW.flushProducer producer

-- hw-kafka-client: produce with headers
hwProduceWithHeaders :: HW.KafkaProducer -> Int -> BS.ByteString -> IO ()
hwProduceWithHeaders producer n payload = do
  let record = HW.ProducerRecord
        { HW.prTopic = "hw-bench"
        , HW.prPartition = HW.UnassignedPartition
        , HW.prKey = Nothing
        , HW.prValue = Just payload
        , HW.prHeaders = HW.headersFromList
            [ ("trace-id", "abc123")
            , ("content-type", "application/octet-stream")
            ]
        }
  replicateM_ n $ HW.produceMessage producer record
  HW.flushProducer producer

-- Helper: hw-kafka-client record
hwRecord :: Text.Text -> Maybe BS.ByteString -> Maybe BS.ByteString -> HW.ProducerRecord
hwRecord topic key value = HW.ProducerRecord
  { HW.prTopic = HW.TopicName topic
  , HW.prPartition = HW.UnassignedPartition
  , HW.prKey = key
  , HW.prValue = value
  , HW.prHeaders = mempty
  }

------------------------------------------------------------------------
-- Payloads (allocated once)
------------------------------------------------------------------------

payload100B, payload1KB, payload10KB :: BS.ByteString
payload100B  = BS.replicate 100 0x41
payload1KB   = BS.replicate 1000 0x41
payload10KB  = BS.replicate 10000 0x41

------------------------------------------------------------------------
-- Main
------------------------------------------------------------------------

main :: IO ()
main = defaultMain
  [ bgroup "low-latency (linger=1ms, batch=1K, acks=1)"
    [ bgroup "kafka-native"
      [ envWithCleanup setupNativeLowLat teardownNative $ \ ~env -> bgroup "async+flush"
          [ bench "1K × 100B"  $ nfIO $ nativeAsyncFlush (neProducer env) "native-bench" 1000 payload100B
          , bench "10K × 100B" $ nfIO $ nativeAsyncFlush (neProducer env) "native-bench" 10000 payload100B
          , bench "1K × 1KB"   $ nfIO $ nativeAsyncFlush (neProducer env) "native-bench" 1000 payload1KB
          , bench "1K × 10KB"  $ nfIO $ nativeAsyncFlush (neProducer env) "native-bench" 1000 payload10KB
          ]
      , envWithCleanup setupNativeLowLat teardownNative $ \ ~env ->
          bench "with-keys 1K × 100B" $ nfIO $ nativeAsyncWithKeys (neProducer env) 1000 payload100B
      , envWithCleanup setupNativeLowLat teardownNative $ \ ~env ->
          bench "with-headers 1K × 100B" $ nfIO $ nativeAsyncWithHeaders (neProducer env) 1000 payload100B
      ]
    , bgroup "hw-kafka-client"
      [ envWithCleanup setupHwLowLat teardownHw $ \ ~env -> bgroup "async+flush"
          [ bench "1K × 100B"  $ nfIO $ hwProduce (heProducer env) "hw-bench" 1000 payload100B
          , bench "10K × 100B" $ nfIO $ hwProduce (heProducer env) "hw-bench" 10000 payload100B
          , bench "1K × 1KB"   $ nfIO $ hwProduce (heProducer env) "hw-bench" 1000 payload1KB
          , bench "1K × 10KB"  $ nfIO $ hwProduce (heProducer env) "hw-bench" 1000 payload10KB
          ]
      , envWithCleanup setupHwLowLat teardownHw $ \ ~env ->
          bench "with-keys 1K × 100B" $ nfIO $ hwProduceWithKeys (heProducer env) 1000 payload100B
      , envWithCleanup setupHwLowLat teardownHw $ \ ~env ->
          bench "with-headers 1K × 100B" $ nfIO $ hwProduceWithHeaders (heProducer env) 1000 payload100B
      ]
    ]
  , bgroup "throughput (linger=5ms, batch=10K, acks=1)"
    [ bgroup "kafka-native"
      [ envWithCleanup setupNativeThroughput teardownNative $ \ ~env -> bgroup "async+flush"
          [ bench "1K × 100B"  $ nfIO $ nativeAsyncFlush (neProducer env) "native-bench" 1000 payload100B
          , bench "10K × 100B" $ nfIO $ nativeAsyncFlush (neProducer env) "native-bench" 10000 payload100B
          , bench "1K × 1KB"   $ nfIO $ nativeAsyncFlush (neProducer env) "native-bench" 1000 payload1KB
          , bench "1K × 10KB"  $ nfIO $ nativeAsyncFlush (neProducer env) "native-bench" 1000 payload10KB
          ]
      ]
    , bgroup "hw-kafka-client"
      [ envWithCleanup setupHwThroughput teardownHw $ \ ~env -> bgroup "async+flush"
          [ bench "1K × 100B"  $ nfIO $ hwProduce (heProducer env) "hw-bench" 1000 payload100B
          , bench "10K × 100B" $ nfIO $ hwProduce (heProducer env) "hw-bench" 10000 payload100B
          , bench "1K × 1KB"   $ nfIO $ hwProduce (heProducer env) "hw-bench" 1000 payload1KB
          , bench "1K × 10KB"  $ nfIO $ hwProduce (heProducer env) "hw-bench" 1000 payload10KB
          ]
      ]
    ]
  , bgroup "compression (linger=5ms, batch=10K, acks=1)"
    [ bgroup "kafka-native"
      [ envWithCleanup (setupNativeCompressed Native.Lz4) teardownNative $ \ ~env ->
          bench "lz4 1K × 100B" $ nfIO $ nativeAsyncFlush (neProducer env) "native-comp" 1000 payload100B
      , envWithCleanup (setupNativeCompressed Native.Snappy) teardownNative $ \ ~env ->
          bench "snappy 1K × 100B" $ nfIO $ nativeAsyncFlush (neProducer env) "native-comp" 1000 payload100B
      , envWithCleanup (setupNativeCompressed Native.Gzip) teardownNative $ \ ~env ->
          bench "gzip 1K × 100B" $ nfIO $ nativeAsyncFlush (neProducer env) "native-comp" 1000 payload100B
      , envWithCleanup (setupNativeCompressed Native.Zstd) teardownNative $ \ ~env ->
          bench "zstd 1K × 100B" $ nfIO $ nativeAsyncFlush (neProducer env) "native-comp" 1000 payload100B
      ]
    , bgroup "hw-kafka-client"
      [ envWithCleanup (setupHwCompressed "lz4") teardownHw $ \ ~env ->
          bench "lz4 1K × 100B" $ nfIO $ hwProduce (heProducer env) "hw-comp" 1000 payload100B
      , envWithCleanup (setupHwCompressed "snappy") teardownHw $ \ ~env ->
          bench "snappy 1K × 100B" $ nfIO $ hwProduce (heProducer env) "hw-comp" 1000 payload100B
      , envWithCleanup (setupHwCompressed "gzip") teardownHw $ \ ~env ->
          bench "gzip 1K × 100B" $ nfIO $ hwProduce (heProducer env) "hw-comp" 1000 payload100B
      , envWithCleanup (setupHwCompressed "zstd") teardownHw $ \ ~env ->
          bench "zstd 1K × 100B" $ nfIO $ hwProduce (heProducer env) "hw-comp" 1000 payload100B
      ]
    ]
  ]
