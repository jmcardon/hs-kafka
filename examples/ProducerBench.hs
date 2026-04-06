{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}

-- | Long-running producer benchmark with delivery report tracking.
--
-- Architecture:
--   Thread 1 (producer): produces ~2000 msg/sec in batches of 100
--   Thread 2 (poller): polls delivery reports, tracks per-batch progress
--   Main thread: prints stats every second
--
-- Run with:
--   cabal run producer-bench -- +RTS -N2 -l -s -h -RTS
--
-- Then inspect:
--   threadscope producer-bench.eventlog
--   hp2ps -c producer-bench.hp && open producer-bench.ps
module Main where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM
import Control.Monad (forM_, when, unless)
import qualified Data.ByteString as BS
import Data.IORef
import Data.Int (Int64)
import Data.Word (Word64)
import System.IO (hSetBuffering, BufferMode(..), stdout)
import GHC.Clock (getMonotonicTimeNSec)

import Kafka.Client
import Kafka.Internal.Config
import Kafka.Producer
import MockCluster

------------------------------------------------------------------------
-- Configuration
------------------------------------------------------------------------

-- | How long to run (seconds)
runDurationSec :: Int
runDurationSec = 30

-- | Messages per batch
batchSize :: Int
batchSize = 100

-- | Target batches per second
batchesPerSec :: Int
batchesPerSec = 20  -- 20 * 100 = 2000 msg/sec

-- | Payload size
payloadSize :: Int
payloadSize = 200

-- | Number of partitions
numPartitions :: Int
numPartitions = 8

------------------------------------------------------------------------
-- Stats tracking
------------------------------------------------------------------------

data Stats = Stats
  { stSent        :: !(IORef Int64)
  , stDelivered   :: !(IORef Int64)
  , stFailed      :: !(IORef Int64)
  , stBatches     :: !(IORef Int64)
  , stBatchesDone :: !(IORef Int64)
  , stLatencySum  :: !(IORef Int64)  -- sum of delivery latencies (μs)
  , stLatencyMax  :: !(IORef Int64)
  }

newStats :: IO Stats
newStats = Stats
  <$> newIORef 0 <*> newIORef 0 <*> newIORef 0
  <*> newIORef 0 <*> newIORef 0
  <*> newIORef 0 <*> newIORef 0

inc :: IORef Int64 -> IO ()
inc ref = atomicModifyIORef' ref (\n -> (n + 1, ()))

add :: IORef Int64 -> Int64 -> IO ()
add ref v = atomicModifyIORef' ref (\n -> (n + v, ()))

maxUpdate :: IORef Int64 -> Int64 -> IO ()
maxUpdate ref v = atomicModifyIORef' ref (\n -> (max n v, ()))

------------------------------------------------------------------------
-- Main
------------------------------------------------------------------------

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  putStrLn "=== kafka-native Producer Benchmark ==="
  putStrLn $ "Config: " ++ show (batchesPerSec * batchSize) ++ " msg/sec target"
  putStrLn $ "        " ++ show batchSize ++ " msg/batch, "
          ++ show payloadSize ++ "B payload, "
          ++ show numPartitions ++ " partitions"
  putStrLn $ "        " ++ show runDurationSec ++ " seconds"
  putStrLn ""

  -- Start mock cluster
  mc <- createMockCluster 3
  mockCreateTopic mc "bench-topic" numPartitions 3
  let addrs = parseBootstraps (mcBootstraps mc)

  -- Create client + producer
  let cfg = defaultConfig
        { ccBootstrap = addrs
        , ccLingerMs = 5
        , ccBatchNumMessages = batchSize
        , ccBatchSize = 1000000
        , ccAcks = AcksAll
        }
  Right client <- newClient cfg
  Right producer <- newProducer client (defaultProducerConfig cfg)

  stats <- newStats
  shutdownVar <- newTVarIO False

  -- Thread 1: Producer
  _ <- forkIO $ producerLoop producer stats shutdownVar

  -- Thread 2: Poller (delivery reports)
  _ <- forkIO $ pollerLoop producer stats shutdownVar

  -- Main thread: stats reporter
  startTime <- getMonotonicTimeNSec
  statsLoop stats startTime shutdownVar

  -- Signal shutdown
  atomically $ writeTVar shutdownVar True
  threadDelay 1000000  -- let threads drain

  -- Final stats
  printFinalStats stats startTime

  -- Cleanup
  flushProducer producer
  closeProducer producer
  closeClient client
  destroyMockCluster mc
  putStrLn "\nDone."

------------------------------------------------------------------------
-- Producer thread
------------------------------------------------------------------------

producerLoop :: KafkaProducer -> Stats -> TVar Bool -> IO ()
producerLoop producer stats shutdownVar = do
  let payload = BS.replicate payloadSize 0x41
      delayUs = 1000000 `div` batchesPerSec
  go payload delayUs
  where
    go !payload !delayUs = do
      done <- readTVarIO shutdownVar
      unless done $ do
        -- Produce a batch
        forM_ [1..batchSize] $ \_ -> do
          let record = ProducerRecord
                { prTopic = "bench-topic"
                , prPartition = UnassignedPartition
                , prKey = Nothing
                , prValue = Just payload
                , prHeaders = []
                }
          _ <- produceAsync producer record
          inc (stSent stats)
        inc (stBatches stats)

        -- Pace to target rate
        threadDelay delayUs
        go payload delayUs

------------------------------------------------------------------------
-- Poller thread
------------------------------------------------------------------------

pollerLoop :: KafkaProducer -> Stats -> TVar Bool -> IO ()
pollerLoop producer stats shutdownVar = do
  done <- readTVarIO shutdownVar
  unless done $ do
    reports <- pollEvents producer 100  -- 100ms timeout
    forM_ reports $ \dr -> case dr of
      DeliverySuccess _ _ -> inc (stDelivered stats)
      DeliveryFailure _ _ -> inc (stFailed stats)

    -- Count batch completions (every batchSize deliveries)
    delivered <- readIORef (stDelivered stats)
    batchesDone <- readIORef (stBatchesDone stats)
    let expectedBatches = delivered `div` fromIntegral batchSize
    when (expectedBatches > batchesDone) $ do
      let newBatches = expectedBatches - batchesDone
      add (stBatchesDone stats) newBatches

    pollerLoop producer stats shutdownVar

------------------------------------------------------------------------
-- Stats reporter (main thread)
------------------------------------------------------------------------

statsLoop :: Stats -> Word64 -> TVar Bool -> IO ()
statsLoop stats _startTime shutdownVar = go (0 :: Int)
  where
    go !elapsed = do
      threadDelay 1000000  -- 1 second
      let !elapsed' = elapsed + 1
      _ <- readTVarIO shutdownVar

      sent <- readIORef (stSent stats)
      delivered <- readIORef (stDelivered stats)
      failed <- readIORef (stFailed stats)
      batches <- readIORef (stBatches stats)
      batchesDone <- readIORef (stBatchesDone stats)

      let !rate = if elapsed' > 0 then sent `div` (fromIntegral elapsed' :: Int64) else 0
          !pending = sent - delivered - failed

      putStrLn $ "[" ++ show elapsed' ++ "s] "
        ++ "sent=" ++ show sent
        ++ " delivered=" ++ show delivered
        ++ " failed=" ++ show failed
        ++ " pending=" ++ show pending
        ++ " rate=" ++ show rate ++ "/s"
        ++ " batches=" ++ show batches ++ "/" ++ show batchesDone

      when (elapsed' >= fromIntegral runDurationSec) $
        atomically $ writeTVar shutdownVar True

      done' <- readTVarIO shutdownVar
      unless done' $ go elapsed'

------------------------------------------------------------------------
-- Final stats
------------------------------------------------------------------------

printFinalStats :: Stats -> Word64 -> IO ()
printFinalStats stats startTime = do
  endTime <- getMonotonicTimeNSec
  let durationMs = (endTime - startTime) `div` 1000000

  sent <- readIORef (stSent stats)
  delivered <- readIORef (stDelivered stats)
  failed <- readIORef (stFailed stats)
  batches <- readIORef (stBatches stats)
  batchesDone <- readIORef (stBatchesDone stats)

  putStrLn "\n=== Final Statistics ==="
  putStrLn $ "Duration:    " ++ show durationMs ++ " ms"
  putStrLn $ "Sent:        " ++ show sent
  putStrLn $ "Delivered:   " ++ show delivered
  putStrLn $ "Failed:      " ++ show failed
  putStrLn $ "Batches:     " ++ show batches ++ " sent, " ++ show batchesDone ++ " completed"
  putStrLn $ "Throughput:  " ++ show (sent * 1000 `div` fromIntegral durationMs) ++ " msg/sec"
  putStrLn $ "Drop rate:   " ++ show (if sent > 0 then failed * 100 `div` sent else 0) ++ "%"
