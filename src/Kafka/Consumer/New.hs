{-# language
    BangPatterns
  , LambdaCase
  , OverloadedStrings
  #-}

-- | Consumer implementation using KafkaClient infrastructure.
--
-- Two modes:
-- 1. Group mode: subscribe(topics) → join/sync/heartbeat lifecycle
-- 2. Assign mode: assign(partitions) → fetch directly (no group)
--
-- All broker communication via enqueueRequest (existing broker threads).
-- Messages queue into TBQueue for consumerPoll.
module Kafka.Consumer.New
  ( -- * Lifecycle
    newConsumer
  , closeConsumer
  , withConsumer
    -- * Polling
  , consumerPoll
  , consumerPollBatch
    -- * Offsets
  , commitSync
  , commitAsync
  , commitOffsetMessage
  , storeOffset
    -- * Assignment / subscription
  , assign
  , subscription
  , assignment
    -- * Seek / pause / resume
  , seek
  , pausePartitions
  , resumePartitions
    -- * Query
  , committed
  , position
    -- * Types (re-export)
  , module Kafka.Consumer.Types
  ) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM
import Control.Exception (mask, onException)
import Control.Monad (void, unless, when, forM_)
import Data.Foldable (traverse_)
import Data.Int (Int32, Int64)
import Data.Map.Strict (Map)
import Data.Set (Set)
import Numeric.Natural (Natural)

import qualified Data.Map.Strict as Map
import qualified Data.Set as Set

import Kafka.Common
import Kafka.Client
import Kafka.Internal.Broker (BrokerEnv(..), enqueueRequest)
import Kafka.Internal.Config
import Kafka.Internal.FindCoordinator.Request (findCoordinatorRequest)
import Kafka.Internal.FindCoordinator.Response (parseFindCoordinatorResponse)
import Kafka.Internal.JoinGroup.Request (joinGroupRequest)
import Kafka.Internal.SyncGroup.Request (syncGroupRequest)
import Kafka.Internal.Heartbeat.Request (heartbeatRequest)
import Kafka.Internal.Fetch.Request (sessionlessFetchRequest)
import Kafka.Internal.OffsetCommit.Request (offsetCommitRequest)
import Kafka.Consumer.Types
import Kafka.Producer.Types (Header(..))
import qualified Kafka.Internal.Wire as Wire
import qualified Kafka.Internal.JoinGroup.Response as J
import qualified Kafka.Internal.SyncGroup.Response as S
import qualified Kafka.Internal.Heartbeat.Response as H
import qualified Kafka.Internal.Fetch.Response as F
import qualified Kafka.Internal.OffsetCommit.Response as C

------------------------------------------------------------------------
-- Lifecycle
------------------------------------------------------------------------

newConsumer :: KafkaClient -> ConsumerConfig -> IO (Either KafkaException KafkaConsumer)
newConsumer client cfg = do
  let fetchQueueSize = fromIntegral (ccMaxPollRecords cfg * 2) :: Natural
  fetchQ <- newTBQueueIO fetchQueueSize
  shutdownVar <- newTVarIO False
  pausedVar <- newTVarIO Set.empty
  stateVar <- newTVarIO ConsumerGroupState
    { cgsJoinState = JoinInit
    , cgsMemberId = Nothing
    , cgsGenerationId = -1
    , cgsAssignment = []
    , cgsOffsets = Map.empty
    , cgsCommitted = Map.empty
    }
  modeVar <- newTVarIO ModeSubscribe
  let consumer = KafkaConsumer client cfg stateVar fetchQ shutdownVar pausedVar modeVar

  forM_ (ccTopics cfg) $ \topic ->
    refreshTopicMetadata client topic

  void $ forkIO $ consumerGroupThread consumer

  when (ccAutoCommit cfg) $
    void $ forkIO $ autoCommitLoop consumer

  pure (Right consumer)

withConsumer :: KafkaClient -> ConsumerConfig
            -> (KafkaConsumer -> IO a) -> IO (Either KafkaException a)
withConsumer client cfg action = mask $ \restore -> do
  result <- newConsumer client cfg
  case result of
    Left err -> pure (Left err)
    Right consumer -> do
      a <- restore (action consumer) `onException` closeConsumer consumer
      closeConsumer consumer
      pure (Right a)

closeConsumer :: KafkaConsumer -> IO ()
closeConsumer consumer = do
  -- Commit offsets before leaving
  state <- readTVarIO (consGroupState consumer)
  when (ccAutoCommit (consConfig consumer) && not (Map.null (cgsOffsets state))) $
    void $ commitSync consumer
  atomically $ writeTVar (consShutdown consumer) True

------------------------------------------------------------------------
-- Polling
------------------------------------------------------------------------

-- | Poll for a single record.
consumerPoll :: KafkaConsumer -> Int -> IO (Maybe ConsumerRecord)
consumerPoll consumer timeoutMs = do
  batch <- consumerPollBatch consumer timeoutMs 1
  pure $ case batch of
    (r:_) -> Just r
    []    -> Nothing

-- | Poll for a batch of records, up to @maxRecords@.
consumerPollBatch :: KafkaConsumer -> Int -> Int -> IO [ConsumerRecord]
consumerPollBatch consumer timeoutMs maxRecords = do
  immediate <- atomically $ flushTBQueue (consFetchQueue consumer)
  if not (null immediate)
    then pure (take maxRecords immediate)
    else if timeoutMs == 0
      then pure []
      else do
        timer <- if timeoutMs < 0
          then newTVarIO False
          else registerDelay (timeoutMs * 1000)
        atomically $ do
          timedOut <- readTVar timer
          if timedOut
            then pure []
            else do
              first <- readTBQueue (consFetchQueue consumer)
              rest <- flushTBQueue (consFetchQueue consumer)
              pure (take maxRecords (first : rest))

------------------------------------------------------------------------
-- Offset management
------------------------------------------------------------------------

-- | Commit all current offsets synchronously.
commitSync :: KafkaConsumer -> IO (Either KafkaException ())
commitSync consumer = do
  state <- readTVarIO (consGroupState consumer)
  if Map.null (cgsOffsets state)
    then pure (Right ())
    else doCommit consumer (cgsOffsets state)

-- | Commit all current offsets asynchronously.
commitAsync :: KafkaConsumer -> IO ()
commitAsync consumer = void $ forkIO $ void $ commitSync consumer

-- | Commit the offset of a specific consumed record.
-- The committed offset is record.offset + 1 (next offset to fetch).
commitOffsetMessage :: KafkaConsumer -> ConsumerRecord -> IO (Either KafkaException ())
commitOffsetMessage consumer record = do
  let key = (crTopic record, crPartition record)
      nextOffset = crOffset record + 1
  doCommit consumer (Map.singleton key nextOffset)

-- | Store an offset locally without committing. The stored offset will
-- be committed on the next auto-commit or manual commitSync.
storeOffset :: KafkaConsumer -> ConsumerRecord -> IO ()
storeOffset consumer record = atomically $ modifyTVar' (consGroupState consumer) $ \s ->
  s { cgsOffsets = Map.insert (crTopic record, crPartition record)
                              (crOffset record + 1) (cgsOffsets s) }

doCommit :: KafkaConsumer -> Map (TopicName, Int32) Int64 -> IO (Either KafkaException ())
doCommit consumer offsets = do
  mBroker <- anyBroker (consClient consumer)
  case mBroker of
    Nothing -> pure (Left (KafkaException "no broker for offset commit"))
    Just env -> do
      let cfg = consConfig consumer
          gName = ccGroupId cfg
          -- Group offsets by topic
          byTopic = Map.foldlWithKey' (\acc (t, p) off ->
            Map.insertWith (++) t [PartitionOffset p off] acc) Map.empty offsets
      state <- readTVarIO (consGroupState consumer)
      let member = GroupMember gName (cgsMemberId state)
          genId = GenerationId (cgsGenerationId state)
      -- Commit each topic's offsets
      results <- mapM (\(topic, partOffs) -> do
        let reqBytes = offsetCommitRequest topic partOffs member genId
        respVar <- enqueueRequest env reqBytes
        response <- atomically $ readTMVar respVar
        case response of
          Left err -> pure (Left err)
          Right bytes -> case Wire.runWire C.parseOffsetCommitResponse bytes of
            Nothing -> pure (Left (KafkaParseException "offset commit parse failed"))
            Just _resp -> pure (Right ())
        ) (Map.toList byTopic)
      case [e | Left e <- results] of
        [] -> do
          atomically $ modifyTVar' (consGroupState consumer) $ \s ->
            s { cgsCommitted = Map.union offsets (cgsCommitted s) }
          pure (Right ())
        (e:_) -> pure (Left e)
------------------------------------------------------------------------
-- Assignment / subscription
------------------------------------------------------------------------

-- | Manually assign partitions (no consumer group needed).
-- Replaces any existing assignment.
assign :: KafkaConsumer -> [TopicPartition] -> IO ()
assign consumer tps = atomically $ do
  writeTVar (consMode consumer) ModeAssign
  modifyTVar' (consGroupState consumer) $ \s ->
    s { cgsAssignment = tps
      , cgsJoinState = JoinSteady
      , cgsOffsets = Map.fromList [((tpTopic tp, tpPartition tp), tpOffset tp) | tp <- tps]
      }

-- | Get current topic subscription.
subscription :: KafkaConsumer -> IO [TopicName]
subscription consumer = pure $ ccTopics (consConfig consumer)

-- | Get current partition assignment.
assignment :: KafkaConsumer -> IO [TopicPartition]
assignment consumer = cgsAssignment <$> readTVarIO (consGroupState consumer)

------------------------------------------------------------------------
-- Seek / pause / resume
------------------------------------------------------------------------

-- | Seek to a specific offset for a partition.
seek :: KafkaConsumer -> TopicName -> Int32 -> Int64 -> IO ()
seek consumer topic partition offset = atomically $
  modifyTVar' (consGroupState consumer) $ \s ->
    s { cgsOffsets = Map.insert (topic, partition) offset (cgsOffsets s) }

-- | Pause fetching from specified partitions.
pausePartitions :: KafkaConsumer -> [(TopicName, Int32)] -> IO ()
pausePartitions consumer parts = atomically $
  modifyTVar' (consPaused consumer) $ \s ->
    Set.union s (Set.fromList parts)

-- | Resume fetching from paused partitions.
resumePartitions :: KafkaConsumer -> [(TopicName, Int32)] -> IO ()
resumePartitions consumer parts = atomically $
  modifyTVar' (consPaused consumer) $ \s ->
    Set.difference s (Set.fromList parts)

------------------------------------------------------------------------
-- Query
------------------------------------------------------------------------

-- | Get last committed offsets for partitions.
committed :: KafkaConsumer -> IO (Map (TopicName, Int32) Int64)
committed consumer = cgsCommitted <$> readTVarIO (consGroupState consumer)

-- | Get current fetch positions (next offset to fetch).
position :: KafkaConsumer -> IO (Map (TopicName, Int32) Int64)
position consumer = cgsOffsets <$> readTVarIO (consGroupState consumer)

------------------------------------------------------------------------
-- Consumer group thread
------------------------------------------------------------------------

consumerGroupThread :: KafkaConsumer -> IO ()
consumerGroupThread consumer = do
  done <- readTVarIO (consShutdown consumer)
  unless done $ do
    mode <- readTVarIO (consMode consumer)
    case mode of
      ModeAssign -> do
        -- In assign mode, skip join/sync. Just run fetch loop.
        fetchLoop consumer
        threadDelay 100000  -- brief pause before re-entering
        consumerGroupThread consumer
      ModeSubscribe -> do
        joinResult <- joinConsumerGroup consumer
        case joinResult of
          Left _err -> do
            threadDelay 1000000
            consumerGroupThread consumer
          Right () -> do
            _ <- forkIO $ heartbeatThread consumer
            fetchLoop consumer
            atomically $ modifyTVar' (consGroupState consumer) $ \s ->
              s { cgsJoinState = JoinInit }
            consumerGroupThread consumer

------------------------------------------------------------------------
-- Join group
------------------------------------------------------------------------

joinConsumerGroup :: KafkaConsumer -> IO (Either KafkaException ())
joinConsumerGroup consumer = do
  let cfg = consConfig consumer
      gName = ccGroupId cfg
      topic = case ccTopics cfg of { (t:_) -> t; [] -> "" }
  mBroker <- anyBroker (consClient consumer)
  case mBroker of
    Nothing -> pure (Left (KafkaException "no broker for join group"))
    Just env -> do
      let coordReq = findCoordinatorRequest (getGroupName gName) 0
      coordVar <- enqueueRequest env coordReq
      coordResp <- atomically $ readTMVar coordVar
      case coordResp of
        Left err -> pure (Left err)
        Right bytes -> case Wire.runWire parseFindCoordinatorResponse bytes of
          Nothing -> pure (Left (KafkaParseException "coordinator parse failed"))
          Just _coord -> do
            state <- readTVarIO (consGroupState consumer)
            let member = GroupMember gName (cgsMemberId state)
                joinReq = joinGroupRequest topic member
            joinVar <- enqueueRequest env joinReq
            joinResp <- atomically $ readTMVar joinVar
            case joinResp of
              Left err -> pure (Left err)
              Right jBytes -> case Wire.runWire J.parseJoinGroupResponse jBytes of
                Nothing -> pure (Left (KafkaParseException "join group parse failed"))
                Just jgr
                  | J.errorCode jgr /= 0 ->
                      pure (Left (KafkaUnexpectedErrorCodeException (J.errorCode jgr)))
                  | otherwise -> do
                      let newMemId = Just (J.memberId jgr)
                          newGenId = J.generationId jgr
                          newMember = GroupMember gName newMemId
                      atomically $ modifyTVar' (consGroupState consumer) $ \s ->
                        s { cgsMemberId = newMemId, cgsGenerationId = newGenId, cgsJoinState = JoinSyncing }
                      let syncReq = syncGroupRequest newMember (GenerationId newGenId) []
                      syncVar <- enqueueRequest env syncReq
                      syncResp <- atomically $ readTMVar syncVar
                      case syncResp of
                        Left err -> pure (Left err)
                        Right sBytes -> case Wire.runWire S.parseSyncGroupResponse sBytes of
                          Nothing -> pure (Left (KafkaParseException "sync group parse failed"))
                          Just sgr
                            | S.errorCode sgr /= 0 ->
                                pure (Left (KafkaUnexpectedErrorCodeException (S.errorCode sgr)))
                            | otherwise -> do
                                let tpAssigns = case S.memberAssignment sgr of
                                      Nothing -> []
                                      Just ma -> concatMap
                                        (\sta -> map (\p -> TopicPartition (S.topic sta) p 0) (S.partitions sta))
                                        (S.partitionAssignments ma)
                                atomically $ modifyTVar' (consGroupState consumer) $ \s ->
                                  s { cgsAssignment = tpAssigns
                                    , cgsJoinState = JoinSteady
                                    , cgsOffsets = Map.fromList
                                        [((tpTopic tp, tpPartition tp), 0) | tp <- tpAssigns]
                                    }
                                traverse_ ($ PartitionsAssigned tpAssigns) (ccRebalanceCallback cfg)
                                pure (Right ())

------------------------------------------------------------------------
-- Fetch loop
------------------------------------------------------------------------

fetchLoop :: KafkaConsumer -> IO ()
fetchLoop consumer = do
  done <- readTVarIO (consShutdown consumer)
  state <- readTVarIO (consGroupState consumer)
  unless (done || cgsJoinState state /= JoinSteady) $ do
    paused <- readTVarIO (consPaused consumer)
    let cfg = consConfig consumer
        activeParts = filter (\tp -> Set.notMember (tpTopic tp, tpPartition tp) paused)
                             (cgsAssignment state)
        offsets = cgsOffsets state
    forM_ activeParts $ \tp -> do
      let currentOff = Map.findWithDefault 0 (tpTopic tp, tpPartition tp) offsets
      mBroker <- leaderBrokerFor (consClient consumer) (tpTopic tp) (tpPartition tp)
      case mBroker of
        Nothing -> pure ()
        Just env -> do
          let fetchReq = sessionlessFetchRequest
                (ccFetchWaitMs cfg) (tpTopic tp)
                [PartitionOffset (tpPartition tp) currentOff]
                (ccFetchMaxBytes cfg)
          respVar <- enqueueRequest env fetchReq
          response <- atomically $ readTMVar respVar
          case response of
            Left _err -> pure ()  -- Broker error — will retry next cycle
            Right bytes -> case Wire.runWire F.parseFetchResponse bytes of
              Nothing -> pure ()  -- Parse error — will retry next cycle
              Just fetchResp -> do
                let records = extractRecords (tpTopic tp) fetchResp
                    maxOff = if null records then currentOff
                             else maximum (map crOffset records) + 1
                -- Enqueue records + update offset in one transaction
                atomically $ do
                  forM_ records $ writeTBQueue (consFetchQueue consumer)
                  modifyTVar' (consGroupState consumer) $ \s ->
                    s { cgsOffsets = Map.insert (tpTopic tp, tpPartition tp) maxOff (cgsOffsets s) }
    threadDelay (ccFetchWaitMs cfg * 1000)
    fetchLoop consumer

extractRecords :: TopicName -> F.FetchResponse -> [ConsumerRecord]
extractRecords topicFilter resp =
  [ ConsumerRecord
      { crTopic = F.topic ft
      , crPartition = F.partition (F.partitionHeader fp)
      , crOffset = F.baseOffset rb + fromIntegral (F.recordOffsetDelta r)
      , crTimestamp = F.firstTimestamp rb + fromIntegral (F.recordTimestampDelta r)
      , crKey = F.recordKey r
      , crValue = F.recordValue r
      , crHeaders = []
      }
  | ft <- F.topics resp
  , F.topic ft == topicFilter
  , fp <- F.partitions ft
  , Just batches <- [F.recordSet fp]
  , rb <- batches
  , r <- F.records rb
  ]

------------------------------------------------------------------------
-- Heartbeat thread
------------------------------------------------------------------------

heartbeatThread :: KafkaConsumer -> IO ()
heartbeatThread consumer = do
  done <- readTVarIO (consShutdown consumer)
  state <- readTVarIO (consGroupState consumer)
  unless (done || cgsJoinState state /= JoinSteady) $ do
    let cfg = consConfig consumer
    threadDelay (ccHeartbeatMs cfg * 1000)
    mBroker <- anyBroker (consClient consumer)
    traverse_ (\env -> do
      let member = GroupMember (ccGroupId cfg) (cgsMemberId state)
          genId = GenerationId (cgsGenerationId state)
          hbReq = heartbeatRequest member genId
      respVar <- enqueueRequest env hbReq
      response <- atomically $ readTMVar respVar
      case response of
        Left _ -> pure ()
        Right bytes -> case Wire.runWire H.parseHeartbeatResponse bytes of
          Nothing -> pure ()
          Just hbResp ->
            when (H.errorCode hbResp /= 0) $
              atomically $ modifyTVar' (consGroupState consumer) $ \s ->
                s { cgsJoinState = JoinInit }
      ) mBroker
    heartbeatThread consumer

------------------------------------------------------------------------
-- Auto-commit loop
------------------------------------------------------------------------

autoCommitLoop :: KafkaConsumer -> IO ()
autoCommitLoop consumer = do
  let intervalMs = ccAutoCommitMs (consConfig consumer)
  threadDelay (intervalMs * 1000)
  done <- readTVarIO (consShutdown consumer)
  unless done $ do
    state <- readTVarIO (consGroupState consumer)
    when (cgsJoinState state == JoinSteady && not (Map.null (cgsOffsets state))) $
      void $ commitSync consumer
    autoCommitLoop consumer
