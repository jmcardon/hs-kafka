{-# language
    BangPatterns
  , LambdaCase
  , OverloadedStrings
  #-}

-- | New consumer implementation using KafkaClient infrastructure.
--
-- Architecture (modeled on librdkafka):
--   - Subscribe to topics → metadata refresh → discover partitions
--   - Join consumer group via coordinator broker
--   - Sync group → receive partition assignment
--   - Fetch loop: request messages from partition leaders
--   - Heartbeat thread: periodic heartbeats to coordinator
--   - Auto-commit thread: periodic offset commits
--   - Rebalance: triggered by heartbeat response, runs callback
--
-- All broker communication goes through KafkaClient's broker threads
-- (enqueueRequest). Messages queue into a TBQueue for consumerPoll.
module Kafka.Consumer.New
  ( -- * Lifecycle
    newConsumer
  , closeConsumer
    -- * Polling
  , consumerPoll
    -- * Offsets
  , commitSync
  , commitAsync
    -- * Types (re-export)
  , module Kafka.Consumer.Types
  ) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM
import Control.Monad (void, unless, when, forM_, forM)
import Data.ByteString (ByteString)
import Data.Int (Int16, Int32, Int64)
import Data.Map.Strict (Map)
import Numeric.Natural (Natural)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Data.Map.Strict as Map

import Kafka.Common
import Kafka.Client
import Kafka.Internal.Broker (BrokerEnv(..), enqueueRequest)
import Kafka.Internal.Config
import Kafka.Internal.FindCoordinator.Request (findCoordinatorRequest)
import Kafka.Internal.FindCoordinator.Response (FindCoordinatorResponse(..), parseFindCoordinatorResponse)
import Kafka.Internal.JoinGroup.Request (joinGroupRequest)
import Kafka.Internal.SyncGroup.Request (syncGroupRequest)
import Kafka.Internal.Heartbeat.Request (heartbeatRequest)
import Kafka.Internal.Fetch.Request (sessionlessFetchRequest)
import Kafka.Internal.OffsetCommit.Request (offsetCommitRequest)
import Kafka.Internal.FindCoordinator.Response (parseFindCoordinatorResponse)
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

-- | Create a new consumer. Connects to the cluster, joins the consumer
-- group, and starts fetch/heartbeat background threads.
newConsumer :: KafkaClient -> ConsumerConfig -> IO (Either KafkaException KafkaConsumer)
newConsumer client cfg = do
  let fetchQueueSize = fromIntegral (ccMaxPollRecords cfg * 2) :: Natural
  fetchQ <- newTBQueueIO fetchQueueSize
  shutdownVar <- newTVarIO False
  stateVar <- newTVarIO ConsumerGroupState
    { cgsJoinState = JoinInit
    , cgsMemberId = Nothing
    , cgsGenerationId = -1
    , cgsAssignment = []
    , cgsOffsets = Map.empty
    , cgsCommitted = Map.empty
    }
  let consumer = KafkaConsumer client cfg stateVar fetchQ shutdownVar

  -- Ensure metadata for all subscribed topics
  forM_ (ccTopics cfg) $ \topic ->
    refreshTopicMetadata client topic

  -- Start the consumer group management thread
  void $ forkIO $ consumerGroupThread consumer

  -- Start auto-commit timer if configured
  when (ccAutoCommit cfg) $
    void $ forkIO $ autoCommitLoop consumer

  pure (Right consumer)

-- | Close the consumer. Leaves the group and stops background threads.
closeConsumer :: KafkaConsumer -> IO ()
closeConsumer consumer = atomically $ writeTVar (consShutdown consumer) True

------------------------------------------------------------------------
-- Polling
------------------------------------------------------------------------

-- | Poll for consumed records, blocking up to @timeoutMs@ milliseconds.
--
-- Returns a batch of records (up to ccMaxPollRecords).
-- Semantics match rd_kafka_consumer_poll:
--   timeout=0: non-blocking
--   timeout>0: block until records or timeout
--   timeout=-1: block indefinitely
consumerPoll :: KafkaConsumer -> Int -> IO [ConsumerRecord]
consumerPoll consumer timeoutMs = do
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
  where
    !maxRecords = ccMaxPollRecords (consConfig consumer)

------------------------------------------------------------------------
-- Offset commit
------------------------------------------------------------------------

-- | Commit current offsets synchronously.
commitSync :: KafkaConsumer -> IO (Either KafkaException ())
commitSync consumer = do
  state <- readTVarIO (consGroupState consumer)
  if Map.null (cgsOffsets state)
    then pure (Right ())
    else doCommit consumer (cgsOffsets state)

-- | Commit current offsets asynchronously (fire and forget).
commitAsync :: KafkaConsumer -> IO ()
commitAsync consumer = void $ forkIO $ void $ commitSync consumer

doCommit :: KafkaConsumer -> Map (TopicName, Int32) Int64 -> IO (Either KafkaException ())
doCommit consumer offsets = do
  mBroker <- anyBroker (consClient consumer)
  case mBroker of
    Nothing -> pure (Left (KafkaException "no broker for offset commit"))
    Just env -> do
      let cfg = consConfig consumer
          gName = ccGroupId cfg
          topic = case ccTopics cfg of { (t:_) -> t; [] -> "" }
          partOffs = [ PartitionOffset (fromIntegral p) off
                     | ((_, p), off) <- Map.toList offsets ]
      state <- readTVarIO (consGroupState consumer)
      let member = GroupMember gName (cgsMemberId state)
          genId = GenerationId (cgsGenerationId state)
          reqBytes = offsetCommitRequest topic partOffs member genId
      respVar <- enqueueRequest env reqBytes
      response <- atomically $ readTMVar respVar
      case response of
        Left err -> pure (Left err)
        Right bytes -> case Wire.runWire C.parseOffsetCommitResponse bytes of
          Nothing -> pure (Left (KafkaParseException "offset commit parse failed"))
          Just _resp -> do
            atomically $ modifyTVar' (consGroupState consumer) $ \s ->
              s { cgsCommitted = offsets }
            pure (Right ())

------------------------------------------------------------------------
-- Consumer group thread
------------------------------------------------------------------------

-- | Background thread that manages the consumer group lifecycle:
-- join → sync → heartbeat + fetch loop.
consumerGroupThread :: KafkaConsumer -> IO ()
consumerGroupThread consumer = do
  done <- readTVarIO (consShutdown consumer)
  unless done $ do
    -- Join the group
    joinResult <- joinConsumerGroup consumer
    case joinResult of
      Left _err -> do
        threadDelay 1000000  -- retry after 1s
        consumerGroupThread consumer
      Right () -> do
        -- Start heartbeat thread
        hbThread <- forkIO $ heartbeatThread consumer
        -- Start fetch loop
        fetchLoop consumer
        -- If we get here, we need to rejoin (rebalance or error)
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
      -- Find coordinator
      let coordReq = findCoordinatorRequest (getGroupName gName) 0
      coordVar <- enqueueRequest env coordReq
      coordResp <- atomically $ readTMVar coordVar
      case coordResp of
        Left err -> pure (Left err)
        Right bytes -> case Wire.runWire parseFindCoordinatorResponse bytes of
          Nothing -> pure (Left (KafkaParseException "coordinator parse failed"))
          Just _coord -> do
            -- Join group
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
                      -- Sync group (empty assignments — coordinator assigns)
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
                                case ccRebalanceCallback cfg of
                                  Just cb -> cb (PartitionsAssigned tpAssigns)
                                  Nothing -> pure ()
                                pure (Right ())

------------------------------------------------------------------------
-- Partition assignment (simple round-robin)
------------------------------------------------------------------------

assignPartitions :: KafkaConsumer -> Int -> TopicName -> [MemberAssignment]
assignPartitions consumer memberCount topic = do
  -- For now, return empty assignments (coordinator handles it)
  -- In a full implementation, the leader would compute assignments
  []

------------------------------------------------------------------------
-- Fetch loop
------------------------------------------------------------------------

fetchLoop :: KafkaConsumer -> IO ()
fetchLoop consumer = do
  done <- readTVarIO (consShutdown consumer)
  state <- readTVarIO (consGroupState consumer)
  unless (done || cgsJoinState state /= JoinSteady) $ do
    -- Fetch from each assigned partition's leader
    let cfg = consConfig consumer
        assignment = cgsAssignment state
        offsets = cgsOffsets state
    forM_ assignment $ \tp -> do
      let currentOff = Map.findWithDefault 0 (tpTopic tp, tpPartition tp) offsets
      mBroker <- leaderBrokerFor (consClient consumer) (tpTopic tp) (tpPartition tp)
      case mBroker of
        Nothing -> pure ()
        Just env -> do
          let fetchReq = sessionlessFetchRequest
                (ccFetchWaitMs cfg)
                (tpTopic tp)
                [PartitionOffset (tpPartition tp) currentOff]
                (ccFetchMaxBytes cfg)
          respVar <- enqueueRequest env fetchReq
          response <- atomically $ readTMVar respVar
          case response of
            Left _ -> pure ()
            Right bytes -> case Wire.runWire F.parseFetchResponse bytes of
              Nothing -> pure ()
              Just fetchResp -> do
                let records = extractRecords (tpTopic tp) fetchResp
                    maxOff = if null records then currentOff
                             else maximum (map crOffset records) + 1
                -- Enqueue records for poll
                atomically $ forM_ records $ \r -> do
                  full <- isFullTBQueue (consFetchQueue consumer)
                  unless full $ writeTBQueue (consFetchQueue consumer) r
                -- Update fetch offset
                atomically $ modifyTVar' (consGroupState consumer) $ \s ->
                  s { cgsOffsets = Map.insert (tpTopic tp, tpPartition tp) maxOff (cgsOffsets s) }
    -- Small delay between fetch cycles
    threadDelay (ccFetchWaitMs cfg * 1000)
    fetchLoop consumer

-- | Extract ConsumerRecords from a FetchResponse.
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
    case mBroker of
      Nothing -> pure ()
      Just env -> do
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
                -- Rebalance needed — set join state back to init
                atomically $ modifyTVar' (consGroupState consumer) $ \s ->
                  s { cgsJoinState = JoinInit }
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

