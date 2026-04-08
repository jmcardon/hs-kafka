module Kafka.Internal.SyncGroup.Response
  ( SyncGroupResponse(..)
  , SyncMemberAssignment(..)
  , SyncTopicAssignment(..)
  , parseSyncGroupResponse
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int16, Int32)

import Kafka.Common (TopicName(..))
import Kafka.Internal.Wire

data SyncGroupResponse = SyncGroupResponse
  { throttleTimeMs :: !Int32
  , errorCode :: !Int16
  , memberAssignment :: !(Maybe SyncMemberAssignment)
  } deriving (Eq, Show)

data SyncMemberAssignment = SyncMemberAssignment
  { version :: !Int16
  , partitionAssignments :: [SyncTopicAssignment]
  , userData :: !ByteString
  } deriving (Eq, Show)

data SyncTopicAssignment = SyncTopicAssignment
  { topic :: !TopicName
  , partitions :: [Int32]
  } deriving (Eq, Show)

parseSyncGroupResponse :: Wire SyncGroupResponse
parseSyncGroupResponse = do
  _correlationId <- int32
  SyncGroupResponse
    <$> int32 <*> int16
    <*> legacyNullableBytes parseMemberAssignment

parseMemberAssignment :: Wire SyncMemberAssignment
parseMemberAssignment = SyncMemberAssignment
  <$> int16
  <*> legacyArray parseTopicPartitions
  <*> takeRest

parseTopicPartitions :: Wire SyncTopicAssignment
parseTopicPartitions = SyncTopicAssignment
  <$> (TopicName <$> legacyString)
  <*> legacyArray int32
{-# INLINE parseTopicPartitions #-}
