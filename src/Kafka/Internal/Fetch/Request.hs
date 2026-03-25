{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.Fetch.Request
  ( fetchRequest
  , sessionlessFetchRequest
  ) where

import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int8, Int16, Int32, Int64)

import Kafka.Common
import Kafka.Internal.Writer

fetchApiVersion :: Int16
fetchApiVersion = 10

fetchApiKey :: Int16
fetchApiKey = 1

data IsolationLevel
  = ReadUncommitted
  | ReadCommitted

isolationLevel :: IsolationLevel -> Int8
isolationLevel ReadUncommitted = 0
isolationLevel ReadCommitted = 1

sessionlessFetchRequest ::
     Int
  -> TopicName
  -> [PartitionOffset]
  -> Int32
  -> BSL.ByteString
sessionlessFetchRequest = fetchRequest 0 (-1)

defaultReplicaId :: Int32
defaultReplicaId = -1

defaultMinBytes :: Int32
defaultMinBytes = 1

defaultCurrentLeaderEpoch :: Int32
defaultCurrentLeaderEpoch = -1

defaultLogStartOffset :: Int64
defaultLogStartOffset = -1

fetchRequest ::
     Int32
  -> Int32
  -> Int
  -> TopicName
  -> [PartitionOffset]
  -> Int32
  -> BSL.ByteString
fetchRequest fetchSessionId fetchSessionEpoch timeout topic partitions maxBytes =
  buildRequest $
    -- common request headers
    int16 fetchApiKey
    <> int16 fetchApiVersion
    <> int32 correlationId
    <> string clientId
    -- fetch request
    <> int32 defaultReplicaId
    <> int32 (fromIntegral timeout) -- max_wait_time
    <> int32 defaultMinBytes
    <> int32 maxBytes
    <> int8 (isolationLevel ReadUncommitted)
    <> int32 fetchSessionId
    <> int32 fetchSessionEpoch
    <> int32 1 -- number of following topics

    <> topicName topic
    <> int32 (fromIntegral partitionCount) -- number of following partitions
    <> foldMap
        (\p -> int32 (partitionIndex p)
          <> int32 defaultCurrentLeaderEpoch
          <> int64 (partitionOffset p)
          <> int64 defaultLogStartOffset
          <> int32 maxBytes -- partition_max_bytes
        ) partitions
    <> int32 0
  where
    partitionCount = length partitions
