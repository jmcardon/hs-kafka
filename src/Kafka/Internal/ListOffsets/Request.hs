{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.ListOffsets.Request
  ( listOffsetsRequest
  ) where

import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int8, Int16, Int32, Int64)

import Kafka.Common
import Kafka.Internal.Writer

listOffsetsApiVersion :: Int16
listOffsetsApiVersion = 5

listOffsetsApiKey :: Int16
listOffsetsApiKey = 2

defaultReplicaId :: Int32
defaultReplicaId = -1

data IsolationLevel
  = ReadUncommitted
  | ReadCommitted

isolationLevel :: IsolationLevel -> Int8
isolationLevel ReadUncommitted = 0
isolationLevel ReadCommitted = 1

defaultIsolationLevel :: Int8
defaultIsolationLevel = isolationLevel ReadUncommitted

defaultCurrentLeaderEpoch :: Int32
defaultCurrentLeaderEpoch = -1

kafkaTimestamp :: KafkaTimestamp -> Int64
kafkaTimestamp Latest = -1
kafkaTimestamp Earliest = -2
kafkaTimestamp (At n) = n

listOffsetsRequest ::
     TopicName
  -> [Int32]
  -> KafkaTimestamp
  -> BSL.ByteString
listOffsetsRequest topic partitions timestamp =
  buildRequest $
    -- common request headers
    int16 listOffsetsApiKey
    <> int16 listOffsetsApiVersion
    <> int32 correlationId
    <> string clientId
    -- listoffsets request
    <> int32 defaultReplicaId
    <> int8 defaultIsolationLevel
    <> int32 1 -- number of following topics

    <> topicName topic
    <> array
        ( fmap
          (\p -> int32 p
            <> int32 defaultCurrentLeaderEpoch
            <> int64 (kafkaTimestamp timestamp)
          )
          partitions
        )
