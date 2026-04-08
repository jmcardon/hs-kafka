{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.Fetch.Request
  ( fetchRequest
  , sessionlessFetchRequest
  , fetchRequestV12
  , sessionlessFetchRequestV12
  ) where

import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int8, Int16, Int32, Int64)

import Kafka.Common
import Kafka.Internal.Writer

------------------------------------------------------------------------
-- Fetch v10 (legacy, pre-flexible)
------------------------------------------------------------------------

fetchApiKey :: Int16
fetchApiKey = 1

fetchApiVersionLegacy :: Int16
fetchApiVersionLegacy = 10

data IsolationLevel = ReadUncommitted | ReadCommitted

isolationLevelInt :: IsolationLevel -> Int8
isolationLevelInt ReadUncommitted = 0
isolationLevelInt ReadCommitted = 1

sessionlessFetchRequest ::
     Int -> TopicName -> [PartitionOffset] -> Int32 -> BSL.ByteString
sessionlessFetchRequest = fetchRequest 0 (-1)

fetchRequest ::
     Int32 -> Int32 -> Int -> TopicName -> [PartitionOffset] -> Int32 -> BSL.ByteString
fetchRequest fetchSessionId fetchSessionEpoch timeout topic partitions maxBytes =
  buildRequest $
    int16 fetchApiKey
    <> int16 fetchApiVersionLegacy
    <> int32 correlationId
    <> string clientId
    <> int32 (-1)  -- replicaId
    <> int32 (fromIntegral timeout)
    <> int32 1     -- minBytes
    <> int32 maxBytes
    <> int8 (isolationLevelInt ReadUncommitted)
    <> int32 fetchSessionId
    <> int32 fetchSessionEpoch
    <> int32 1  -- 1 topic
    <> topicName topic
    <> int32 (fromIntegral (length partitions))
    <> foldMap
        (\p -> int32 (partitionIndex p)
          <> int32 (-1)  -- currentLeaderEpoch
          <> int64 (partitionOffset p)
          <> int64 (-1)  -- logStartOffset
          <> int32 maxBytes
        ) partitions
    <> int32 0  -- forgotten topics count

------------------------------------------------------------------------
-- Fetch v12 (flexible, KIP-482)
------------------------------------------------------------------------

fetchApiVersionFlex :: Int16
fetchApiVersionFlex = 12

sessionlessFetchRequestV12 ::
     Int -> TopicName -> [PartitionOffset] -> Int32 -> BSL.ByteString
sessionlessFetchRequestV12 = fetchRequestV12 0 (-1)

fetchRequestV12 ::
     Int32 -> Int32 -> Int -> TopicName -> [PartitionOffset] -> Int32 -> BSL.ByteString
fetchRequestV12 fetchSessionId fetchSessionEpoch timeout (TopicName tn) partitions maxBytes =
  let body =
        -- Request header v2
        int16 fetchApiKey
        <> int16 fetchApiVersionFlex
        <> int32 correlationId
        <> string clientId
        <> taggedFields  -- header tagged fields
        -- Fetch body
        <> int32 (-1)  -- replicaId
        <> int32 (fromIntegral timeout)
        <> int32 1     -- minBytes
        <> int32 maxBytes
        <> int8 (isolationLevelInt ReadUncommitted)
        <> int32 fetchSessionId
        <> int32 fetchSessionEpoch
        -- Topics: compact array (count+1)
        <> unsignedVarInt 2  -- 1 topic
        <> compactString tn
        -- Partitions: compact array (count+1)
        <> unsignedVarInt (length partitions + 1)
        <> foldMap
            (\p -> int32 (partitionIndex p)
              <> int32 (-1)  -- currentLeaderEpoch
              <> int64 (partitionOffset p)
              <> int64 (-1)  -- lastFetchedEpoch (v12+)
              <> int64 (-1)  -- logStartOffset
              <> int32 maxBytes
              <> taggedFields  -- partition tagged fields
            ) partitions
        <> taggedFields  -- topic tagged fields
        -- Forgotten topics: compact array (empty)
        <> unsignedVarInt 1  -- 0 forgotten topics
        -- RackId: compact string (empty)
        <> compactString ""
        <> taggedFields  -- body tagged fields
      bodyBytes = toLazyByteString body
      bodySize = fromIntegral (BSL.length bodyBytes) :: Int32
  in toLazyByteString (int32 bodySize) <> bodyBytes
