{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.OffsetCommit.Request
  ( offsetCommitRequest
  ) where

import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int16, Int32, Int64)

import Kafka.Common
import Kafka.Internal.Writer

serializePartition :: PartitionOffset -> BuildR
serializePartition a =
  int32 (partitionIndex a)
  <> int64 (partitionOffset a)
  <> int32 (-1) -- leader epoch
  <> int16 (-1) -- metadata

offsetCommitApiKey :: Int16
offsetCommitApiKey = 8

offsetCommitApiVersion :: Int16
offsetCommitApiVersion = 6

offsetCommitRequest ::
     TopicName
  -> [PartitionOffset]
  -> GroupMember
  -> GenerationId
  -> BSL.ByteString
offsetCommitRequest topic offs groupMember generationId =
  let
    GroupMember (GroupName gid) mid = groupMember
    GenerationId genId = generationId
  in
    buildRequest $
      int16 offsetCommitApiKey
      <> int16 offsetCommitApiVersion
      <> int32 correlationId
      <> string clientId
      <> string gid
      <> int32 genId
      <> maybe
          (int16 0)
          (\m -> bytearray m)
          mid
      <> int32 1 -- 1 topic
      <> topicName topic
      <> int32 (fromIntegral $ length offs)
      <> foldMap serializePartition offs
