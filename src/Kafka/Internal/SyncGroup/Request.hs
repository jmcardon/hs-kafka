{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.SyncGroup.Request
  ( syncGroupRequest
  ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int16, Int32)

import Kafka.Common
import Kafka.Internal.Writer

syncGroupApiVersion :: Int16
syncGroupApiVersion = 2

syncGroupApiKey :: Int16
syncGroupApiKey = 14

defaultAssignmentData :: MemberAssignment -> BuildR
defaultAssignmentData assignment =
  let
    assn = mconcat
      [ int16 0 -- version
      , mapArray (assignedTopics assignment)
          (\top ->
            let
              atn = assignedTopicName top
              aps = assignedPartitions top
            in topicName atn
              <> mapArray aps int32
          )
      , int32 0 -- userdata bytes length
      ]
    assnBytes = BSL.toStrict (toLazyByteString assn)
  in bytearray memId
    <> int32 (fromIntegral (BS.length assnBytes))
    <> assn
  where
    memId = assignedMemberId assignment

syncGroupRequest ::
     GroupMember
  -> GenerationId
  -> [MemberAssignment]
  -> BSL.ByteString
syncGroupRequest (GroupMember (GroupName gid) mid) (GenerationId genId) assignments =
  buildRequest $
    int16 syncGroupApiKey
    <> int16 syncGroupApiVersion
    <> int32 correlationId
    <> string clientId
    <> string gid
    <> int32 genId
    <> maybe (int16 0) (\m -> bytearray m) mid
    <> mapArray assignments defaultAssignmentData
