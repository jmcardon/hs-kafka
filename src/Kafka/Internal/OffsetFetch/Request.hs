{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.OffsetFetch.Request where

import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int16, Int32)

import Kafka.Common
import Kafka.Internal.Writer

offsetFetchApiKey :: Int16
offsetFetchApiKey = 9

offsetFetchApiVersion :: Int16
offsetFetchApiVersion = 5

offsetFetchRequest ::
     GroupMember
  -> TopicName
  -> [Int32]
  -> BSL.ByteString
offsetFetchRequest (GroupMember (GroupName gid) _) tn offs =
  buildRequest $
    int16 offsetFetchApiKey
    <> int16 offsetFetchApiVersion
    <> int32 correlationId
    <> string clientId
    <> string gid
    <> int32 1 -- 1 topic
    <> topicName tn
    <> int32 (fromIntegral (length offs))
    <> foldMap int32 offs
