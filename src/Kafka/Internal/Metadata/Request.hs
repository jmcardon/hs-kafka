{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.Metadata.Request
  ( metadataRequest
  ) where

import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int16, Int32)

import Kafka.Common
import Kafka.Internal.Writer

metadataApiVersion :: Int16
metadataApiVersion = 7

metadataApiKey :: Int16
metadataApiKey = 3

metadataRequest ::
     TopicName
  -> AutoCreateTopic
  -> BSL.ByteString
metadataRequest tn autoCreate =
  buildRequest $
    int16 metadataApiKey
    <> int16 metadataApiVersion
    <> int32 correlationId
    <> string clientId
    <> int32 1
    <> topicName tn
    <> bool (case autoCreate of Create -> True; NeverCreate -> False)
