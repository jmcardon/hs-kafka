{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.Metadata.Request
  ( metadataRequest
  ) where

import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int16)

import Kafka.Common
import Kafka.Internal.Writer

metadataApiVersion :: Int16
metadataApiVersion = 12

metadataApiKey :: Int16
metadataApiKey = 3

-- | Metadata v12 request (flexible encoding, Kafka 4.2 compatible).
--
-- Request header v2 + compact body.
-- v12: topics use COMPACT_NULLABLE_ARRAY with TopicId (UUID) + Name (nullable string).
-- We send TopicId as null UUID (16 zero bytes) and Name as the topic name.
metadataRequest ::
     TopicName
  -> AutoCreateTopic
  -> BSL.ByteString
metadataRequest (TopicName tn) autoCreate =
  buildRequest $
    -- Request header v2 (clientId is always legacy INT16 string per KIP-482)
    int16 metadataApiKey
    <> int16 metadataApiVersion
    <> int32 correlationId
    <> string clientId
    <> taggedFields  -- header tagged fields
    -- Metadata v12 body
    <> unsignedVarInt 2  -- compact array: 1 topic (count+1 = 2)
    -- MetadataRequestTopic
    <> int64 0 <> int64 0  -- TopicId: null UUID (16 zero bytes)
    <> compactNullableString (Just tn)  -- Name
    <> taggedFields  -- topic tagged fields
    -- Body fields
    <> bool (case autoCreate of Create -> True; NeverCreate -> False)
    <> bool False  -- include_topic_authorized_operations
    <> taggedFields  -- body tagged fields
