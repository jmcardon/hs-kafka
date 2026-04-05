{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.InitProducerId.Request
  ( initProducerIdRequest
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int16, Int32, Int64)
import qualified Data.ByteString.Lazy as BSL

import Kafka.Common
import Kafka.Internal.Writer

initProducerIdApiKey :: Int16
initProducerIdApiKey = 22

-- | InitProducerId v5 (flexible encoding, Kafka 4.2 compatible).
-- Used to obtain a producer ID for idempotent/transactional producing.
-- Request header v2 + compact body.
-- Flexible versions: 2+
-- v3 added ProducerId + ProducerEpoch fields.
-- v5 added TRANSACTION_ABORTABLE error code support.
--
-- transactionalId: Nothing for non-transactional idempotent producer.
-- transactionTimeoutMs: timeout for the transaction coordinator.
initProducerIdRequest :: Maybe ByteString -> Int32 -> BSL.ByteString
initProducerIdRequest txnId transactionTimeoutMs =
  buildRequest $
    -- Request header v2 (clientId is always legacy INT16 string per KIP-482)
    int16 initProducerIdApiKey
    <> int16 5  -- api version 5
    <> int32 correlationId
    <> string clientId
    <> taggedFields  -- header tagged fields
    -- InitProducerId v5 body
    <> compactNullableString txnId  -- transactional_id
    <> int32 transactionTimeoutMs   -- transaction_timeout_ms
    <> int64 (-1 :: Int64)          -- producer_id (-1 = new)
    <> int16 (-1 :: Int16)          -- producer_epoch (-1 = new)
    <> taggedFields                 -- body tagged fields
