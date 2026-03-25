{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.InitProducerId.Request
  ( initProducerIdRequest
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int16, Int32)
import qualified Data.ByteString.Lazy as BSL

import Kafka.Common
import Kafka.Internal.Writer

initProducerIdApiKey :: Int16
initProducerIdApiKey = 22

-- | InitProducerId v0 (pre-flexible).
-- Used to obtain a producer ID for idempotent/transactional producing.
-- transactionalId: Nothing for non-transactional idempotent producer.
-- transactionTimeoutMs: timeout for the transaction coordinator.
initProducerIdRequest :: Maybe ByteString -> Int32 -> BSL.ByteString
initProducerIdRequest txnId transactionTimeoutMs =
  buildRequest $
    int16 initProducerIdApiKey
    <> int16 0  -- api version 0
    <> int32 correlationId
    <> string clientId
    <> nullableString txnId
    <> int32 transactionTimeoutMs
