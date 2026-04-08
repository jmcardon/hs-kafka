{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.ApiVersions.Request
  ( apiVersionsRequest
  ) where

import Data.Int (Int16)
import qualified Data.ByteString.Lazy as BSL

import Kafka.Common
import Kafka.Internal.Writer

apiVersionsApiKey :: Int16
apiVersionsApiKey = 18

-- | ApiVersions v0 (legacy header).
-- Sent on connection startup to discover broker's supported API versions.
--
-- We intentionally use v0 for the handshake because:
-- 1. All Kafka brokers (0.10+) support v0
-- 2. The mock cluster (librdkafka) reliably parses v0
-- 3. v3 flexible header can cause issues with older brokers
-- 4. The response tells us what versions the broker actually supports
--
-- After learning the broker's supported versions from the v0 response,
-- subsequent requests use the appropriate flexible versions.
apiVersionsRequest :: BSL.ByteString
apiVersionsRequest =
  buildRequest $
    int16 apiVersionsApiKey
    <> int16 0  -- api version 0
    <> int32 correlationId
    <> string clientId
