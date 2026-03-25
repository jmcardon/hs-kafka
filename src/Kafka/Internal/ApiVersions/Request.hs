{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.ApiVersions.Request
  ( apiVersionsRequest
  ) where

import Data.Int (Int16, Int32)
import qualified Data.ByteString.Lazy as BSL

import Kafka.Common
import Kafka.Internal.Writer

apiVersionsApiKey :: Int16
apiVersionsApiKey = 18

-- | ApiVersions v0 (pre-flexible).
-- Sent on connection startup to discover broker's supported API versions.
-- The request body is empty — just the standard header.
apiVersionsRequest :: BSL.ByteString
apiVersionsRequest =
  buildRequest $
    int16 apiVersionsApiKey
    <> int16 0  -- api version 0
    <> int32 correlationId
    <> string clientId
