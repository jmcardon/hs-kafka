{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.FindCoordinator.Request
  ( findCoordinatorRequest
  ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int8, Int16)

import Kafka.Common
import Kafka.Internal.Writer

findCoordinatorApiVersion :: Int16
findCoordinatorApiVersion = 2

findCoordinatorApiKey :: Int16
findCoordinatorApiKey = 10

findCoordinatorRequest :: ByteString -> Int8 -> BSL.ByteString
findCoordinatorRequest !key !keyType =
  buildRequest $
    int16 findCoordinatorApiKey
    <> int16 findCoordinatorApiVersion
    <> int32 correlationId
    <> string clientId
    <> bytearray key
    <> int8 keyType
