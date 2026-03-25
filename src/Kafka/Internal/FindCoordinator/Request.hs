{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.FindCoordinator.Request
  ( findCoordinatorRequest
  ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BSL
import Data.Bytes.Types (Bytes(Bytes))
import qualified Data.Bytes
import Data.Int (Int8, Int16, Int32)
import Data.Primitive.ByteArray (ByteArray, sizeofByteArray)

import Kafka.Common
import Kafka.Internal.Writer

findCoordinatorApiVersion :: Int16
findCoordinatorApiVersion = 2

findCoordinatorApiKey :: Int16
findCoordinatorApiKey = 10

baToBS :: ByteArray -> ByteString
baToBS ba = Data.Bytes.toByteString (Bytes ba 0 (sizeofByteArray ba))

findCoordinatorRequest ::
     ByteArray -- key, a.k.a. group name
  -> Int8
  -> BSL.ByteString
findCoordinatorRequest !key !keyType =
  buildRequest $
    int16 findCoordinatorApiKey
    <> int16 findCoordinatorApiVersion
    <> int32 correlationId
    <> string clientId
    <> bytearray (baToBS key)
    <> int8 keyType
