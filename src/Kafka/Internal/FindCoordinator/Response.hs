module Kafka.Internal.FindCoordinator.Response
  ( FindCoordinatorResponse(..)
  , parseFindCoordinatorResponse
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int16, Int32)

import Kafka.Internal.Wire

data FindCoordinatorResponse = FindCoordinatorResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , errorCode :: {-# UNPACK #-} !Int16
  , errorMessage :: !(Maybe ByteString)
  , node_id :: {-# UNPACK #-} !Int32
  , host :: !ByteString
  , port :: {-# UNPACK #-} !Int32
  } deriving (Eq, Show)

parseFindCoordinatorResponse :: Wire FindCoordinatorResponse
parseFindCoordinatorResponse = do
  _correlationId <- int32
  FindCoordinatorResponse
    <$> int32
    <*> int16
    <*> legacyNullableString
    <*> int32
    <*> legacyString
    <*> int32
