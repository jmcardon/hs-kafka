module Kafka.Internal.ApiVersions.Response
  ( ApiVersionsResponse(..)
  , ApiVersionEntry(..)
  , parseApiVersionsResponse
  , getApiVersionsResponse
  ) where

import Control.Concurrent.STM (TVar)
import Data.Int (Int16, Int32)
import System.IO (Handle)

import Kafka.Common
import Kafka.Internal.Combinator
import Kafka.Internal.Response

data ApiVersionsResponse = ApiVersionsResponse
  { avErrorCode   :: {-# UNPACK #-} !Int16
  , avApiVersions :: [ApiVersionEntry]
  } deriving (Eq, Show)

data ApiVersionEntry = ApiVersionEntry
  { aveApiKey     :: {-# UNPACK #-} !Int16
  , aveMinVersion :: {-# UNPACK #-} !Int16
  , aveMaxVersion :: {-# UNPACK #-} !Int16
  } deriving (Eq, Show)

parseApiVersionsResponse :: Parser ApiVersionsResponse
parseApiVersionsResponse = do
  _correlationId <- int32 "correlation id"
  ApiVersionsResponse
    <$> int16 "error code"
    <*> array parseApiVersionEntry

parseApiVersionEntry :: Parser ApiVersionEntry
parseApiVersionEntry = ApiVersionEntry
  <$> int16 "api key"
  <*> int16 "min version"
  <*> int16 "max version"

getApiVersionsResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String ApiVersionsResponse))
getApiVersionsResponse = fromKafkaResponse parseApiVersionsResponse
