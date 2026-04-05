module Kafka.Internal.ApiVersions.Response
  ( ApiVersionsResponse(..)
  , ApiVersionEntry(..)
  , parseApiVersionsResponse
  , parseApiVersionsResponseV3
  ) where

import Data.Int (Int16, Int32)

import Kafka.Internal.Wire

data ApiVersionsResponse = ApiVersionsResponse
  { avErrorCode    :: {-# UNPACK #-} !Int16
  , avApiVersions  :: [ApiVersionEntry]
  , avThrottleTime :: {-# UNPACK #-} !Int32
  } deriving (Eq, Show)

data ApiVersionEntry = ApiVersionEntry
  { aveApiKey     :: {-# UNPACK #-} !Int16
  , aveMinVersion :: {-# UNPACK #-} !Int16
  , aveMaxVersion :: {-# UNPACK #-} !Int16
  } deriving (Eq, Show)

-- | Parse ApiVersions v0-v2 response (legacy encoding).
-- Response header v0: just correlation_id (no tagged fields).
parseApiVersionsResponse :: Wire ApiVersionsResponse
parseApiVersionsResponse = do
  _correlationId <- int32
  errCode <- int16
  arrayLen <- int32
  entries <- count (fromIntegral arrayLen) parseApiVersionEntry
  pure (ApiVersionsResponse errCode entries 0)

parseApiVersionEntry :: Wire ApiVersionEntry
parseApiVersionEntry = ApiVersionEntry <$> int16 <*> int16 <*> int16
{-# INLINE parseApiVersionEntry #-}

-- | Parse ApiVersions v3+ response (flexible/compact encoding).
-- Response header v0 (ApiVersions is special: no header tagged fields even in v3).
parseApiVersionsResponseV3 :: Wire ApiVersionsResponse
parseApiVersionsResponseV3 = do
  _correlationId <- int32
  errCode <- int16
  entries <- compactArray parseApiVersionEntryV3
  throttle <- int32
  skipTaggedFields
  pure (ApiVersionsResponse errCode entries throttle)

parseApiVersionEntryV3 :: Wire ApiVersionEntry
parseApiVersionEntryV3 = do
  entry <- ApiVersionEntry <$> int16 <*> int16 <*> int16
  skipTaggedFields
  pure entry
{-# INLINE parseApiVersionEntryV3 #-}
