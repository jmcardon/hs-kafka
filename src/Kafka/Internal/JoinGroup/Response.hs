module Kafka.Internal.JoinGroup.Response
  ( JoinGroupResponse(..)
  , Member(..)
  , parseJoinGroupResponse
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int16, Int32)

import Kafka.Internal.Wire

data JoinGroupResponse = JoinGroupResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , errorCode :: {-# UNPACK #-} !Int16
  , generationId :: {-# UNPACK #-} !Int32
  , groupProtocol :: !ByteString
  , leaderId :: !ByteString
  , memberId :: !ByteString
  , members :: [Member]
  } deriving (Eq, Show)

data Member = Member
  { groupMemberId :: !ByteString
  , groupMemberMetadata :: !ByteString
  } deriving (Eq, Show)

parseJoinGroupResponse :: Wire JoinGroupResponse
parseJoinGroupResponse = do
  _correlationId <- int32
  JoinGroupResponse
    <$> int32 <*> int16 <*> int32
    <*> legacyString <*> legacyString <*> legacyString
    <*> legacyArray parseMember

parseMember :: Wire Member
parseMember = Member <$> legacyString <*> legacySizedBytes
{-# INLINE parseMember #-}
