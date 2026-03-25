module Kafka.Internal.ShowDebug
  (ShowDebug(..)
  ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS8
import Data.Int (Int8, Int16, Int32, Int64)
import Data.List (intercalate)
import Data.Primitive.ByteArray (ByteArray)

import Kafka.Common

class ShowDebug a where
  showDebug :: a -> String

instance ShowDebug Int8 where
  showDebug = show

instance ShowDebug Int16 where
  showDebug = show

instance ShowDebug Int32 where
  showDebug = show

instance ShowDebug Int64 where
  showDebug = show

instance ShowDebug Int where
  showDebug = show

instance ShowDebug a => ShowDebug [a] where
  showDebug xs = "[" <> intercalate ", " (fmap showDebug xs) <> "]"

instance ShowDebug a => ShowDebug (Maybe a) where
  showDebug Nothing = "Nothing"
  showDebug (Just x) = "Just " <> showDebug x

instance ShowDebug ByteArray where
  showDebug = show

instance ShowDebug ByteString where
  showDebug = BS8.unpack

instance ShowDebug GroupName where
  showDebug (GroupName gid) = "GroupName(" <> showDebug gid <> ")"

instance ShowDebug Topic where
  showDebug (Topic topicName parts _) =
    "Topic (" <> showDebug topicName <> ", " <> show parts <> ")"

instance ShowDebug TopicName where
  showDebug (TopicName t) = showDebug t

instance ShowDebug GroupMember where
  showDebug (GroupMember gid mid) =
    "GroupMember (" <> showDebug gid <> ", " <> showDebug mid <> ")"

instance ShowDebug MemberAssignment where
  showDebug (MemberAssignment mid assignments) =
    "MemberAssignment (" <> showDebug mid <> ", " <> showDebug assignments <> ")"

instance ShowDebug TopicAssignment where
  showDebug (TopicAssignment topicName partitions) =
    "TopicAssignment (" <> showDebug topicName <> ", " <> show partitions <> ")"
