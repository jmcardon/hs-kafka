-- | Kafka protocol encoding using proto3-wire's reverse builder (BuildR).
--
-- Legacy (pre-flexible) functions: string, bytearray, array, mapArray
-- Compact (flexible) functions:    compactString, compactNullableString,
--                                  compactBytes, compactNullableBytes,
--                                  compactArray, compactNullableArray,
--                                  unsignedVarInt, taggedFields
module Kafka.Internal.Writer
  ( BuildR
  , toLazyByteString
  , buildRequest
  -- Legacy encoding (pre-flexible versions)
  , mapArray
  , topicName
  , string
  , bytearray
  , array
  , nullableString
  , size32
  , bool
  , bs
  , int8
  , int16
  , int32
  , int64
  -- Compact encoding (flexible versions, KIP-482)
  , unsignedVarInt
  , compactString
  , compactNullableString
  , compactBytes
  , compactNullableBytes
  , compactArray
  , compactNullableArray
  , taggedFields
  ) where

import Data.Bits ((.&.), (.|.), shiftR)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int8, Int16, Int32, Int64)
import Proto3.Wire.Reverse (BuildR, toLazyByteString)
import qualified Proto3.Wire.Reverse as R

import Kafka.Common (TopicName(..))

size32 :: ByteString -> Int32
size32 = fromIntegral . BS.length

mapArray :: Foldable t => t a -> (a -> BuildR) -> BuildR
mapArray xs f = int32 (fromIntegral (length xs)) <> foldMap f xs

bytearray :: ByteString -> BuildR
bytearray s = int16 (fromIntegral (BS.length s)) <> R.byteString s

string :: ByteString -> BuildR
string = bytearray

topicName :: TopicName -> BuildR
topicName (TopicName tn) = string tn

array :: [BuildR] -> BuildR
array src = int32 (fromIntegral (length src)) <> mconcat src

bool :: Bool -> BuildR
bool b = int8 (if b then 1 else 0)

bs :: ByteString -> BuildR
bs = R.byteString

int8 :: Int8 -> BuildR
int8 = R.int8
{-# INLINE int8 #-}

int16 :: Int16 -> BuildR
int16 = R.int16BE
{-# INLINE int16 #-}

int32 :: Int32 -> BuildR
int32 = R.int32BE
{-# INLINE int32 #-}

int64 :: Int64 -> BuildR
int64 = R.int64BE
{-# INLINE int64 #-}

-- | Legacy nullable string: int16 length (-1 = null).
nullableString :: Maybe ByteString -> BuildR
nullableString Nothing  = int16 (-1)
nullableString (Just s) = string s

-- | Build a complete Kafka request: 4-byte size prefix + body.
buildRequest :: BuildR -> BSL.ByteString
buildRequest bodyBuilder =
  let body = toLazyByteString bodyBuilder
      size = fromIntegral (BSL.length body) :: Int32
  in toLazyByteString (int32 size) <> body

------------------------------------------------------------------------
-- Compact encoding (flexible versions, KIP-482)
------------------------------------------------------------------------

-- | Unsigned variable-length integer (LEB128).
-- Used for compact string/array/bytes lengths and tagged field headers.
unsignedVarInt :: Int -> BuildR
unsignedVarInt n
  | n < 0     = error "unsignedVarInt: negative"
  | n < 0x80  = R.word8 (fromIntegral n)
  | otherwise = R.word8 (fromIntegral (n .&. 0x7F .|. 0x80))
                <> unsignedVarInt (n `shiftR` 7)

-- | Compact string: unsigned_varint(length + 1) then bytes.
-- varint(0) = null. varint(1) = empty string.
compactString :: ByteString -> BuildR
compactString s = unsignedVarInt (BS.length s + 1) <> R.byteString s

-- | Compact nullable string.
compactNullableString :: Maybe ByteString -> BuildR
compactNullableString Nothing  = unsignedVarInt 0
compactNullableString (Just s) = compactString s

-- | Compact bytes: unsigned_varint(length + 1) then bytes.
compactBytes :: ByteString -> BuildR
compactBytes = compactString

-- | Compact nullable bytes.
compactNullableBytes :: Maybe ByteString -> BuildR
compactNullableBytes = compactNullableString

-- | Compact array: unsigned_varint(count + 1) then elements.
-- varint(0) = null array. varint(1) = empty array.
compactArray :: [BuildR] -> BuildR
compactArray xs = unsignedVarInt (length xs + 1) <> mconcat xs

-- | Compact nullable array.
compactNullableArray :: Maybe [BuildR] -> BuildR
compactNullableArray Nothing   = unsignedVarInt 0
compactNullableArray (Just xs) = compactArray xs

-- | Tagged fields terminator. Appended at the end of every struct
-- in flexible-version messages. Encodes 0 (no tagged fields).
taggedFields :: BuildR
taggedFields = unsignedVarInt 0
