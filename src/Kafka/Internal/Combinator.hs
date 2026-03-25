{-# language
    BangPatterns
  , LambdaCase
  , RankNTypes
  , ScopedTypeVariables
  #-}

module Kafka.Internal.Combinator
  ( Parser
  -- Legacy decoding (pre-flexible versions)
  , array
  , count
  , bool
  , bytearray
  , topicName
  , int8
  , int16
  , int32
  , int64
  , nullableArray
  , sizedBytes
  , nullableByteArray
  , nullableByteArrayVar
  , nullableBytes
  , nullableSequence
  , takeByteArray
  , varInt
  , (Smith.<?>)
  -- Compact decoding (flexible versions, KIP-482)
  , unsignedVarInt
  , compactString
  , compactNullableString
  , compactBytes
  , compactNullableBytes
  , compactArray
  , skipTaggedFields
  , byteArrayToByteString
  )
  where

import Control.Applicative (liftA2)
import Control.Monad (replicateM)
import Data.Bits (testBit, clearBit, (.&.), (.|.), shiftL)
import Data.Int (Int8, Int16, Int32, Int64)
import Data.ByteString (ByteString)
import Data.Primitive.ByteArray (ByteArray, sizeofByteArray)

import qualified Data.Bytes as B
import Data.Bytes.Types (Bytes(Bytes))
import qualified Data.Bytes.Parser as Smith
import qualified Data.Bytes.Parser.BigEndian as Smith

import Kafka.Common

type Parser a = forall s. Smith.Parser String s a

int8 :: String -> Smith.Parser String s Int8
int8 = Smith.int8

int16 :: String -> Smith.Parser String s Int16
int16 = Smith.int16

int32 :: String -> Smith.Parser String s Int32
int32 = Smith.int32

int64 :: String -> Smith.Parser String s Int64
int64 = Smith.int64

bool :: String -> Smith.Parser String s Bool
bool e = Smith.word8 e >>= \case
  0 -> pure False
  _ -> pure True

count :: Integral i => i -> Smith.Parser String s a -> Smith.Parser String s [a]
count n p = replicateM (fromIntegral n) p
{-# inlineable count #-}

{-
c_array :: forall arr a. (Contiguous arr, Element arr a)
  => Parser a
  -> Parser (arr a)
c_array p = do
  len <- fromIntegral <$> int32 "c_array: len"
  marr :: Mutable arr s a <- Smith.effect (C.new len)
  let go :: Int -> Smith.Parser String s ()
      go !ix = if ix < len
        then do
          (a :: a) <- p <?> ("c_array: element " <> show ix)
          Smith.effect (C.write marr ix a)
          go (ix + 1)
        else do
          pure ()
  go 0
  Smith.effect (C.unsafeFreeze marr)
{-# inlineable c_array #-}
-}

--nullableArray :: forall arr a. (Contiguous arr, Element arr a)
array :: Parser a -> Smith.Parser String s [a]
array p = do
  arraySize <- int32 "array: arraySize"
  count arraySize p

nullableArray :: Parser a -> Smith.Parser String s [a]
nullableArray p = do
  arraySize <- int32 "nullableArray: array size"
  if arraySize <= 0
    then pure []
    else count arraySize p

bytearray :: Parser ByteArray
bytearray = do
  len <- int16 "bytearray: len"
  bytes <- Smith.take ("take " <> show len) (fromIntegral len)
  pure (B.toByteArray bytes)

topicName :: Parser TopicName
topicName = TopicName . byteArrayToByteString <$> bytearray

-- | Convert a ByteArray to a strict ByteString (one memcpy).
byteArrayToByteString :: ByteArray -> ByteString
byteArrayToByteString ba =
  B.toByteString (Bytes ba 0 (sizeofByteArray ba))

sizedBytes :: Parser ByteArray
sizedBytes = do
  len <- int32 "sizedBytes: len"
  bytes <- Smith.take ("take " <> show len) (fromIntegral len)
  pure (B.toByteArray bytes)

nullableByteArray :: Parser (Maybe ByteArray)
nullableByteArray = do
  len <- int16 "nullableByteArray: len"
  if len < 0
    then pure Nothing
    else do
      bytes <- Smith.take ("take " <> show len) (fromIntegral len)
      pure (Just (B.toByteArray bytes))

nullableByteArrayVar :: Parser (Maybe ByteArray)
nullableByteArrayVar = do
  len <- varInt
  if len < 0
    then pure Nothing
    else do
      bytes <- Smith.take ("take " <> show len) (fromIntegral len)
      pure (Just (B.toByteArray bytes))

nullableBytes :: Parser a -> Smith.Parser String s (Maybe a)
nullableBytes p = do
  len <- int32 "nullableBytes: len"
  if len <= 0
    then pure Nothing
    else Just <$> p

takeByteArray :: Parser ByteArray
takeByteArray = do
  bytes <- Smith.remaining
  pure (B.toByteArray bytes)

many :: Parser a -> Smith.Parser String s [a]
many v = many_v
  where
     many_v = some_v `Smith.orElse` pure []
     some_v = liftA2 (:) v many_v

nullableSequence :: Parser a -> Smith.Parser String s (Maybe [a])
nullableSequence p = do
  len <- int32 "nullableSequence: len"
  bytes <- Smith.take ("take " <> show len) (fromIntegral len)
  if len <= 0
    then pure Nothing
    else case Smith.parseBytes (many p) bytes of
      Smith.Failure _ -> pure Nothing
      Smith.Success (Smith.Slice _ _ as) -> pure (Just as)

varInt :: Parser Int
varInt = fmap unZigZag (go 1)
  where
    go :: Int -> Smith.Parser String s Int
    go !n = do
      b <- fromIntegral <$> Smith.any ("varInt: " <> show n)
      if testBit b 7
        then do
          rest <- go (n * 128)
          pure (clearBit b 7 * n + rest)
        else do
          pure (b * n)

unZigZag :: Int -> Int
unZigZag n
  | even n = n `div` 2
  | otherwise = (-1) * ((n + 1) `div` 2)

------------------------------------------------------------------------
-- Compact decoding (flexible versions, KIP-482)
------------------------------------------------------------------------

-- | Unsigned variable-length integer (LEB128).
-- Different from varInt which does zigzag decoding for signed ints.
unsignedVarInt :: Smith.Parser String s Int
unsignedVarInt = go 0 0
  where
    go :: Int -> Int -> Smith.Parser String s Int
    go !acc !shift = do
      b <- fromIntegral <$> Smith.any "unsignedVarInt"
      let val = acc .|. ((b .&. 0x7F) `shiftL` shift)
      if testBit (b :: Int) 7
        then go val (shift + 7)
        else pure val

-- | Compact string: unsigned_varint(length + 1) then bytes.
-- varint(0) = null (error here — use compactNullableString for nullable).
-- varint(1) = empty string.
compactString :: Smith.Parser String s ByteString
compactString = do
  n <- unsignedVarInt
  if n <= 0
    then Smith.fail "compactString: unexpected null"
    else do
      bytes <- Smith.take "compactString" (n - 1)
      pure (B.toByteString bytes)

-- | Compact nullable string. varint(0) = null.
compactNullableString :: Smith.Parser String s (Maybe ByteString)
compactNullableString = do
  n <- unsignedVarInt
  if n == 0
    then pure Nothing
    else do
      bytes <- Smith.take "compactNullableString" (n - 1)
      pure (Just (B.toByteString bytes))

-- | Compact bytes (same wire format as compact string).
compactBytes :: Smith.Parser String s ByteString
compactBytes = compactString

-- | Compact nullable bytes.
compactNullableBytes :: Smith.Parser String s (Maybe ByteString)
compactNullableBytes = compactNullableString

-- | Compact array: unsigned_varint(count + 1) then elements.
-- varint(0) = null (treated as empty). varint(1) = empty array.
compactArray :: Smith.Parser String s a -> Smith.Parser String s [a]
compactArray p = do
  n <- unsignedVarInt
  if n <= 0
    then pure []
    else count (n - 1) p

-- | Skip over tagged fields section (read and discard).
-- Each tagged field: varint tag, varint size, then size bytes of data.
-- The section starts with a varint count of tagged fields.
skipTaggedFields :: Smith.Parser String s ()
skipTaggedFields = do
  numFields <- unsignedVarInt
  go numFields
  where
    go 0 = pure ()
    go !n = do
      _tag  <- unsignedVarInt
      size  <- unsignedVarInt
      _     <- Smith.take "taggedField" size
      go (n - 1)
