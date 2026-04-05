{-# language
    BangPatterns
  , CPP
  , MagicHash
  , PatternSynonyms
  , UnboxedSums
  , UnboxedTuples
  , ScopedTypeVariables
  #-}

-- for WORDS_BIGENDIAN
#include "MachDeps.h"

-- | Zero-allocation wire format parser for the Kafka protocol.
--
-- Operates directly on 'Addr#' from a 'ByteString', using raw pointer
-- arithmetic. The parser state is two machine words (position + end)
-- threaded through unboxed sums. No heap allocation in the hot path.
--
-- Design follows flatparse (Kovacs) and bytesmith (Andrew Martin):
--   - Parser is @ForeignPtrContents -> Addr# -> Addr# -> Result#@
--   - Fixed-size reads use native-endian load + byteSwap (single BSWAP insn)
--   - Failure propagation via unsafeCoerce# (avoids re-tagging the sum)
--   - ByteString slices are zero-copy (share the original ForeignPtr)
module Kafka.Internal.Wire
  ( -- * Parser type
    Wire(..)
  , Result#
  , pattern OK#
  , pattern Fail#
    -- * Running
  , runWire
    -- * Primitives: fixed-size big-endian reads
  , int8
  , int16
  , int32
  , int64
  , word8
  , word16
  , word32
  , word64
    -- * Primitives: variable-length
  , unsignedVarInt
  , signedVarInt
    -- * Kafka compact encoding (KIP-482)
  , compactString
  , compactNullableString
  , compactBytes
  , compactNullableBytes
  , compactArray
  , skipTaggedFields
    -- * Legacy encoding (pre-flexible)
  , legacyString
  , legacyArray
  , legacyNullableArray
  , legacyNullableString
  , legacySizedBytes
  , legacyNullableBytes
    -- * Combinators
  , skip
  , takeBytes
  , takeRest
  , remaining
  , ensure
  , eof
  , count
  , parseBool
  ) where

import GHC.Exts
import GHC.Int
import GHC.Word
import GHC.ForeignPtr (ForeignPtr(ForeignPtr), ForeignPtrContents)

import Data.ByteString (ByteString)
import qualified Data.ByteString.Internal as BSI

------------------------------------------------------------------------
-- Result type
------------------------------------------------------------------------

-- | Unboxed parser result: failure or success with value + new position.
type Result# a = (# (# #) | (# a, Addr# #) #)

pattern OK# :: a -> Addr# -> Result# a
pattern OK# a s = (# | (# a, s #) #)

pattern Fail# :: Result# a
pattern Fail# = (# (# #) | #)

{-# COMPLETE OK#, Fail# #-}

------------------------------------------------------------------------
-- Parser type
------------------------------------------------------------------------

-- | A wire format parser. Operates on raw 'Addr#' pointers into a
-- 'ByteString'. The 'ForeignPtrContents' keeps the buffer alive.
--
-- @pos@ = current read position, @end@ = one past last byte.
newtype Wire a = Wire
  { unWire :: ForeignPtrContents -> Addr# -> Addr# -> Result# a }

instance Functor Wire where
  fmap f (Wire g) = Wire $ \fpc pos end -> case g fpc pos end of
    OK# a pos' -> let !b = f a in OK# b pos'
    x          -> unsafeCoerce# x
  {-# INLINE fmap #-}

  a <$ Wire g = Wire $ \fpc pos end -> case g fpc pos end of
    OK# _ pos' -> OK# a pos'
    x          -> unsafeCoerce# x
  {-# INLINE (<$) #-}

instance Applicative Wire where
  pure !a = Wire $ \_ pos _ -> OK# a pos
  {-# INLINE pure #-}

  Wire ff <*> Wire fa = Wire $ \fpc pos end -> case ff fpc pos end of
    OK# f pos' -> case fa fpc pos' end of
      OK# a pos'' -> let !b = f a in OK# b pos''
      x           -> unsafeCoerce# x
    x -> unsafeCoerce# x
  {-# INLINE (<*>) #-}

  Wire fa *> Wire fb = Wire $ \fpc pos end -> case fa fpc pos end of
    OK# _ pos' -> fb fpc pos' end
    x          -> unsafeCoerce# x
  {-# INLINE (*>) #-}

  Wire fa <* Wire fb = Wire $ \fpc pos end -> case fa fpc pos end of
    OK# a pos' -> case fb fpc pos' end of
      OK# _ pos'' -> OK# a pos''
      x           -> unsafeCoerce# x
    x -> unsafeCoerce# x
  {-# INLINE (<*) #-}

instance Monad Wire where
  Wire fa >>= f = Wire $ \fpc pos end -> case fa fpc pos end of
    OK# a pos' -> unWire (f a) fpc pos' end
    x          -> unsafeCoerce# x
  {-# INLINE (>>=) #-}

  (>>) = (*>)
  {-# INLINE (>>) #-}

------------------------------------------------------------------------
-- Running
------------------------------------------------------------------------

-- | Run a parser on a 'ByteString'. Zero-copy — operates directly
-- on the ByteString's underlying memory.
runWire :: Wire a -> ByteString -> Maybe a
runWire (Wire f) bs =
  let !(BSI.BS (ForeignPtr addr fpc) len) = bs
      !(I# len#) = len
      end = plusAddr# addr len#
  in case f fpc addr end of
    OK# a _ -> Just a
    Fail#   -> Nothing
{-# INLINE runWire #-}

------------------------------------------------------------------------
-- Fixed-size big-endian reads
--
-- Strategy (from flatparse): use native-endian indexXXXOffAddr#, then
-- byteSwap on little-endian. Compiles to a single unaligned load +
-- BSWAP instruction on x86/ARM.
------------------------------------------------------------------------

word8 :: Wire Word8
word8 = Wire $ \_ pos end ->
  case eqAddr# pos end of
    1# -> Fail#
    _  -> OK# (W8# (indexWord8OffAddr# pos 0#)) (plusAddr# pos 1#)
{-# INLINE word8 #-}

int8 :: Wire Int8
int8 = Wire $ \_ pos end ->
  case eqAddr# pos end of
    1# -> Fail#
    _  -> OK# (I8# (indexInt8OffAddr# pos 0#)) (plusAddr# pos 1#)
{-# INLINE int8 #-}

-- | Read a big-endian 'Word16'.
word16 :: Wire Word16
word16 = Wire $ \_ pos end ->
  case 2# <=# minusAddr# end pos of
    0# -> Fail#
    _  -> let w = W16# (indexWord16OffAddr# pos 0#)
#if defined(WORDS_BIGENDIAN)
          in OK# w (plusAddr# pos 2#)
#else
          in OK# (byteSwap16 w) (plusAddr# pos 2#)
#endif
{-# INLINE word16 #-}

-- | Read a big-endian 'Int16'.
int16 :: Wire Int16
int16 = Wire $ \_ pos end ->
  case 2# <=# minusAddr# end pos of
    0# -> Fail#
    _  -> let w = W16# (indexWord16OffAddr# pos 0#)
#if defined(WORDS_BIGENDIAN)
          in OK# (word16ToInt16 w) (plusAddr# pos 2#)
#else
          in OK# (word16ToInt16 (byteSwap16 w)) (plusAddr# pos 2#)
#endif
{-# INLINE int16 #-}

-- | Read a big-endian 'Word32'.
word32 :: Wire Word32
word32 = Wire $ \_ pos end ->
  case 4# <=# minusAddr# end pos of
    0# -> Fail#
    _  -> let w = W32# (indexWord32OffAddr# pos 0#)
#if defined(WORDS_BIGENDIAN)
          in OK# w (plusAddr# pos 4#)
#else
          in OK# (byteSwap32 w) (plusAddr# pos 4#)
#endif
{-# INLINE word32 #-}

-- | Read a big-endian 'Int32'.
int32 :: Wire Int32
int32 = Wire $ \_ pos end ->
  case 4# <=# minusAddr# end pos of
    0# -> Fail#
    _  -> let w = W32# (indexWord32OffAddr# pos 0#)
#if defined(WORDS_BIGENDIAN)
          in OK# (word32ToInt32 w) (plusAddr# pos 4#)
#else
          in OK# (word32ToInt32 (byteSwap32 w)) (plusAddr# pos 4#)
#endif
{-# INLINE int32 #-}

-- | Read a big-endian 'Word64'.
word64 :: Wire Word64
word64 = Wire $ \_ pos end ->
  case 8# <=# minusAddr# end pos of
    0# -> Fail#
    _  -> let w = W64# (indexWord64OffAddr# pos 0#)
#if defined(WORDS_BIGENDIAN)
          in OK# w (plusAddr# pos 8#)
#else
          in OK# (byteSwap64 w) (plusAddr# pos 8#)
#endif
{-# INLINE word64 #-}

-- | Read a big-endian 'Int64'.
int64 :: Wire Int64
int64 = Wire $ \_ pos end ->
  case 8# <=# minusAddr# end pos of
    0# -> Fail#
    _  -> let w = W64# (indexWord64OffAddr# pos 0#)
#if defined(WORDS_BIGENDIAN)
          in OK# (word64ToInt64 w) (plusAddr# pos 8#)
#else
          in OK# (word64ToInt64 (byteSwap64 w)) (plusAddr# pos 8#)
#endif
{-# INLINE int64 #-}

-- Cast helpers (same bit pattern, different type)
word16ToInt16 :: Word16 -> Int16
word16ToInt16 (W16# w) = I16# (word16ToInt16# w)
{-# INLINE word16ToInt16 #-}

word32ToInt32 :: Word32 -> Int32
word32ToInt32 (W32# w) = I32# (word32ToInt32# w)
{-# INLINE word32ToInt32 #-}

word64ToInt64 :: Word64 -> Int64
word64ToInt64 (W64# w) = I64# (word64ToInt64# w)
{-# INLINE word64ToInt64 #-}

------------------------------------------------------------------------
-- Variable-length integers
------------------------------------------------------------------------

-- | Unsigned variable-length integer (LEB128).
unsignedVarInt :: Wire Int
unsignedVarInt = Wire $ \_ pos end -> goUVarInt pos end 0# 0#
{-# INLINE unsignedVarInt #-}

goUVarInt :: Addr# -> Addr# -> Int# -> Int# -> Result# Int
goUVarInt pos end acc shift =
  case eqAddr# pos end of
    1# -> Fail#
    _  -> let b = word2Int# (word8ToWord# (indexWord8OffAddr# pos 0#))
              val = acc `orI#` ((b `andI#` 0x7F#) `uncheckedIShiftL#` shift)
              pos' = plusAddr# pos 1#
          in case b `andI#` 0x80# of
              0# -> OK# (I# val) pos'
              _  -> goUVarInt pos' end val (shift +# 7#)

-- | Signed variable-length integer (zigzag decoded).
signedVarInt :: Wire Int
signedVarInt = Wire $ \fpc pos end -> case unWire unsignedVarInt fpc pos end of
  OK# (I# n) pos' -> OK# (I# (unZigZag# n)) pos'
  x                -> unsafeCoerce# x
{-# INLINE signedVarInt #-}

unZigZag# :: Int# -> Int#
unZigZag# n = (n `uncheckedIShiftRL#` 1#) `xorI#` negateInt# (n `andI#` 1#)
{-# INLINE unZigZag# #-}

------------------------------------------------------------------------
-- Kafka compact encoding (KIP-482)
--
-- Compact fields return ByteString (zero-copy slice into the input
-- buffer via ForeignPtr sharing).
------------------------------------------------------------------------

-- | Compact string: uvarint(length+1) then bytes.
-- Returns a zero-copy ByteString slice into the original input buffer.
compactString :: Wire ByteString
compactString = do
  n <- unsignedVarInt
  case n of
    0 -> pure BSI.empty
    _ -> takeBytes (n - 1)
{-# INLINE compactString #-}

-- | Compact nullable string. uvarint(0) = null.
compactNullableString :: Wire (Maybe ByteString)
compactNullableString = do
  n <- unsignedVarInt
  case n of
    0 -> pure Nothing
    _ -> Just <$> takeBytes (n - 1)
{-# INLINE compactNullableString #-}

-- | Compact bytes (same wire format as compact string).
compactBytes :: Wire ByteString
compactBytes = compactString
{-# INLINE compactBytes #-}

-- | Compact nullable bytes.
compactNullableBytes :: Wire (Maybe ByteString)
compactNullableBytes = compactNullableString
{-# INLINE compactNullableBytes #-}

-- | Compact array: uvarint(count+1) then elements.
compactArray :: Wire a -> Wire [a]
compactArray p = do
  n <- unsignedVarInt
  case n of
    0 -> pure []
    _ -> count (n - 1) p
{-# INLINE compactArray #-}

-- | Skip tagged fields section.
skipTaggedFields :: Wire ()
skipTaggedFields = do
  n <- unsignedVarInt
  goSkipTags n
{-# INLINE skipTaggedFields #-}

goSkipTags :: Int -> Wire ()
goSkipTags 0 = pure ()
goSkipTags n = do
  _ <- unsignedVarInt  -- tag
  sz <- unsignedVarInt -- size
  skip sz
  goSkipTags (n - 1)

------------------------------------------------------------------------
-- Combinators
------------------------------------------------------------------------

-- | Skip n bytes.
skip :: Int -> Wire ()
skip (I# n) = Wire $ \_ pos end ->
  case n <=# minusAddr# end pos of
    0# -> Fail#
    _  -> OK# () (plusAddr# pos n)
{-# INLINE skip #-}

-- | Take n bytes as a zero-copy 'ByteString' slice.
-- Shares the underlying buffer — no memcpy.
takeBytes :: Int -> Wire ByteString
takeBytes (I# n) = Wire $ \fpc pos end ->
  case n <=# minusAddr# end pos of
    0# -> Fail#
    _  -> OK# (BSI.BS (ForeignPtr pos fpc) (I# n)) (plusAddr# pos n)
{-# INLINE takeBytes #-}

-- | Get the number of remaining bytes.
remaining :: Wire Int
remaining = Wire $ \_ pos end -> OK# (I# (minusAddr# end pos)) pos
{-# INLINE remaining #-}

-- | Ensure at least n bytes available without consuming.
ensure :: Int -> Wire ()
ensure (I# n) = Wire $ \_ pos end ->
  case n <=# minusAddr# end pos of
    0# -> Fail#
    _  -> OK# () pos
{-# INLINE ensure #-}

-- | Succeed only if all input consumed.
eof :: Wire ()
eof = Wire $ \_ pos end ->
  case eqAddr# pos end of
    1# -> OK# () pos
    _  -> Fail#
{-# INLINE eof #-}

-- | Take all remaining bytes as a zero-copy ByteString.
takeRest :: Wire ByteString
takeRest = Wire $ \fpc pos end ->
  let n = minusAddr# end pos
  in OK# (BSI.BS (ForeignPtr pos fpc) (I# n)) end
{-# INLINE takeRest #-}

-- | Parse a Bool (0 = False, nonzero = True).
parseBool :: Wire Bool
parseBool = do
  b <- word8
  pure (b /= 0)
{-# INLINE parseBool #-}

-- | Parse exactly n items.
count :: Int -> Wire a -> Wire [a]
count n p = go n []
  where
    go 0 !acc = pure (reverse acc)
    go i !acc = do
      a <- p
      go (i - 1) (a : acc)
{-# INLINE count #-}

------------------------------------------------------------------------
-- Legacy encoding (pre-flexible versions)
------------------------------------------------------------------------

-- | Legacy string: INT16 length + bytes. Returns zero-copy ByteString.
legacyString :: Wire ByteString
legacyString = do
  len <- int16
  takeBytes (fromIntegral len)
{-# INLINE legacyString #-}

-- | Legacy nullable string: INT16 length (-1 = null).
legacyNullableString :: Wire (Maybe ByteString)
legacyNullableString = do
  len <- int16
  if len < 0
    then pure Nothing
    else Just <$> takeBytes (fromIntegral len)
{-# INLINE legacyNullableString #-}

-- | Legacy array: INT32 count + elements.
legacyArray :: Wire a -> Wire [a]
legacyArray p = do
  n <- int32
  count (fromIntegral n) p
{-# INLINE legacyArray #-}

-- | Legacy nullable array: INT32 count (-1 or 0 = empty).
legacyNullableArray :: Wire a -> Wire [a]
legacyNullableArray p = do
  n <- int32
  if n <= 0 then pure [] else count (fromIntegral n) p
{-# INLINE legacyNullableArray #-}

-- | Legacy sized bytes: INT32 length + bytes.
legacySizedBytes :: Wire ByteString
legacySizedBytes = do
  len <- int32
  takeBytes (fromIntegral len)
{-# INLINE legacySizedBytes #-}

-- | Legacy nullable bytes: INT32 length (-1 = null). If present, parse with p.
legacyNullableBytes :: Wire a -> Wire (Maybe a)
legacyNullableBytes p = do
  len <- int32
  if len <= 0 then pure Nothing else Just <$> p
{-# INLINE legacyNullableBytes #-}
