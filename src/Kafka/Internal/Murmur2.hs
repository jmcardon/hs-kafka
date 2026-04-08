{-# language BangPatterns #-}

-- | MurmurHash2 implementation compatible with Java Kafka producer.
--
-- Ported from librdkafka's rdmurmur2.c (MurmurHashNeutral2 variant).
-- Endian- and alignment-neutral. Uses seed 0x9747b28c.
--
-- The result is a positive Int32 (last bit set to 0), matching
-- the Java producer's toPositive(murmur2(key)) convention.
module Kafka.Internal.Murmur2
  ( murmur2
  ) where

import Data.Bits (xor, shiftR, shiftL, (.&.), (.|.), complement)
import Data.ByteString (ByteString)
import Data.Int (Int32)
import Data.Word (Word32)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU

-- | Compute MurmurHash2 of a ByteString, compatible with Kafka's
-- Java producer default partitioner. Returns a non-negative Int32.
murmur2 :: ByteString -> Int32
murmur2 bs = fromIntegral (h3 .&. 0x7fffffff)
  where
    !len = BS.length bs
    !seed = 0x9747b28c :: Word32
    !m = 0x5bd1e995 :: Word32
    !r = 24 :: Int

    -- Process 4-byte chunks (unaligned/neutral variant)
    !h1 = processChunks (seed `xor` fromIntegral len) 0
    -- Process remaining bytes
    !h2 = processTail h1 (len .&. complement 3)
    -- Finalize
    !h3 = let a = h2 `xor` shiftR h2 13
              b = a * m
          in b `xor` shiftR b 15

    processChunks :: Word32 -> Int -> Word32
    processChunks !h !off
      | off + 4 > len = h
      | otherwise =
          let !b0 = fromIntegral (BSU.unsafeIndex bs off) :: Word32
              !b1 = fromIntegral (BSU.unsafeIndex bs (off + 1)) :: Word32
              !b2 = fromIntegral (BSU.unsafeIndex bs (off + 2)) :: Word32
              !b3 = fromIntegral (BSU.unsafeIndex bs (off + 3)) :: Word32
              !k0 = b0 .|. (b1 `shiftL` 8) .|. (b2 `shiftL` 16) .|. (b3 `shiftL` 24)
              !k1 = k0 * m
              !k2 = k1 `xor` shiftR k1 r
              !k3 = k2 * m
              !h' = (h * m) `xor` k3
          in processChunks h' (off + 4)

    processTail :: Word32 -> Int -> Word32
    processTail !h !off = case len - off of
      3 -> let !h' = h `xor` (fromIntegral (BSU.unsafeIndex bs (off + 2)) `shiftL` 16)
               !h'' = h' `xor` (fromIntegral (BSU.unsafeIndex bs (off + 1)) `shiftL` 8)
               !h''' = h'' `xor` fromIntegral (BSU.unsafeIndex bs off)
           in h''' * m
      2 -> let !h' = h `xor` (fromIntegral (BSU.unsafeIndex bs (off + 1)) `shiftL` 8)
               !h'' = h' `xor` fromIntegral (BSU.unsafeIndex bs off)
           in h'' * m
      1 -> (h `xor` fromIntegral (BSU.unsafeIndex bs off)) * m
      _ -> h
