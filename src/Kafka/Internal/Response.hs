{-# language LambdaCase #-}

module Kafka.Internal.Response
  ( getKafkaResponse
  , getResponseSizeHeader
  , parseResponse
  ) where

import Control.Exception (try, IOException)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Network.Socket.ByteString as NBS
import Control.Concurrent.STM (TVar)

import Kafka.Common
import Kafka.Internal.Wire (Wire)
import qualified Kafka.Internal.Wire as Wire

-- | Receive exactly n bytes from a socket, or fail.
recvExact :: Kafka -> Int -> IO (Either KafkaException ByteString)
recvExact kafka n = do
  result <- try (go n [])
  case result of
    Left (e :: IOException) -> pure (Left (KafkaIOError (show e)))
    Right bs -> pure (Right bs)
  where
    go 0 acc = pure (BS.concat (reverse acc))
    go remaining acc = do
      chunk <- NBS.recv (getSocket kafka) remaining
      if BS.null chunk
        then ioError (userError "connection closed by peer")
        else go (remaining - BS.length chunk) (chunk : acc)

-- | Read a full Kafka response (size header + body) as a ByteString.
getKafkaResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException ByteString)
getKafkaResponse kafka _interrupt =
  getResponseSizeHeader kafka _interrupt >>= \case
    Right byteCount -> recvExact kafka byteCount
    Left e -> pure (Left e)

getResponseSizeHeader ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException Int)
getResponseSizeHeader kafka _interrupt = do
  result <- recvExact kafka 4
  case result of
    Left e -> pure (Left e)
    Right bs ->
      let b0 = fromIntegral (BS.index bs 0) :: Int
          b1 = fromIntegral (BS.index bs 1) :: Int
          b2 = fromIntegral (BS.index bs 2) :: Int
          b3 = fromIntegral (BS.index bs 3) :: Int
      in pure (Right (b0 * 16777216 + b1 * 65536 + b2 * 256 + b3))

-- | Read a response from the socket and parse it with a Wire parser.
-- Used by old Consumer-path code that reads directly from a socket.
parseResponse :: Wire a -> Kafka -> TVar Bool -> IO (Either KafkaException a)
parseResponse parser kafka interrupt =
  getKafkaResponse kafka interrupt >>= \case
    Left err -> pure (Left err)
    Right bs -> case Wire.runWire parser bs of
      Nothing -> pure (Left (KafkaParseException "wire parse failed"))
      Just a  -> pure (Right a)
