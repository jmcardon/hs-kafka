{-# language
    BangPatterns
  , DerivingStrategies
  , OverloadedStrings
  #-}

-- | Typed configuration for the Kafka client.
--
-- Mirrors librdkafka configuration keys where applicable.
-- See: librdkafka/CONFIGURATION.md
module Kafka.Internal.Config
  ( BrokerAddress(..)
  , ClientConfig(..)
  , Compression(..)
  , Acknowledgments(..)
  , LogLevel(..)
  , defaultConfig
  , acksToInt16
  , compressionAttribute
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int16)
import Network.Socket (HostName, PortNumber)

-- | A broker address: hostname (or IP string) + port.
data BrokerAddress = BrokerAddress
  { baHost :: !HostName
  , baPort :: !PortNumber
  } deriving (Eq, Ord, Show)

-- | Client configuration. All time values are in milliseconds
-- to match Kafka convention.
data ClientConfig = ClientConfig
  { ccBootstrap        :: ![BrokerAddress]
    -- ^ Bootstrap broker addresses.
  , ccClientId         :: !ByteString
    -- ^ Client identifier sent in every request header.
  , ccAcks             :: !Acknowledgments
    -- ^ Producer acknowledgment level.
  , ccLingerMs         :: {-# UNPACK #-} !Int
    -- ^ How long to wait for more messages before sending a batch.
    -- Maps to librdkafka queue.buffering.max.ms / linger.ms.
    -- Default: 5
  , ccBatchSize        :: {-# UNPACK #-} !Int
    -- ^ Maximum batch size in bytes.
    -- Maps to librdkafka batch.size.
    -- Default: 1000000
  , ccBatchNumMessages :: {-# UNPACK #-} !Int
    -- ^ Maximum number of messages per batch.
    -- Maps to librdkafka batch.num.messages.
    -- Default: 10000
  , ccRequestTimeoutMs :: {-# UNPACK #-} !Int
    -- ^ Timeout for broker responses.
    -- Default: 30000
  , ccReconnectMs      :: {-# UNPACK #-} !Int
    -- ^ Initial reconnection backoff in ms.
    -- Maps to librdkafka reconnect.backoff.ms.
    -- Default: 100
  , ccReconnectMaxMs   :: {-# UNPACK #-} !Int
    -- ^ Maximum reconnection backoff in ms.
    -- Maps to librdkafka reconnect.backoff.max.ms.
    -- Default: 10000
  , ccCompression      :: !Compression
    -- ^ Compression codec for produce batches.
    -- Default: NoCompression
  , ccQueueSize        :: {-# UNPACK #-} !Int
    -- ^ Maximum buffered messages per partition queue (backpressure).
    -- Maps to librdkafka queue.buffering.max.messages.
    -- Default: 100000
  , ccMaxInFlight      :: {-# UNPACK #-} !Int
    -- ^ Maximum number of unacknowledged produce requests per broker.
    -- Maps to librdkafka max.in.flight.requests.per.connection.
    -- Default: 5
  , ccIdempotent       :: !Bool
    -- ^ Enable idempotent producer (requires acks=all).
    -- When True, obtains a ProducerId via InitProducerId and tracks
    -- per-partition sequence numbers for exactly-once delivery.
    -- Default: False
  , ccRetries          :: {-# UNPACK #-} !Int
    -- ^ Number of retries for retriable errors.
    -- Default: 3
  , ccMetadataMaxAgeMs :: {-# UNPACK #-} !Int
    -- ^ Maximum age of metadata before forced refresh.
    -- Maps to librdkafka topic.metadata.refresh.interval.ms.
    -- Default: 300000 (5 minutes). Set to -1 to disable.
  , ccMessageTimeoutMs :: {-# UNPACK #-} !Int
    -- ^ Maximum time a message can wait in the producer queue.
    -- Maps to librdkafka message.timeout.ms.
    -- Default: 300000 (5 minutes). Set to 0 for infinite.
  , ccLogCallback     :: !(Maybe (LogLevel -> String -> IO ()))
    -- ^ Log callback. Called for internal log messages.
  , ccErrorCallback   :: !(Maybe (String -> IO ()))
    -- ^ Error callback. Called for non-message errors (connection failures, etc).
  , ccStatsCallback   :: !(Maybe (String -> IO ()))
    -- ^ Statistics callback. Called periodically with JSON stats.
  }

-- | Log severity levels.
data LogLevel = LogDebug | LogInfo | LogWarn | LogError
  deriving (Eq, Ord, Show)

-- | Compression codec. Stored in bits 0-2 of record batch attributes.
data Compression
  = NoCompression  -- ^ 0
  | Gzip           -- ^ 1
  | Snappy         -- ^ 2
  | Lz4            -- ^ 3
  | Zstd           -- ^ 4
  deriving stock (Eq, Show)

-- | Producer acknowledgment level.
data Acknowledgments
  = AcksNone       -- ^ acks=0: no acknowledgment
  | AcksLeader     -- ^ acks=1: leader only
  | AcksAll        -- ^ acks=-1: all in-sync replicas
  deriving stock (Eq, Show)

acksToInt16 :: Acknowledgments -> Int16
acksToInt16 AcksNone   = 0
acksToInt16 AcksLeader = 1
acksToInt16 AcksAll    = -1

-- | Record batch attribute bits for compression codec.
compressionAttribute :: Compression -> Int16
compressionAttribute NoCompression = 0
compressionAttribute Gzip          = 1
compressionAttribute Snappy        = 2
compressionAttribute Lz4           = 3
compressionAttribute Zstd          = 4

defaultConfig :: ClientConfig
defaultConfig = ClientConfig
  { ccBootstrap        = []
  , ccClientId         = "kafka-native"
  , ccAcks             = AcksAll
  , ccLingerMs         = 5
  , ccBatchSize        = 1000000
  , ccBatchNumMessages = 10000
  , ccRequestTimeoutMs = 30000
  , ccReconnectMs      = 100
  , ccReconnectMaxMs   = 10000
  , ccCompression      = NoCompression
  , ccQueueSize        = 100000
  , ccMaxInFlight      = 5
  , ccIdempotent       = False
  , ccRetries          = 3
  , ccMetadataMaxAgeMs = 300000
  , ccMessageTimeoutMs = 300000
  , ccLogCallback     = Nothing
  , ccErrorCallback   = Nothing
  , ccStatsCallback   = Nothing
  }
