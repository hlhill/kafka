<?php

namespace EasySwoole\Kafka1\Protocol;

use EasySwoole\Kafka1\Exception\Exception;
use EasySwoole\Kafka1\Exception\NotSupported;
use EasySwoole\Kafka1\Exception\Protocol as ProtocolException;

class Protocol
{
    /**
     * protocol request code
     */
    public const PRODUCE_REQUEST = 0;
    public const FETCH_REQUEST = 1;
    public const OFFSET_REQUEST = 2;
    public const METADATA_REQUEST = 3;
    public const OFFSET_COMMIT_REQUEST = 8;
    public const OFFSET_FETCH_REQUEST = 9;
    public const GROUP_COORDINATOR_REQUEST = 10;
    public const JOIN_GROUP_REQUEST = 11;
    public const HEART_BEAT_REQUEST = 12;
    public const LEAVE_GROUP_REQUEST = 13;
    public const SYNC_GROUP_REQUEST = 14;
    public const DESCRIBE_GROUPS_REQUEST = 15;
    public const LIST_GROUPS_REQUEST = 16;
    public const SASL_HAND_SHAKE_REQUEST = 17;
    public const API_VERSIONS_REQUEST = 18;
    public const CREATE_TOPICS_REQUEST = 19;
    public const DELETE_TOPICS_REQUEST = 20;
    public const DELETE_RECORDS_REQUEST = 21;
    public const INIT_PRODUCER_ID_REQUEST = 22;
    public const OFFSET_FOR_LEADER_EPOCH_REQUEST = 23;
    public const ADD_PARTITIONS_TO_TXN_REQUEST = 24;
    public const ADD_OFFSETS_TO_TXN_REQUEST = 25;
    public const END_TXN_REQUEST = 26;
    public const WRITE_TXN_MARKERS_REQUEST = 27;
    public const TXN_OFFSET_COMMIT_REQUEST = 28;
    public const DESCRIBE_ACLS_REQUEST = 29;
    public const CREATE_ACLS_REQUEST = 30;
    public const DELETE_ACLS_REQUEST = 31;
    public const DESCRIBE_CONFIGS_REQUEST = 32;
    public const ALTER_CONFIGS_REQUEST = 33;
    public const ALTER_REPLICA_LOG_DIRS_REQUEST = 34;
    public const DESCRIBE_LOG_DIRS_REQUEST = 35;
    public const SASL_AUTHENTICATE_REQUEST = 36;
    public const CREATE_PARTITIONS_REQUEST = 37;
    public const CREATE_DELEGATION_TOKEN_REQUEST = 38;
    public const RENEW_DELEGATION_TOKEN_REQUEST = 39;
    public const EXPIRE_DELEGATION_TOKEN_REQUEST = 40;
    public const DESCRIBE_DELEGATION_TOKEN_REQUEST = 41;
    public const DELETE_GROUPS_REQUEST = 42;
    public const ELECT_PREFERRED_LEADERS_REQUEST = 43;
    public const INCREMENTAL_ALTER_CONFIGS_REQUEST = 44;

    // protocol error code
    public const NO_ERROR = 0;
    public const ERROR_UNKNOWN = -1;
    public const OFFSET_OUT_OF_RANGE = 1;
    public const INVALID_MESSAGE = 2;
    public const UNKNOWN_TOPIC_OR_PARTITION = 3;
    public const INVALID_MESSAGE_SIZE = 4;
    public const LEADER_NOT_AVAILABLE = 5;
    public const NOT_LEADER_FOR_PARTITION = 6;
    public const REQUEST_TIMED_OUT = 7;
    public const BROKER_NOT_AVAILABLE = 8;
    public const REPLICA_NOT_AVAILABLE = 9;
    public const MESSAGE_SIZE_TOO_LARGE = 10;
    public const STALE_CONTROLLER_EPOCH = 11;
    public const OFFSET_METADATA_TOO_LARGE = 12;
    public const GROUP_LOAD_IN_PROGRESS = 14;
    public const GROUP_COORDINATOR_NOT_AVAILABLE = 15;
    public const NOT_COORDINATOR_FOR_GROUP = 16;
    public const INVALID_TOPIC = 17;
    public const RECORD_LIST_TOO_LARGE = 18;
    public const NOT_ENOUGH_REPLICAS = 19;
    public const NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20;
    public const INVALID_REQUIRED_ACKS = 21;
    public const ILLEGAL_GENERATION = 22;
    public const INCONSISTENT_GROUP_PROTOCOL = 23;
    public const INVALID_GROUP_ID = 24;
    public const UNKNOWN_MEMBER_ID = 25;
    public const INVALID_SESSION_TIMEOUT = 26;
    public const REBALANCE_IN_PROGRESS = 27;
    public const INVALID_COMMIT_OFFSET_SIZE = 28;
    public const TOPIC_AUTHORIZATION_FAILED = 29;
    public const GROUP_AUTHORIZATION_FAILED = 30;
    public const CLUSTER_AUTHORIZATION_FAILED = 31;
    public const INVALID_TIMESTAMP = 32;
    public const UNSUPPORTED_SASL_MECHANISM = 33;
    public const ILLEGAL_SASL_STATE = 34;
    public const UNSUPPORTED_VERSION = 35;
    public const TOPIC_ALREADY_EXISTS = 36;
    public const INVALID_PARTITIONS = 37;
    public const INVALID_REPLICATION_FACTOR = 38;
    public const INVALID_REPLICA_ASSIGNMENT = 39;
    public const INVALID_CONFIG = 40;
    public const NOT_CONTROLLER = 41;
    public const INVALID_REQUEST = 42;
    public const UNSUPPORTED_FOR_MESSAGE_FORMAT = 43;
    public const POLICY_VIOLATION = 44;
    public const OUT_OF_ORDER_SEQUENCE_NUMBER = 45;
    public const DUPLICATE_SEQUENCE_NUMBER = 46;
    public const INVALID_PRODUCER_EPOCH = 47;
    public const INVALID_TXN_STATE = 48;
    public const INVALID_PRODUCER_ID_MAPPING = 49;
    public const INVALID_TRANSACTION_TIMEOUT = 50;
    public const CONCURRENT_TRANSACTIONS = 51;
    public const TRANSACTION_COORDINATOR_FENCED = 52;
    public const TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53;
    public const SECURITY_DISABLED = 54;
    public const OPERATION_NOT_ATTEMPTED = 55;
    public const KAFKA_STORAGE_ERROR = 56;
    public const LOG_DIR_NOT_FOUND = 57;
    public const SASL_AUTHENTICATION_FAILED = 58;
    public const UNKNOWN_PRODUCER_ID = 59;
    public const REASSIGNMENT_IN_PROGRESS = 60;
    public const DELEGATION_TOKEN_AUTH_DISABLED = 61;
    public const DELEGATION_TOKEN_NOT_FOUND = 62;
    public const DELEGATION_TOKEN_OWNER_MISMATCH = 63;
    public const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64;
    public const DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65;
    public const DELEGATION_TOKEN_EXPIRED = 66;
    public const INVALID_PRINCIPAL_TYPE = 67;
    public const NON_EMPTY_GROUP = 68;
    public const GROUP_ID_NOT_FOUND = 69;
    public const FETCH_SESSION_ID_NOT_FOUND = 70;
    public const INVALID_FETCH_SESSION_EPOCH = 71;
    public const LISTENER_NOT_FOUND = 72;
    public const TOPIC_DELETION_DISABLED = 73;
    public const FENCED_LEADER_EPOCH = 74;
    public const UNKNOWN_LEADER_EPOCH = 75;
    public const UNSUPPORTED_COMPRESSION_TYPE = 76;
    public const STALE_BROKER_EPOCH = 77;
    public const OFFSET_NOT_AVAILABLE = 78;
    public const MEMBER_ID_REQUIRED = 79;
    public const PREFERRED_LEADER_NOT_AVAILABLE = 80;
    public const GROUP_MAX_SIZE_REACHED = 81;
    public const FENCED_INSTANCE_ID = 82;

    private const PROTOCOL_ERROR_MAP = [
        0 => 'No error--it worked!',
        -1 => 'An unexpected server error',
        1 => 'The requested offset is outside the range of offsets maintained by the server for the given topic/partition.',
        2 => 'This indicates that a message contents does not match its CRC',
        3 => 'This request is for a topic or partition that does not exist on this broker.',
        4 => 'The message has a negative size',
        5 => 'This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes',
        6 => 'This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.',
        7 => 'This error is thrown if the request exceeds the user-specified time limit in the request.',
        8 => 'This is not a client facing error and is used only internally by intra-cluster broker communication.',
        9 => 'The replica is not available for the requested topic-partition',
        10 => 'The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.',
        11 => 'Internal error code for broker-to-broker communication.',
        12 => 'If you specify a string larger than configured maximum for offset metadata',
        13 => 'The server disconnected before a response was received.',
        14 => 'The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).',
        15 => 'The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.',
        16 => 'The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.',
        17 => 'The request attempted to perform an operation on an invalid topic.',
        18 => 'The request included message batch larger than the configured segment size on the server.',
        19 => 'Messages are rejected since there are fewer in-sync replicas than required.',
        20 => 'Messages are written to the log, but to fewer in-sync replicas than required.',
        21 => 'Produce request specified an invalid value for required acks.',
        22 => 'Specified group generation id is not valid.',
        23 => 'The group member\'s supported protocols are incompatible with those of existing members.',
        24 => 'The configured groupId is invalid',
        25 => 'The coordinator is not aware of this member.',
        26 => 'The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).',
        27 => 'The group is rebalancing, so a rejoin is needed.',
        28 => 'The committing offset data size is not valid',
        29 => 'Topic authorization failed.',
        30 => 'Group authorization failed.',
        31 => 'Cluster authorization failed.',
        32 => 'The timestamp of the message is out of acceptable range.',
        33 => 'The broker does not support the requested SASL mechanism.',
        34 => 'Request is not valid given the current SASL state.',
        35 => 'The version of API is not supported.',
        36 => 'Topic with this name already exists.',
        37 => 'Number of partitions is invalid.',
        38 => 'Replication-factor is invalid.',
        39 => 'Replica assignment is invalid.',
        40 => 'Configuration is invalid.',
        41 => 'This is not the correct controller for this cluster.',
        42 => 'This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.',
        43 => 'The message format version on the broker does not support the request.',
        44 => 'Request parameters do not satisfy the configured policy.',
        45 => 'The broker received an out of order sequence number',
        46 => 'The broker received a duplicate sequence number',
        47 => 'Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer\'s transaction has been expired by the broker.',
        48 => 'The producer attempted a transactional operation in an invalid state',
        49 => 'The producer attempted to use a producer id which is not currently assigned to its transactional id',
        50 => 'The transaction timeout is larger than the maximum value allowed by the broker (as configured by max.transaction.timeout.ms).',
        51 => 'The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing',
        52 => 'Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer',
        53 => 'Transactional Id authorization failed',
        54 => 'Security features are disabled.',
        55 => 'The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.',
        56 => 'Disk error when trying to access log file on the disk.',
        57 => 'The user-specified log directory is not found in the broker config.',
        58 => 'SASL Authentication failed.',
        59 => 'This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question',
        60 => 'A partition reassignment is in progress.',
        61 => 'Delegation Token feature is not enabled.',
        62 => 'Delegation Token is not found on server.',
        63 => 'Specified Principal is not valid Owner/Renewer.',
        64 => 'Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.',
        65 => 'Delegation Token authorization failed.',
        66 => 'Delegation Token is expired.',
        67 => 'Supplied principalType is not supported.',
        68 => 'The group is not empty.',
        69 => 'The group id does not exist.',
        70 => 'The fetch session ID was not found.',
        71 => 'The fetch session epoch is invalid.',
        72 => 'There is no listener on the leader broker that matches the listener on which metadata request was processed.',
        73 => 'Topic deletion is disabled.',
        74 => 'The leader epoch in the request is older than the epoch on the broker.',
        75 => 'The leader epoch in the request is newer than the epoch on the broker.',
        76 => 'The requesting client does not support the compression type of given partition.',
        77 => 'Broker epoch has changed.',
        78 => 'The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.',
        79 => 'The group member needs to have a valid member id before actually entering a consumer group.',
        80 => 'The preferred leader was not available',
        81 => 'Consumer group The consumer group has reached its max size. already has the configured maximum number of members.',
        82 => 'The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.',
    ];

    private static $objMap = [
        self::PRODUCE_REQUEST           => Produce::class,
        self::METADATA_REQUEST          => Metadata::class,
        self::FETCH_REQUEST             => Fetch::class,
        self::OFFSET_REQUEST            => Offset::class,
        self::OFFSET_COMMIT_REQUEST     => CommitOffset::class,
        self::OFFSET_FETCH_REQUEST      => FetchOffset::class,
        self::GROUP_COORDINATOR_REQUEST => GroupCoordinator::class,
        self::JOIN_GROUP_REQUEST        => JoinGroup::class,
        self::HEART_BEAT_REQUEST        => HeartBeat::class,
        self::LEAVE_GROUP_REQUEST       => LeaveGroup::class,
        self::SYNC_GROUP_REQUEST        => SyncGroup::class,
        self::DESCRIBE_GROUPS_REQUEST   => DescribeGroups::class,
        self::LIST_GROUPS_REQUEST       => ListGroup::class,
        self::SASL_HAND_SHAKE_REQUEST   => SaslHandShake::class,
        self::API_VERSIONS_REQUEST      => ApiVersions::class,
    ];

    /**
     * @var array
     */
    private $objList = [];

    /**
     * @param $key
     * @param $payloads
     * @return mixed
     * @throws ProtocolException
     */
    public function encode($key, $payloads)
    {
        if (!isset(self::$objMap[$key])) {
            throw new ProtocolException('undefined protocol type : '. $key);
        }

        if (!isset($this->objList[$key])) {
            $this->objList[$key] = new self::$objMap[$key]();
        }

        return $this->objList[$key]->encoding($payloads);
    }

    /**
     * @param $key
     * @param $data
     * @return mixed
     * @throws ProtocolException
     */
    public function decode($key, $data)
    {
        if (!isset(self::$objMap[$key])) {
            throw new ProtocolException('undefined protocol type : '. $key);
        }

        if (!isset($this->objList[$key])) {
            $this->objList[$key] = new self::$objMap[$key]();
        }

        return $this->objList[$key]->decoding($data);
    }

    /**
     * @param array $payloads
     * @return string
     * @throws Exception
     */
    protected function encoding(array $payloads): string
    {
        foreach ($payloads as $topic) {
            if (! is_string($topic)) {
                throw new ProtocolException(
                    "request metadata topic array have invalid value."
                );
            }
        }

        $header = $this->requestHeader('Easyswoole-kafka', self::METADATA_REQUEST, self::METADATA_REQUEST);
        $data   = self::encodeArray($payloads, [$this, 'encodeString'], self::PACK_INT16);
        $data   = self::encodeString($header . $data, self::PACK_INT32);
        return $data;
    }

    /**
     * @param string $data
     * @return array
     * @throws Exception
     */
    protected function decoding(string $data): array
    {
        $offset       = 0;
        $brokerRet    = $this->decodeArray(substr($data, $offset), [$this, 'metaBroker']);
        $offset      += $brokerRet['length'];
        $topicMetaRet = $this->decodeArray(substr($data, $offset), [$this, 'metaTopicMetaData']);
        $offset      += $topicMetaRet['length'];

        $result = [
            'brokers' => $brokerRet['data'],
            'topics'  => $topicMetaRet['data'],
        ];
        return $result;
    }

    /**
     * @param int $errCode
     * @return string
     */
    public static function getError(int $errCode): string
    {
        if (! isset(self::PROTOCOL_ERROR_MAP[$errCode])) {
            return "Unknow error ({$errCode})";
        }

        return self::PROTOCOL_ERROR_MAP[$errCode];
    }


    /**
     *  Default kafka broker verion
     */
    public const DEFAULT_BROKER_VERION = '0.9.0.0';

    /**
     *  Kafka server protocol version0
     */
    public const API_VERSION0 = 0;

    /**
     *  Kafka server protocol version 1
     */
    public const API_VERSION1 = 1;

    /**
     *  Kafka server protocol version 2
     */
    public const API_VERSION2 = 2;

    /**
     * use encode message, This is a version id used to allow backwards
     * compatible evolution of the message binary format.
     */
    public const MESSAGE_MAGIC_VERSION0 = 0;

    /**
     * use encode message, This is a version id used to allow backwards
     * compatible evolution of the message binary format.
     */
    public const MESSAGE_MAGIC_VERSION1 = 1;

    public const MESSAGE_MAGIC_VERSION2 = 2;

    /**
     * message no compression
     */
    public const COMPRESSION_NONE = 0;

    /**
     * Message using gzip compression
     */
    public const COMPRESSION_GZIP = 1;

    /**
     * Message using Snappy compression
     */
    public const COMPRESSION_SNAPPY = 2;

    /**
     *  pack int32 type
     */
    public const PACK_INT32 = 0;

    /**
     * pack int16 type
     */
    public const PACK_INT16 = 1;

    // unpack/pack bit
    public const BIT_B64 = 'N2';

    public const BIT_B32 = 'N';

    public const BIT_B16 = 'n';

    public const BIT_B16_SIGNED = 's';

    public const BIT_B8 = 'C';

    /**
     * @var string
     */
    protected $version = self::DEFAULT_BROKER_VERION;

    private static $isLittleEndianSystem;

    /**
     * Protocol constructor.
     * @param string $version
     */
    public function __construct(string $version = self::DEFAULT_BROKER_VERION)
    {
        $this->version = $version;
    }

    /**
     * Converts a signed short (16 bits) from little endian to big endian.
     *
     * @param int[] $bits
     *
     * @return int[]
     */
    public static function convertSignedShortFromLittleEndianToBigEndian(array $bits): array
    {
        $convert = function (int $bit): int {
            $lsb = $bit & 0xff;
            $msb = $bit >> 8 & 0xff;
            $bit = $lsb << 8 | $msb;

            if ($bit >= 32768) {
                $bit -= 65536;
            }

            return $bit;
        };

        return array_map($convert, $bits);
    }

    /**
     * @param string $clientId
     * @param int    $correlationId
     * @param int    $apiKey
     * @return string
     * @throws NotSupported
     */
    public function requestHeader(string $clientId, int $correlationId, int $apiKey): string
    {
        // int16 -- apiKey, int16 -- apiVersion, int32 correlationId, string clientId
        $version        = (string)$this->getApiVersion($apiKey);
        $apiKey         = self::pack(self::BIT_B16, (string)$apiKey);
        $apiVersion     = self::pack(self::BIT_B16, $version);
        $correlationId  = self::pack(self::BIT_B32, (string)$correlationId);
        $clientId       = self::encodeString($clientId, self::PACK_INT16);

        $binData = $apiKey . $apiVersion . $correlationId . $clientId;
        return $binData;
    }

    public static function pack(string $type, ?string $data): string
    {
        if ($type !== self::BIT_B64) {
            return pack($type, $data);
        }

        if ((int) $data === -1) { // -1L
            return hex2bin('ffffffffffffffff');
        }

        if ((int) $data === -2) { // -2L
            return hex2bin('fffffffffffffffe');
        }

        $left  = 0xffffffff00000000;
        $right = 0x00000000ffffffff;

        $l = ($data & $left) >> 32;
        $r = $data & $right;

        return pack($type, $l, $r);
    }

    /**
     * @param string $type
     * @param string $bytes
     * @return array|int|int[]|mixed
     * @throws Exception
     */
    public static function unpack(string $type, string $bytes)
    {
        self::checkLen($type, $bytes);

        if ($type === self::BIT_B64) {
            $set    = unpack($type, $bytes);
            $result = ($set[1] & 0xFFFFFFFF) << 32 | ($set[2] & 0xFFFFFFFF);
        } elseif ($type === self::BIT_B16_SIGNED) {
            // According to PHP docs: 's' = signed short (always 16 bit, machine byte order)
            // So lets unpack it..
            $set = unpack($type, $bytes);

            // But if our system is little endian
            if (self::isSystemLittleEndian()) {
                // We need to flip the endianess because coming from kafka it is big endian
                $set = self::convertSignedShortFromLittleEndianToBigEndian($set);
            }
            $result = $set;
        } else {
            $result = unpack($type, $bytes);
        }

        return is_array($result) ? array_shift($result) : $result;
    }

    /**
     * @param string $type
     * @param string $bytes
     * @throws Exception
     */
    protected static function checkLen(string $type, string $bytes): void
    {
        $expectedLength = 0;
        switch ($type) {
            case self::BIT_B64:
                $expectedLength = 8;
                break;
            case self::BIT_B32:
                $expectedLength = 4;
                break;
            case self::BIT_B16:
                $expectedLength = 2;
                break;
            case self::BIT_B16_SIGNED:
                $expectedLength = 2;
                break;
            case self::BIT_B8:
                $expectedLength = 1;
                break;
        }

        $length = strlen($bytes);

        if ($length !== $expectedLength) {
            throw new Exception('unpack failed. string(raw) length is ' . $length . ' , TO ' . $type);
        }
    }

    /**
     * Get kafka api version according to specify kafka broker version
     */
    public function getApiVersion(int $apiKey): int
    {
        switch ($apiKey) {
            case self::FETCH_REQUEST:
            case self::PRODUCE_REQUEST:
                if (version_compare($this->version, '0.10.0') >= 0) {
                    return self::API_VERSION2;
                }
                if (version_compare($this->version, '0.9.0') >= 0) {
                    return self::API_VERSION1;
                }
                return self::API_VERSION0;

            case self::OFFSET_COMMIT_REQUEST:
                if (version_compare($this->version, '0.9.0') >= 0) {
                    return self::API_VERSION2;
                }
                if (version_compare($this->version, '0.8.2') >= 0) {
                    return self::API_VERSION1;
                }
                return self::API_VERSION0;

            case self::OFFSET_FETCH_REQUEST:
                if (version_compare($this->version, '0.8.2') >= 0) {
                    return self::API_VERSION1; // Offset Fetch Request v1 will fetch offset from Kafka
                }
                return self::API_VERSION0;

            case self::JOIN_GROUP_REQUEST:
                if (version_compare($this->version, '0.10.1.0') >= 0) {
                    return self::API_VERSION1;
                }
                return self::API_VERSION0;

            default:
                return self::API_VERSION0;
        }
    }

    /**
     * @param string $string
     * @param int    $bytes
     * @param int    $compression
     * @return string
     * @throws NotSupported
     */
    public static function encodeString(string $string, int $bytes, int $compression = self::COMPRESSION_NONE): string
    {
        $packLen = $bytes === self::PACK_INT32 ? self::BIT_B32 : self::BIT_B16;
        $string  = self::compress($string, $compression);

        return self::pack($packLen, (string) strlen($string)) . $string;
    }

    /**
     * @param array    $array
     * @param callable $func
     * @param int|null $options
     * @return string
     */
    public static function encodeArray(array $array, callable $func, ?int $options = null): string
    {
        $arrayCount = count($array);

        $body = '';
        foreach ($array as $value) {
            $body .= $options !== null ? $func($value, $options) : $func($value);
        }

        return self::pack(self::BIT_B32, (string) $arrayCount) . $body;
    }

    /**
     * @param string   $data
     * @param callable $func
     * @param null     $options
     * @return array
     * @throws Exception
     */
    public function decodeArray(string $data, callable $func, $options = null): array
    {
        $offset     = 0;
        $arrayCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset    += 4;
        $result = [];

        for ($i = 0; $i < $arrayCount; $i++) {
            $value = substr($data, $offset);
            $ret   = $options !== null ? $func($value, $options) : $func($value);

            if (! is_array($ret) && $ret === false) {
                break;
            }

            if (! isset($ret['length'], $ret['data'])) {
                throw new ProtocolException('Decode array failed, given function return format is invalid');
            }
            if ((int) $ret['length'] === 0) {
                continue;
            }

            $offset  += $ret['length'];
            $result[] = $ret['data'];
        }
        return ['length' => $offset, 'data' => $result];
    }

    /**
     * @param string $data
     * @param string $bytes
     * @param int    $compression
     * @return array
     * @throws Exception
     */
    public function decodeString(string $data, string $bytes, int $compression = self::COMPRESSION_NONE): array
    {
        $offset  = $bytes === self::BIT_B32 ? 4 : 2;
        $packLen = self::unpack($bytes, substr($data, 0, $offset)); // int16 topic name length

        if ($packLen === 4294967295) { // uint32(4294967295) is int32 (-1)
            $packLen = 0;
        }

        if ($packLen === 0) {
            return ['length' => $offset, 'data' => ''];
        }

        $data    = (string) substr($data, $offset, $packLen);
        $offset += $packLen;

        return ['length' => $offset, 'data' => self::decompress($data, $compression)];
    }

    /**
     * @param string $string
     * @param int    $compression
     * @return string
     * @throws NotSupported
     */
    private static function decompress(string $string, int $compression): string
    {
        if ($compression === self::COMPRESSION_NONE) {
            return $string;
        }

        if ($compression === self::COMPRESSION_SNAPPY) {
            throw new NotSupported('SNAPPY compression not yet implemented');
        }

        if ($compression !== self::COMPRESSION_GZIP) {
            throw new NotSupported('Unknown compression flag: ' . $compression);
        }

        return gzdecode($string);
    }

    /**
     * @param string $string
     * @param int    $compression
     * @return string
     * @throws NotSupported
     */
    private static function compress(string $string, int $compression): string
    {
        if ($compression === self::COMPRESSION_NONE) {
            return $string;
        }

        if ($compression === self::COMPRESSION_SNAPPY) {
            throw new NotSupported('SNAPPY compression not yet implemented');
        }

        if ($compression !== self::COMPRESSION_GZIP) {
            throw new NotSupported('Unknown compression flag: ' . $compression);
        }

        return gzencode($string);
    }

    /**
     * @param string $data
     * @param string $bit
     * @return array
     * @throws Exception
     */
    public function decodePrimitiveArray(string $data, string $bit): array
    {
        $offset     = 0;
        $arrayCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset    += 4;

        if ($arrayCount === 4294967295) {
            $arrayCount = 0;
        }

        $result = [];

        for ($i = 0; $i < $arrayCount; $i++) {
            if ($bit === self::BIT_B64) {
                $result[] = self::unpack(self::BIT_B64, substr($data, $offset, 8));
                $offset  += 8;
            } elseif ($bit === self::BIT_B32) {
                $result[] = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset  += 4;
            } elseif (in_array($bit, [self::BIT_B16, self::BIT_B16_SIGNED], true)) {
                $result[] = self::unpack($bit, substr($data, $offset, 2));
                $offset  += 2;
            } elseif ($bit === self::BIT_B8) {
                $result[] = self::unpack($bit, substr($data, $offset, 1));
                ++$offset;
            }
        }

        return ['length' => $offset, 'data' => $result];
    }

    public static function isSystemLittleEndian(): bool
    {
        // If we don't know if our system is big endian or not yet...
        if (self::$isLittleEndianSystem === null) {
            [$endianTest] = array_values(unpack('L1L', pack('V', 1)));

            self::$isLittleEndianSystem = (int) $endianTest === 1;
        }

        return self::$isLittleEndianSystem;
    }

}