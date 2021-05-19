<?php

namespace EasySwoole\Kafka1\Config;

use EasySwoole\Spl\SplBean;
use EasySwoole\Kafka1\Exception;

class Config extends SplBean
{
    public const SECURITY_PROTOCOL_PLAINTEXT = 'PLAINTEXT';
    public const SECURITY_PROTOCOL_SSL = 'SSL';
    public const SECURITY_PROTOCOL_SASL_PLAINTEXT = 'SASL_PLAINTEXT';
    public const SECURITY_PROTOCOL_SASL_SSL = 'SASL_SSL';

    public const SASL_MECHANISMS_PLAIN = 'PLAIN';
    public const SASL_MECHANISMS_GSSAPI = 'GSSAPI';
    public const SASL_MECHANISMS_SCRAM_SHA_256 = 'SCRAM_SHA_256';
    public const SASL_MECHANISMS_SCRAM_SHA_512 = 'SCRAM_SHA_512';

    private const ALLOW_SECURITY_PROTOCOLS = [
        self::SECURITY_PROTOCOL_PLAINTEXT,
        self::SECURITY_PROTOCOL_SSL,
        self::SECURITY_PROTOCOL_SASL_PLAINTEXT,
        self::SECURITY_PROTOCOL_SASL_SSL,
    ];

    private const ALLOW_MECHANISMS = [
        self::SASL_MECHANISMS_PLAIN,
        self::SASL_MECHANISMS_GSSAPI,
        self::SASL_MECHANISMS_SCRAM_SHA_256,
        self::SASL_MECHANISMS_SCRAM_SHA_512,
    ];

    private $clientId               = 'Easyswoole-kafka';
    private $brokerVersion          = '0.10.1.0';
    private $metadataBrokerList     = '';
    private $requestTimeoutMs       = 60000;
    private $refreshIntervalMs      = 1000;
    private $securityProtocol       = self::SECURITY_PROTOCOL_PLAINTEXT;
    private $saslMechanism          = self::SASL_MECHANISMS_PLAIN;
    private $saslUsername           = '';
    private $saslPassword           = '';
    private $saslKeytab             = '';
    private $saslPrincipal          = '';

    private $sslEnable              = false;
    private $sslLocalCert           = '';
    private $sslLocalPk             = '';
    private $sslVerifyPeer          = false;
    private $sslPassPhrase          = '';
    private $sslCafile              = '';
    private $sslPeerName            = '';


    /**
     * @param string $client
     * @throws Exception\Config
     */
    public function setClientId(string $client): void
    {
        $client = trim($client);

        if ($client === '') {
            throw new Exception\Config("Set ClientId value is invalid, must is not empty string.");
        }

        $this->clientId = $client;
    }

    /**
     * @return string
     */
    public function getClientId(): string
    {
        return $this->clientId;
    }

    /**
     * @param string $version
     * @throws Exception\Config
     */
    public function setBrokerVersion(string $version): void
    {
        $version = trim($version);

        if ($version === '' || version_compare($version, '0.8.0', '<')) {
            throw new Exception\Config("Set broker version value is invalid, must is not empty string and gt 0.8.0.");
        }

        $this->brokerVersion = $version;
    }

    /**
     * @return string
     */
    public function getBrokerVersion(): string
    {
        return $this->brokerVersion;
    }

    /**
     * @param string $brokerList
     * @throws Exception\Config
     */
    public function setMetadataBrokerList(string $brokerList): void
    {
        $brokerList = trim($brokerList);

        // 拆分数组，检验每一个成员是否符合规则。最后仍存储string
        $brokers = array_filter(
            explode(',', $brokerList),
            function (string $broker): bool {
                return preg_match('/^(.*:[\d]+)$/', $broker) === 1;
            }
        );

        if (empty($brokers)) {
            throw new Exception\Config("Broker list must be a comma-separated list of brokers (format: 'host:port'), with at least one broker.");
        }

        $this->metadataBrokerList = $brokerList;
    }

    public function getMetadataBrokerList()
    {
        return $this->metadataBrokerList;
    }

    /**
     * @param int $requestTimeoutMs
     * @throws Exception\Config
     */
    public function setRequestTimeoutMs(int $requestTimeoutMs): void
    {
        if ($requestTimeoutMs < 10 || $requestTimeoutMs > 900000) {
            throw new Exception\Config("Set request timeout value is invalid, must set it 10 .. 900000.");
        }

        $this->requestTimeoutMs = $requestTimeoutMs;
    }

    public function getRequestTimeoutMs()
    {
        return $this->requestTimeoutMs;
    }

    /**
     * @param int $refreshIntervalMs
     * @throws Exception\Config
     */
    public function setRefreshIntervalMs(int $refreshIntervalMs): void
    {
        if ($refreshIntervalMs < 10 || $refreshIntervalMs > 360000) {
            throw new Exception\Config("Set refresh interval value is invalid, must set it 10 .. 360000.");
        }

        $this->refreshIntervalMs = $refreshIntervalMs;
    }

    public function getRefreshIntervalMs()
    {
        return $this->refreshIntervalMs;
    }

    /**
     * @param string $securityProtocol
     * @throws Exception\Config
     */
    public function setSecurityProtocol(string $securityProtocol): void
    {
        if (! in_array($securityProtocol, self::ALLOW_SECURITY_PROTOCOLS, true)) {
            throw new Exception\Config("Invalid security protocol given.");
        }

        $this->securityProtocol = $securityProtocol;
    }

    public function getSecurityProtocol()
    {
        return $this->securityProtocol;
    }

    /**
     * @param string $username
     * @throws Exception\Config
     */
    public function setSaslUsername(string $username): void
    {
        $username = trim($username);
        if ($username === '') {
            throw new Exception\Config("Set sasl username value is invalid, must is not empty string.");
        }

        $this->saslUsername = $username;
    }

    public function getSaslUsername()
    {
        return $this->saslUsername;
    }

    /**
     * @param string $password
     * @throws Exception\Config
     */
    public function setSaslPassword(string $password): void
    {
        $password = trim($password);
        if ($password === '') {
            throw new Exception\Config("Set sasl password value is invalid, must is not empty string.");
        }

        $this->saslPassword = $password;
    }

    public function getSaslPassword()
    {
        return $this->saslPassword;
    }

    /**
     * @param string $principal
     * @throws Exception\Config
     */
    public function setSaslPrincipal(string $principal): void
    {
        $principal = trim($principal);
        if ($principal === '') {
            throw new Exception\Config("Set sasl principal value is invalid, must is not empty string.");
        }

        $this->saslPrincipal = $principal;
    }

    public function getSaslPrincipal()
    {
        return $this->saslPrincipal;
    }

    /**
     * @param string $keytab
     * @throws Exception\Config
     */
    public function setSaslKeytab(string $keytab): void
    {
        if (! is_file($keytab)) {
            throw new Exception\Config("Set sasl gssapi keytab file is invalid.");
        }

        $this->saslKeytab = $keytab;
    }

    public function getSaslKeytab()
    {
        return $this->saslKeytab;
    }

    /**
     * @param string $mechnism
     * @throws Exception\Config
     */
    public function setSaslMechanism(string $mechnism): void
    {
        if (! in_array($mechnism, self::ALLOW_MECHANISMS, true)) {
            throw new Exception\Config("Invalid security sasl mechanism given");
        }

        $this->saslMechanism = $mechnism;
    }

    public function getSaslMechanism()
    {
        return $this->saslMechanism;
    }



    /**
     * @param bool $enable
     * @throws Exception\Config
     */
    public function setSslEnable(bool $enable): void
    {
        if (! is_bool($enable)) {
            throw new Exception\Config("Invalid ss enbale given");
        }

        $this->sslEnable = $enable;
    }

    public function getSslEnable()
    {
        return $this->sslEnable;
    }

    /**
     * @param string $localCert
     * @throws Exception\Config
     */
    public function setSslLocalCert(string $localCert): void
    {
        if (! is_file($localCert)) {
            throw new Exception\Config("Set ssl local cert file is invalid.");
        }

        $this->sslLocalCert = $localCert;
    }

    public function getSslLocalCert()
    {
        return $this->sslLocalCert;
    }

    /**
     * @param string $localPk
     * @throws Exception\Config
     */
    public function setSslLocalPk(string $localPk): void
    {
        if (! is_file($localPk)) {
            throw new Exception\Config("Set ssl local private key file is invalid.");
        }

        $this->sslLocalPk = $localPk;
    }

    public function getSslLocalPk()
    {
        return $this->sslLocalPk;
    }

    /**
     * @param bool $verifyPeer
     * @throws Exception\Config
     */
    public function setSslVerifyPeer(bool $verifyPeer): void
    {
        if (! is_bool($verifyPeer)) {
            throw new Exception\Config("Set ssl verify peer values is invalid, must is not empty string.");
        }

        $this->sslVerifyPeer = $verifyPeer;
    }

    public function getSslVerifyPeer()
    {
        return $this->sslVerifyPeer;
    }

    /**
     * @param string $passPhrase
     * @throws Exception\Config
     */
    public function setSslPassPhrase(string $passPhrase): void
    {
        $passPhrase = trim($passPhrase);
        if ($passPhrase === '') {
            throw new Exception\Config("Set ssl passPhare value is invalid, must is not empty string.");
        }

        $this->sslPassPhrase = $passPhrase;
    }

    public function getSslPassPhrase()
    {
        return $this->sslPassPhrase;
    }

    /**
     * @param string $cafile
     * @throws Exception\Config
     */
    public function setSslCafile(string $cafile): void
    {
        if (! is_file($cafile)) {
            throw new Exception\Config("Set ssl ca file is invalid.");
        }

        $this->sslCafile = $cafile;
    }

    public function getSslCafile()
    {
        return $this->sslCafile;
    }

    /**
     * @param string $peerName
     * @throws Exception\Config
     */
    public function setSslPeerName(string $peerName): void
    {
        $peerName = trim($peerName);
        if ($peerName === '') {
            throw new Exception\Config();
        }

        $this->sslPeerName = $peerName;
    }

    public function getSslPeerName()
    {
        return $this->sslPeerName;
    }

}