<?php


namespace EasySwoole\Kafka2\Breaker;


use EasySwoole\Kafka2\Exception\ErrorBreakerOpenException;
use EasySwoole\Kafka2\Exception\Exception;

class Breaker
{
    const CLOSED = 0;
    const OPEN = 1;
    const HALF_OPEN = 2;
    /**
     * @var int
     */
    private $errorThreshold;
    /**
     * @var int
     */
    private $successThreshold;

    /**
     * @var int
     */
    private $timeout;
    /**
     * @var bool
     */
    private $lock;
    /**
     * @var int
     */
    private $state = 0;

    /**
     * @var int
     */
    private $error;
    /**
     * @var int
     */
    private $success;

    private $lastError;

    public function __construct(int $errorThreshold, int $successThreshold, int $timeout)
    {
        $this->errorThreshold = $errorThreshold;
        $this->successThreshold = $successThreshold;
        $this->timeout = $timeout;
    }

    public function run(callable $callable)
    {
        if ($this->state == self::OPEN) {
            throw new ErrorBreakerOpenException('circuit breaker is open');
        }
        return $this->doWork($this->state, $callable);
    }

    public function go(callable $callable)
    {
        if ($this->state == self::OPEN) {
            throw new ErrorBreakerOpenException('circuit breaker is open');
        }
        go($this->doWork($this->state, $callable));
        return null;
    }

    private function doWork(int $state, callable $callable)
    {
        $panicValue = null;
        $fun = function () use($callable) {
            defer(function () {

            });
            return $callable();
        };
        $result = $fun();

        if (is_null($result) && is_null($panicValue) && $state == self::CLOSED) {
            return null;
        }



        if (!is_null($panicValue)) {
            throw new Exception($panicValue);
        }

        return $result;
    }

    private function processResult($result, $panicValue){}
}