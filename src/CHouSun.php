<?php

namespace Luminee\CHouSun;

use Exception;
use Luminee\CHouSun\Query\Builder;
use Luminee\CHouSun\Query\Grammar;
use Luminee\CHouSun\Transport\Http;
use Luminee\CHouSun\Transport\ConnectionInterface;

class CHouSun
{
    /**
     * @var array
     */
    protected $config;

    /**
     * @var ConnectionInterface
     */
    protected $connection;

    /**
     * @var Grammar
     */
    protected $grammar;

    /**
     * CHouSun constructor.
     * @param null $config
     * @throws Exception
     */
    public function __construct($config = null)
    {
        $this->config = $config ?? config('chousun');
        $this->connection = $this->connect();
        $this->grammar = new Grammar();
    }

    public function table($table)
    {
        return $this->query()->from($table);
    }

    protected function query()
    {
        return new Builder($this->connection, $this->grammar);
    }

    /**
     * @return ConnectionInterface
     * @throws
     */
    protected function connect(): ConnectionInterface
    {
        $default = $this->config['default'];
        $conn = $this->config['connections'][$default];
        $driver = $conn['driver'];
        if ($driver == 'http')
            return new Http($conn['host'], $conn['port'],
                $conn['database'], $conn['username'], $conn['password']);
        throw new Exception('Driver Exception');
    }

}