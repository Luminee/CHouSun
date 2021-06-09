<?php

namespace Luminee\CHouSun\Transport;

use const PHP_EOL;
use Exception;
use Luminee\CHouSun\Core\Query;
use Luminee\CHouSun\Core\Settings;
use Luminee\CHouSun\Core\Statement;
use Luminee\CHouSun\Core\WhereInFile;
use Luminee\CHouSun\Core\WriteToFile;
use Luminee\CHouSun\Exception\TransportException;

class Http implements ConnectionInterface
{
    const AUTH_METHOD_HEADER = 1;
    const AUTH_METHOD_QUERY_STRING = 2;
    const AUTH_METHOD_BASIC_AUTH = 3;

    /**
     * @var string
     */
    private $_username = null;

    /**
     * @var string
     */
    private $_password = null;

    /**
     * The username and password can be indicated in one of three ways:
     *  - Using HTTP Basic Authentication.
     *  - In the ‘user’ and ‘password’ URL parameters.
     *  - Using ‘X-ClickHouse-User’ and ‘X-ClickHouse-Key’ headers (by default)
     *
     * @see https://clickhouse.tech/docs/en/interfaces/http/
     * @var int
     */
    private $_authMethod = self::AUTH_METHOD_HEADER;

    /**
     * @var string
     */
    private $_host = '';

    /**
     * @var int
     */
    private $_port = 0;

    /**
     * @var bool|int
     */
    private $_verbose = false;

    /**
     * @var CurlerRolling
     */
    private $_curler = null;

    /**
     * @var Settings
     */
    private $_settings = null;

    /**
     * Count seconds (int)
     *
     * @var int
     */
    private $_connectTimeOut = 5;

    /**
     * @var callable
     */
    private $xClickHouseProgress = null;

    /**
     * @var null|string
     */
    private $sslCA = null;

    /**
     * Http constructor.
     * @param string $host
     * @param int $port
     * @param string $database
     * @param string $username
     * @param $password
     */
    public function __construct(string $host, int $port, string $database,
                                string $username, $password)
    {
        $this->_host = $host;
        $this->_port = $port;
        $this->_username = $username;
        $this->_password = $password;

        $this->_settings = new Settings($this);

        $this->_settings->database($database);
        $this->setCurler();
    }


    public function setCurler(): void
    {
        $this->_curler = new CurlerRolling();
    }

    /**
     * Sets client SSL certificate for Yandex Cloud
     *
     * @param string $caPath
     */
    public function setSslCa(string $caPath): void
    {
        $this->sslCA = $caPath;
    }

    /**
     * @return string
     */
    public function getUri(): string
    {
        $proto = 'http';
        if ($this->settings()->isHttps()) {
            $proto = 'https';
        }
        $uri = $proto . '://' . $this->_host;
        if (stripos($this->_host, '/') !== false || stripos($this->_host, ':') !== false) {
            return $uri;
        }
        if (intval($this->_port) > 0) {
            return $uri . ':' . $this->_port;
        }
        return $uri;
    }

    /**
     * @return Settings
     */
    public function settings(): Settings
    {
        return $this->_settings;
    }

    /**
     * @param bool|int $flag
     * @return mixed
     */
    public function verbose($flag): mixed
    {
        $this->_verbose = $flag;
        return $flag;
    }

    /**
     * @param array $params
     * @return string
     */
    private function getUrl($params = []): string
    {
        $settings = $this->settings()->getSettings();

        if (is_array($params) && sizeof($params)) {
            $settings = array_merge($settings, $params);
        }


        if ($this->settings()->isReadOnlyUser()) {
            unset($settings['extremes']);
            unset($settings['readonly']);
            unset($settings['enable_http_compression']);
            unset($settings['max_execution_time']);

        }

        unset($settings['https']);


        return $this->getUri() . '?' . http_build_query($settings);
    }

    /**
     * @param array $extendInfo
     * @return CurlerRequest
     */
    private function newRequest(array $extendInfo): CurlerRequest
    {
        $new = new CurlerRequest();

        switch ($this->_authMethod) {
            case self::AUTH_METHOD_QUERY_STRING:
                /* @todo: Move this implementation to CurlerRequest class. Possible options: the authentication method
                 *        should be applied in method `CurlerRequest:prepareRequest()`.
                 */
                $this->settings()->set('user', $this->_username);
                $this->settings()->set('password', $this->_password);
                break;
            case self::AUTH_METHOD_BASIC_AUTH:
                $new->authByBasicAuth($this->_username, $this->_password);
                break;
            default:
                // Auth with headers by default
                $new->authByHeaders($this->_username, $this->_password);
                break;
        }

        $new->POST()->setRequestExtendedInfo($extendInfo);

        if ($this->settings()->isEnableHttpCompression()) {
            $new->httpCompression(true);
        }
        if ($this->settings()->getSessionId()) {
            $new->persistent();
        }
        if ($this->sslCA) {
            $new->setSslCa($this->sslCA);
        }

        $new->timeOut($this->settings()->getTimeOut());
        $new->connectTimeOut($this->_connectTimeOut);//->keepAlive(); // one sec
        $new->verbose(boolval($this->_verbose));

        return $new;
    }

    /**
     * @param Query $query
     * @param array $urlParams
     * @param bool $query_as_string
     * @return CurlerRequest
     * @throws TransportException
     */
    private function makeRequest(Query $query, $urlParams = [], $query_as_string = false): CurlerRequest
    {
        $sql = $query->toSql();

        if ($query_as_string) {
            $urlParams['query'] = $sql;
        }

        $extendInfo = [
            'sql' => $sql,
            'query' => $query,
            'format' => $query->getFormat()
        ];

        $new = $this->newRequest($extendInfo);

        /*
         * Build URL after request making, since URL may contain auth data. This will not matter after the
         * implantation of the todo in the `HTTP:newRequest()` method.
         */
        $url = $this->getUrl($urlParams);
        $new->url($url);


        if (!$query_as_string) {
            $new->parameters_json($sql);
        }
        if ($this->settings()->isEnableHttpCompression()) {
            $new->httpCompression(true);
        }

        return $new;
    }

    /**
     * @param string|Query $sql
     * @return CurlerRequest
     */
    public function writeStreamData($sql): CurlerRequest
    {
        if ($sql instanceof Query) {
            $query = $sql;
        } else {
            $query = new Query($sql);
        }

        $extendInfo = [
            'sql' => $sql,
            'query' => $query,
            'format' => $query->getFormat()
        ];

        $request = $this->newRequest($extendInfo);

        /*
         * Build URL after request making, since URL may contain auth data. This will not matter after the
         * implantation of the todo in the `HTTP:newRequest()` method.
         */
        $url = $this->getUrl([
            'readonly' => 0,
            'query' => $query->toSql()
        ]);

        $request->url($url);
        return $request;
    }

    /**
     * get Count Pending Query in Queue
     *
     * @return int
     */
    public function getCountPendingQueue(): int
    {
        return $this->_curler->countPending();
    }

    /**
     * get ConnectTimeOut in seconds
     *
     * @return int
     */
    public function getConnectTimeOut(): int
    {
        return $this->_connectTimeOut;
    }


    public function __findXClickHouseProgress($handle): bool
    {
        $code = curl_getinfo($handle, CURLINFO_HTTP_CODE);

        // Search X-ClickHouse-Progress
        if ($code == 200) {
            $response = curl_multi_getcontent($handle);
            $header_size = curl_getinfo($handle, CURLINFO_HEADER_SIZE);
            if (!$header_size) {
                return false;
            }

            $header = substr($response, 0, $header_size);
            if (!$header_size) {
                return false;
            }

            $pos = strrpos($header, 'X-ClickHouse-Summary:');
            if (!$pos) {
                return false;
            }

            $last = substr($header, $pos);
            $data = @json_decode(str_ireplace('X-ClickHouse-Summary:', '', $last), true);

            if ($data && is_callable($this->xClickHouseProgress)) {

                if (is_array($this->xClickHouseProgress)) {
                    call_user_func_array($this->xClickHouseProgress, [$data]);
                } else {
                    call_user_func($this->xClickHouseProgress, $data);
                }


            }

        }
        return false;
    }

    /**
     * @param Query $query
     * @param null|WhereInFile $whereInFile
     * @param null|WriteToFile $writeToFile
     * @return CurlerRequest
     * @throws Exception
     */
    public function getRequestRead(Query $query, $whereInFile = null, $writeToFile = null): CurlerRequest
    {
        $urlParams = ['readonly' => 2];
        $query_as_string = false;
        // ---------------------------------------------------------------------------------
        if ($whereInFile instanceof WhereInFile && $whereInFile->size()) {
            // $request = $this->prepareSelectWhereIn($request, $whereInFile);
            $structure = $whereInFile->fetchUrlParams();
            // $structure = [];
            $urlParams = array_merge($urlParams, $structure);
            $query_as_string = true;
        }
        // ---------------------------------------------------------------------------------
        // if result to file
        if ($writeToFile instanceof WriteToFile && $writeToFile->fetchFormat()) {
            $query->setFormat($writeToFile->fetchFormat());
            unset($urlParams['extremes']);
        }
        // ---------------------------------------------------------------------------------
        // makeRequest read
        $request = $this->makeRequest($query, $urlParams, $query_as_string);
        // ---------------------------------------------------------------------------------
        // attach files
        if ($whereInFile instanceof WhereInFile && $whereInFile->size()) {
            $request->attachFiles($whereInFile->fetchFiles());
        }
        // ---------------------------------------------------------------------------------
        // result to file
        if ($writeToFile instanceof WriteToFile && $writeToFile->fetchFormat()) {

            $fout = fopen($writeToFile->fetchFile(), 'w');
            if (is_resource($fout)) {

                $isGz = $writeToFile->getGzip();

                if ($isGz) {
                    // write gzip header
                    // "\x1f\x8b\x08\x00\x00\x00\x00\x00"
                    // fwrite($fout, "\x1F\x8B\x08\x08".pack("V", time())."\0\xFF", 10);
                    // write the original file name
                    // $oname = str_replace("\0", "", basename($writeToFile->fetchFile()));
                    // fwrite($fout, $oname."\0", 1+strlen($oname));

                    fwrite($fout, "\x1f\x8b\x08\x00\x00\x00\x00\x00");

                }


                $request->setResultFileHandle($fout, $isGz)->setCallbackFunction(function (CurlerRequest $request) {
                    fclose($request->getResultFileHandle());
                });
            }
        }
        if ($this->xClickHouseProgress) {
            $request->setFunctionProgress([$this, '__findXClickHouseProgress']);
        }
        // ---------------------------------------------------------------------------------
        return $request;

    }

    /**
     * @param Query $query
     * @return CurlerRequest
     * @throws TransportException
     */
    public function getRequestWrite(Query $query): CurlerRequest
    {
        $urlParams = ['readonly' => 0];
        return $this->makeRequest($query, $urlParams);
    }

    /**
     * @throws TransportException
     */
    public function ping(): bool
    {
        $request = new CurlerRequest();
        $request->url($this->getUri())->verbose(false)->GET()->connectTimeOut($this->getConnectTimeOut());
        $this->_curler->execOne($request);

        return $request->response()->body() === 'Ok.' . PHP_EOL;
    }

    /**
     * @param Query|string $sql
     * @param mixed[] $bindings
     * @param null|WhereInFile $whereInFile
     * @param null|WriteToFile $writeToFile
     * @return CurlerRequest
     * @throws Exception
     */
    private function prepareSelect($sql, array $bindings, $whereInFile, $writeToFile = null): CurlerRequest
    {
        $sql = $this->buildSql($sql, $bindings);
        if ($sql instanceof Query) {
            return $this->getRequestWrite($sql);
        }
        $query = new Query($sql);
        $query->setFormat('JSON');
        return $this->getRequestRead($query, $whereInFile, $writeToFile);
    }


    /**
     * @param Query|string $sql
     * @param mixed[] $bindings
     * @return CurlerRequest
     * @throws TransportException
     */
    private function prepareWrite($sql, $bindings = []): CurlerRequest
    {
        $sql = $this->buildSql($sql, $bindings);
        if ($sql instanceof Query) {
            return $this->getRequestWrite($sql);
        }

        $query = new Query($sql);
        return $this->getRequestWrite($query);
    }

    /**
     * Is Using...
     *
     * @param Query|string $sql
     * @param mixed[] $bindings
     * @param null|WhereInFile $whereInFile
     * @param null|WriteToFile $writeToFile
     * @return Statement
     * @throws TransportException
     * @throws Exception
     */
    public function select($sql, array $bindings = [], $whereInFile = null, $writeToFile = null): Statement
    {
        $request = $this->prepareSelect($sql, $bindings, $whereInFile, $writeToFile);
        $this->_curler->execOne($request);
        return new Statement($request);
    }

    /**
     * @param Query|string $sql
     * @param mixed[] $bindings
     * @param null|WhereInFile $whereInFile
     * @param null|WriteToFile $writeToFile
     * @return Statement
     * @throws TransportException
     * @throws Exception
     */
    public function selectAsync($sql, array $bindings = [], $whereInFile = null, $writeToFile = null): Statement
    {
        $request = $this->prepareSelect($sql, $bindings, $whereInFile, $writeToFile);
        $this->_curler->addQueLoop($request);
        return new Statement($request);
    }

    /**
     * @param string $sql
     * @param mixed[] $bindings
     * @param bool $exception
     * @return Statement
     * @throws TransportException
     */
    public function write(string $sql, array $bindings = [], $exception = true): Statement
    {
        $request = $this->prepareWrite($sql, $bindings);
        $this->_curler->execOne($request);
        $response = new Statement($request);
        if ($exception) {
            if ($response->isError()) {
                $response->error();
            }
        }
        return $response;
    }

    /**
     * @param Stream $streamRW
     * @param CurlerRequest $request
     * @return Statement
     * @throws TransportException
     */
    private function streaming(Stream $streamRW, CurlerRequest $request): Statement
    {
        $callable = $streamRW->getClosure();
        $stream = $streamRW->getStream();

        try {
            if (!is_callable($callable)) {
                if ($streamRW->isWrite()) {

                    $callable = function ($ch, $fd, $length) use ($stream) {
                        return ($line = fread($stream, $length)) ? $line : '';
                    };
                } else {
                    $callable = function ($ch, $fd) use ($stream) {
                        return fwrite($stream, $fd);
                    };
                }
            }

            if ($streamRW->isGzipHeader()) {
                if ($streamRW->isWrite()) {
                    $request->header('Content-Encoding', 'gzip');
                    $request->header('Content-Type', 'application/x-www-form-urlencoded');
                } else {
                    $request->header('Accept-Encoding', 'gzip');
                }
            }

            $request->header('Transfer-Encoding', 'chunked');

            if ($streamRW->isWrite()) {
                $request->setReadFunction($callable);
            } else {
                $request->setWriteFunction($callable);
//                $request->setHeaderFunction($callableHead);
            }

            $this->_curler->execOne($request, true);
            $response = new Statement($request);
            if ($response->isError()) {
                $response->error();
            }
            return $response;
        } finally {
            if ($streamRW->isWrite())
                fclose($stream);
        }

    }

    /**
     * @param Stream $streamRead
     * @param string $sql
     * @param mixed[] $bindings
     * @return Statement
     * @throws
     */
    public function streamRead(Stream $streamRead, string $sql, $bindings = []): Statement
    {
        $sql = new Query($sql);
        $request = $this->getRequestRead($sql);
        return $this->streaming($streamRead, $request);

    }

    /**
     * @param Stream $streamWrite
     * @param string $sql
     * @param mixed[] $bindings
     * @return Statement
     * @throws TransportException
     */
    public function streamWrite(Stream $streamWrite, string $sql, $bindings = []): Statement
    {
        $sql = new Query($sql);
        $request = $this->writeStreamData($sql);
        return $this->streaming($streamWrite, $request);
    }

    protected function buildSql($sql, $bindings)
    {
        return vsprintf(str_replace("?", "'%s'", $sql), $bindings);
    }
}
