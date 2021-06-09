<?php

namespace Luminee\CHouSun\Core;

use Luminee\CHouSun\Exception\QueryException;

class Query
{
    /**
     * @var string
     */
    protected $sql;

    /**
     * @var string|null
     */
    protected $format = null;

    private $supportFormats = [
        "FORMAT\\s+TSVRaw",
        "FORMAT\\s+TSVWithNamesAndTypes",
        "FORMAT\\s+TSVWithNames",
        "FORMAT\\s+TSV",
        "FORMAT\\s+Vertical",
        "FORMAT\\s+JSONCompact",
        "FORMAT\\s+JSONEachRow",
        "FORMAT\\s+TSKV",
        "FORMAT\\s+TabSeparatedWithNames",
        "FORMAT\\s+TabSeparatedWithNamesAndTypes",
        "FORMAT\\s+TabSeparatedRaw",
        "FORMAT\\s+BlockTabSeparated",
        "FORMAT\\s+CSVWithNames",
        "FORMAT\\s+CSV",
        "FORMAT\\s+JSON",
        "FORMAT\\s+TabSeparated"
    ];

    /**
     * Query constructor.
     * @param string $sql
     */
    public function __construct(string $sql)
    {
        if (!trim($sql))
            throw new QueryException('Empty Query');

        $this->sql = $sql;
    }

    /**
     * @param string|null $format
     */
    public function setFormat(string $format)
    {
        $this->format = $format;
    }

    private function applyFormatQuery()
    {
        // FORMAT\s(\w)*$
        if (null === $this->format) {
            return;
        }
        $supportFormats = implode("|", $this->supportFormats);

        $this->sql = trim($this->sql);
        if (substr($this->sql, -1) == ';') {
            $this->sql = substr($this->sql, 0, -1);
        }

        $matches = [];
        if (preg_match_all('%(' . $supportFormats . ')%ius', $this->sql, $matches)) {

            // skip add "format json"
            if (isset($matches[0])) {

                $this->format = trim(str_ireplace('format', '', $matches[0][0]));

            }
        } else {
            $this->sql = $this->sql . ' FORMAT ' . $this->format;
        }
    }

    /**
     * @return null|string
     */
    public function getFormat()
    {

        return $this->format;
    }

    public function toSql()
    {
        if ($this->format !== null) {
            $this->applyFormatQuery();
        }

        return $this->sql;
    }

    /**
     * @return string
     */
    public function __toString()
    {
        return $this->toSql();
    }
}
