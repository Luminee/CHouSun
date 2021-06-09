<?php

namespace Luminee\CHouSun\Transport;

interface IStream
{
    public function isGzipHeader();
    public function closure(callable $callable);
    public function getStream();
    public function getClosure();
    public function isWrite();
    public function applyGzip();
}
