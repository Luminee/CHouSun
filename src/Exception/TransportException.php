<?php

declare(strict_types=1);

namespace Luminee\CHouSun\Exception;

use LogicException;

final class TransportException extends LogicException implements ClickHouseException
{
}
