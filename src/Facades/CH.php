<?php

namespace Luminee\CHouSun\Facades;

use Illuminate\Support\Facades\Facade;

class CH extends Facade
{
    protected static function getFacadeAccessor()
    {
        return 'ch';
    }
}