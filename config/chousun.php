<?php

return [

    'default' => env('CH_CONNECTION', 'default'),

    /**
     * ES Connections
     */
    'connections' => [

        'default' => [

            'driver' => 'http',

            'host' => env('CH_HOST', '127.0.0.1'),

            'port' => env('CH_PORT', 8123),

            'database' => env('CH_DATABASE', 'default'),

            'username' => env('CH_USERNAME', 'default'),

            'password' => env('CH_PASSWORD', null)

        ],

    ],

];