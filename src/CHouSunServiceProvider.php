<?php

namespace Luminee\CHouSun;

use Illuminate\Support\ServiceProvider;

class CHouSunServiceProvider extends ServiceProvider
{
    /**
     * @var string
     */
    protected $config = __DIR__ . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR . 'config' . DIRECTORY_SEPARATOR;

    /**
     * Boot the service provider.
     *
     * @return void
     */
    public function boot()
    {
        $this->publishes([$this->config . 'chousun.php' => config_path('chousun.php')]);
    }

    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        if (file_exists($this->config . 'chousun.php')) $this->mergeConfigFrom($this->config . 'chousun.php', 'chousun');

        $this->app->singleton('ch', function ($app) {
            return new CHouSun();
        });
    }

}