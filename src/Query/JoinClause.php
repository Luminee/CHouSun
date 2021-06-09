<?php

namespace Luminee\CHouSun\Query;

use Closure;
use InvalidArgumentException;

class JoinClause extends Builder
{
    /**
     * The type of join being performed.
     *
     * @var string
     */
    public $type;

    /**
     * The table the join clause is joining to.
     *
     * @var string
     */
    public $table;

    /**
     * The parent query builder instance.
     *
     * @var Builder
     */
    private $builder;

    /**
     * Create a new join clause instance.
     *
     * @param Builder $builder
     * @param string $type
     * @param string $table
     * @return void
     */
    public function __construct(Builder $builder, string $type, string $table)
    {
        $this->type = $type;
        $this->table = $table;
        $this->builder = $builder;

        parent::__construct($builder->getConnection(), $builder->getGrammar());
    }

    /**
     * Add an "on" clause to the join.
     *
     * @param Closure|string $first
     * @param string|null $operator
     * @param string|null $second
     * @param string $boolean
     * @return $this
     *
     * @throws InvalidArgumentException
     */
    public function on($first, $operator = null, $second = null, $boolean = 'and')
    {
        if ($first instanceof Closure) {
            return $this->whereNested($first, $boolean);
        }

        return $this->whereColumn($first, $operator, $second, $boolean);
    }

    /**
     * Get a new instance of the join clause builder.
     *
     * @return $this
     */
    public function newQuery()
    {
        return new static($this->builder, $this->type, $this->table);
    }

}
