<?php

namespace Luminee\CHouSun\Query;

use Illuminate\Support\Arr;
use Illuminate\Support\Str;
use Illuminate\Database\Query\JsonExpression;

class Grammar
{
    /**
     * The grammar table prefix.
     *
     * @var string
     */
    protected $tablePrefix = '';

    /**
     * The components that make up a select clause.
     *
     * @var array
     */
    protected $selectComponents = [
        'aggregate',
        'columns',
        'from',
        'joins',
        'wheres',
        'groups',
        'having',
        'orders',
        'limit',
        'offset',
    ];

    /**
     * Compile a select query into SQL.
     *
     * @param Builder $query
     * @return string
     */
    public function compileSelect(Builder $query)
    {
        $original = $query->columns;

        if (is_null($query->columns)) {
            $query->columns = ['*'];
        }

        $sql = trim($this->concatenate(
            $this->compileComponents($query))
        );

        $query->columns = $original;

        if ($query->unions) {
            $sql = '(' . $sql . ') ' . $this->compileUnions($query);
        }

        return $sql;
    }

    /**
     * Compile a single union statement.
     *
     * @param array $union
     * @return string
     */
    protected function compileUnion(array $union)
    {
        $conduction = $union['all'] ? ' union all ' : ' union ';

        return $conduction . '(' . $union['query']->toSql() . ')';
    }

    /**
     * Wrap a single string in keyword identifiers.
     *
     * @param string $value
     * @return string
     */
    protected function wrapValue(string $value)
    {
        if ($value === '*') {
            return $value;
        }

        if ($this->isJsonSelector($value)) {
            return $this->wrapJsonSelector($value);
        }

        return '`' . str_replace('`', '``', $value) . '`';
    }

    /**
     * Wrap the given JSON selector.
     *
     * @param string $value
     * @return string
     */
    protected function wrapJsonSelector(string $value)
    {
        $path = explode('->', $value);

        $field = $this->wrapValue(array_shift($path));

        return sprintf('%s->\'$.%s\'', $field, collect($path)->map(function ($part) {
            return '"' . $part . '"';
        })->implode('.'));
    }

    /**
     * Determine if the given string is a JSON selector.
     *
     * @param string $value
     * @return bool
     */
    protected function isJsonSelector(string $value)
    {
        return Str::contains($value, '->');
    }

    /**
     * Compile the components necessary for a select clause.
     *
     * @param Builder $query
     * @return array
     */
    protected function compileComponents(Builder $query)
    {
        $sql = [];

        foreach ($this->selectComponents as $component) {
            if (!is_null($query->$component)) {
                $method = 'compile' . ucfirst($component);

                $sql[$component] = $this->$method($query, $query->$component);
            }
        }

        return $sql;
    }

    /**
     * Compile an aggregated select clause.
     *
     * @param Builder $query
     * @param array $aggregate
     * @return string
     */
    protected function compileAggregate(Builder $query, array $aggregate)
    {
        $column = $this->columnIze($aggregate['columns']);

        if ($query->distinct && $column !== '*') {
            $column = 'distinct ' . $column;
        }

        return 'select ' . $aggregate['function'] . '(' . $column . ') as aggregate';
    }

    /**
     * Compile the "select *" portion of the query.
     *
     * @param Builder $query
     * @param array $columns
     * @return string|null
     */
    protected function compileColumns(Builder $query, array $columns)
    {
        if (!is_null($query->aggregate)) {
            return;
        }

        $select = $query->distinct ? 'select distinct ' : 'select ';

        return $select . $this->columnIze($columns);
    }

    /**
     * Compile the "from" portion of the query.
     *
     * @param Builder $query
     * @param string $table
     * @return string
     */
    protected function compileFrom(Builder $query, string $table)
    {
        return 'from ' . $this->wrapTable($table);
    }

    /**
     * Compile the "join" portions of the query.
     *
     * @param Builder $query
     * @param array $joins
     * @return string
     */
    protected function compileJoins(Builder $query, array $joins)
    {
        return collect($joins)->map(function ($join) {
            $table = $this->wrapTable($join->table);

            return trim("{$join->type} join {$table} {$this->compileWheres($join)}");
        })->implode(' ');
    }

    /**
     * Compile the "where" portions of the query.
     *
     * @param Builder $query
     * @return string
     */
    protected function compileWheres(Builder $query)
    {
        if (is_null($query->wheres)) {
            return '';
        }

        if (count($sql = $this->compileWheresToArray($query)) > 0) {
            return $this->concatenateWhereClauses($query, $sql);
        }

        return '';
    }

    /**
     * Get an array of all the where clauses for the query.
     *
     * @param Builder $query
     * @return array
     */
    protected function compileWheresToArray(Builder $query)
    {
        return collect($query->wheres)->map(function ($where) use ($query) {
            return $where['boolean'] . ' ' . $this->{"where{$where['type']}"}($query, $where);
        })->all();
    }

    /**
     * Format the where clause statements into one string.
     *
     * @param Builder $query
     * @param array $sql
     * @return string
     */
    protected function concatenateWhereClauses(Builder $query, array $sql)
    {
        $conjunction = $query instanceof JoinClause ? 'on' : 'where';

        return $conjunction . ' ' . $this->removeLeadingBoolean(implode(' ', $sql));
    }

    /**
     * Compile a raw where clause.
     *
     * @param Builder $query
     * @param array $where
     * @return string
     */
    protected function whereRaw(Builder $query, array $where)
    {
        return $where['sql'];
    }

    /**
     * Compile a basic where clause.
     *
     * @param Builder $query
     * @param array $where
     * @return string
     */
    protected function whereBasic(Builder $query, array $where)
    {
        $value = $this->parameter($where['value']);

        return $this->wrap($where['column']) . ' ' . $where['operator'] . ' ' . $value;
    }

    /**
     * Compile a "where in" clause.
     *
     * @param Builder $query
     * @param array $where
     * @return string
     */
    protected function whereIn(Builder $query, array $where)
    {
        if (!empty($where['values'])) {
            return $this->wrap($where['column']) . ' in (' . $this->parameterize($where['values']) . ')';
        }

        return '0 = 1';
    }

    /**
     * Compile a "where not in" clause.
     *
     * @param Builder $query
     * @param array $where
     * @return string
     */
    protected function whereNotIn(Builder $query, array $where)
    {
        if (!empty($where['values'])) {
            return $this->wrap($where['column']) . ' not in (' . $this->parameterize($where['values']) . ')';
        }

        return '1 = 1';
    }

    /**
     * Compile a where in sub-select clause.
     *
     * @param Builder $query
     * @param array $where
     * @return string
     */
    protected function whereInSub(Builder $query, array $where)
    {
        return $this->wrap($where['column']) . ' in (' . $this->compileSelect($where['query']) . ')';
    }

    /**
     * Compile a where not in sub-select clause.
     *
     * @param Builder $query
     * @param array $where
     * @return string
     */
    protected function whereNotInSub(Builder $query, array $where)
    {
        return $this->wrap($where['column']) . ' not in (' . $this->compileSelect($where['query']) . ')';
    }

    /**
     * Compile a "where null" clause.
     *
     * @param Builder $query
     * @param array $where
     * @return string
     */
    protected function whereNull(Builder $query, array $where)
    {
        return $this->wrap($where['column']) . ' is null';
    }

    /**
     * Compile a "where not null" clause.
     *
     * @param Builder $query
     * @param array $where
     * @return string
     */
    protected function whereNotNull(Builder $query, array $where)
    {
        return $this->wrap($where['column']) . ' is not null';
    }

    /**
     * Compile a "between" where clause.
     *
     * @param Builder $query
     * @param array $where
     * @return string
     */
    protected function whereBetween(Builder $query, array $where)
    {
        $between = $where['not'] ? 'not between' : 'between';

        return $this->wrap($where['column']) . ' ' . $between . ' ? and ?';
    }

    /**
     * Compile a date based where clause.
     *
     * @param string $type
     * @param Builder $query
     * @param array $where
     * @return string
     */
    protected function dateBasedWhere(string $type, Builder $query, array $where)
    {
        $value = $this->parameter($where['value']);

        return $type . '(' . $this->wrap($where['column']) . ') ' . $where['operator'] . ' ' . $value;
    }

    /**
     * Compile a nested where clause.
     *
     * @param Builder $query
     * @param array $where
     * @return string
     */
    protected function whereNested(Builder $query, array $where)
    {
        $offset = $query instanceof JoinClause ? 3 : 6;

        return '(' . substr($this->compileWheres($where['query']), $offset) . ')';
    }

    /**
     * Compile a where condition with a sub-select.
     *
     * @param Builder $query
     * @param array $where
     * @return string
     */
    protected function whereSub(Builder $query, array $where)
    {
        $select = $this->compileSelect($where['query']);

        return $this->wrap($where['column']) . ' ' . $where['operator'] . " ($select)";
    }

    /**
     * Compile a where exists clause.
     *
     * @param Builder $query
     * @param array $where
     * @return string
     */
    protected function whereExists(Builder $query, array $where)
    {
        return 'exists (' . $this->compileSelect($where['query']) . ')';
    }

    /**
     * Compile a where exists clause.
     *
     * @param Builder $query
     * @param array $where
     * @return string
     */
    protected function whereNotExists(Builder $query, array $where)
    {
        return 'not exists (' . $this->compileSelect($where['query']) . ')';
    }

    /**
     * Compile a where clause comparing two columns..
     *
     * @param  Builder  $query
     * @param  array  $where
     * @return string
     */
    protected function whereColumn(Builder $query, array $where)
    {
        return $this->wrap($where['first']).' '.$where['operator'].' '.$this->wrap($where['second']);
    }

    /**
     * Compile the "group by" portions of the query.
     *
     * @param Builder $query
     * @param array $groups
     * @return string
     */
    protected function compileGroups(Builder $query, array $groups)
    {
        return 'group by ' . $this->columnIze($groups);
    }

    /**
     * Compile the "having" portions of the query.
     *
     * @param Builder $query
     * @param array $having
     * @return string
     */
    protected function compileHaving(Builder $query, array $having)
    {
        $sql = implode(' ', array_map([$this, 'compileHaving'], $having));

        return 'having ' . $this->removeLeadingBoolean($sql);
    }

    /**
     * Compile the "order by" portions of the query.
     *
     * @param Builder $query
     * @param array $orders
     * @return string
     */
    protected function compileOrders(Builder $query, array $orders)
    {
        if (!empty($orders)) {
            return 'order by ' . implode(', ', $this->compileOrdersToArray($query, $orders));
        }

        return '';
    }

    /**
     * Compile the query orders to an array.
     *
     * @param Builder $query
     * @param array $orders
     * @return array
     */
    protected function compileOrdersToArray(Builder $query, array $orders)
    {
        return array_map(function ($order) {
            return !isset($order['sql'])
                ? $this->wrap($order['column']) . ' ' . $order['direction']
                : $order['sql'];
        }, $orders);
    }

    /**
     * Compile the "limit" portions of the query.
     *
     * @param Builder $query
     * @param int $limit
     * @return string
     */
    protected function compileLimit(Builder $query, int $limit)
    {
        return 'limit ' . (int)$limit;
    }

    /**
     * Compile the "offset" portions of the query.
     *
     * @param Builder $query
     * @param int $offset
     * @return string
     */
    protected function compileOffset(Builder $query, int $offset)
    {
        return 'offset ' . (int)$offset;
    }

    /**
     * Compile the "union" queries attached to the main query.
     *
     * @param Builder $query
     * @return string
     */
    protected function compileUnions(Builder $query)
    {
        $sql = '';

        foreach ($query->unions as $union) {
            $sql .= $this->compileUnion($union);
        }

        if (!empty($query->unionOrders)) {
            $sql .= ' ' . $this->compileOrders($query, $query->unionOrders);
        }

        if (isset($query->unionLimit)) {
            $sql .= ' ' . $this->compileLimit($query, $query->unionLimit);
        }

        if (isset($query->unionOffset)) {
            $sql .= ' ' . $this->compileOffset($query, $query->unionOffset);
        }

        return ltrim($sql);
    }

    /**
     * Compile an exists statement into SQL.
     *
     * @param Builder $query
     * @return string
     */
    public function compileExists(Builder $query)
    {
        $select = $this->compileSelect($query);

        return "select exists({$select}) as {$this->wrap('exists')}";
    }

    /**
     * Compile an insert statement into SQL.
     *
     * @param Builder $query
     * @param array $values
     * @return string
     */
    public function compileInsert(Builder $query, array $values)
    {
        $table = $this->wrapTable($query->from);

        if (!is_array(reset($values))) {
            $values = [$values];
        }

        $columns = $this->columnIze(array_keys(reset($values)));

        $parameters = collect($values)->map(function ($record) {
            return '(' . $this->parameterize($record) . ')';
        })->implode(', ');

        return "insert into $table ($columns) values $parameters";
    }

    /**
     * Concatenate an array of segments, removing empties.
     *
     * @param array $segments
     * @return string
     */
    protected function concatenate(array $segments)
    {
        return implode(' ', array_filter($segments, function ($value) {
            return (string)$value !== '';
        }));
    }

    /**
     * Remove the leading boolean from a statement.
     *
     * @param string $value
     * @return string
     */
    protected function removeLeadingBoolean(string $value)
    {
        return preg_replace('/and |or /i', '', $value, 1);
    }

    /**
     * Convert an array of column names into a delimited string.
     *
     * @param array $columns
     * @return string
     */
    public function columnIze(array $columns)
    {
        return implode(', ', array_map([$this, 'wrap'], $columns));
    }

    /**
     * Wrap a value in keyword identifiers.
     *
     * @param Expression|string $value
     * @param bool $prefixAlias
     * @return string
     */
    public function wrap($value, $prefixAlias = false)
    {
        if ($this->isExpression($value)) {
            return $this->getValue($value);
        }

        if (strpos(strtolower($value), ' as ') !== false) {
            return $this->wrapAliasedValue($value, $prefixAlias);
        }

        return $this->wrapSegments(explode('.', $value));
    }

    /**
     * Determine if the given value is a raw expression.
     *
     * @param mixed $value
     * @return bool
     */
    public function isExpression($value)
    {
        return $value instanceof Expression;
    }

    /**
     * Wrap the given value segments.
     *
     * @param array $segments
     * @return string
     */
    protected function wrapSegments(array $segments)
    {
        return collect($segments)->map(function ($segment, $key) use ($segments) {
            return $key == 0 && count($segments) > 1
                ? $this->wrapTable($segment)
                : $this->wrapValue($segment);
        })->implode('.');
    }

    /**
     * Wrap a table in keyword identifiers.
     *
     * @param Expression|string $table
     * @return string
     */
    public function wrapTable($table)
    {
        if (!$this->isExpression($table)) {
            return $this->wrap($this->tablePrefix . $table, true);
        }

        return $this->getValue($table);
    }

    /**
     * Get the appropriate query parameter place-holder for a value.
     *
     * @param mixed $value
     * @return string
     */
    public function parameter($value)
    {
        return $this->isExpression($value) ? $this->getValue($value) : '?';
    }

    /**
     * Get the value of a raw expression.
     *
     * @param Expression $expression
     * @return string
     */
    public function getValue(Expression $expression)
    {
        return $expression->getValue();
    }

    /**
     * Wrap a value that has an alias.
     *
     * @param string $value
     * @param bool $prefixAlias
     * @return string
     */
    protected function wrapAliasedValue(string $value, $prefixAlias = false)
    {
        $segments = preg_split('/\s+as\s+/i', $value);

        // If we are wrapping a table we need to prefix the alias with the table prefix
        // as well in order to generate proper syntax. If this is a column of course
        // no prefix is necessary. The condition will be true when from wrapTable.
        if ($prefixAlias) {
            $segments[1] = $this->tablePrefix . $segments[1];
        }

        return $this->wrap(
                $segments[0]) . ' as ' . $this->wrapValue($segments[1]
            );
    }

    /**
     * Create query parameter place-holders for an array.
     *
     * @param array $values
     * @return string
     */
    public function parameterize(array $values)
    {
        return implode(', ', array_map([$this, 'parameter'], $values));
    }

}
