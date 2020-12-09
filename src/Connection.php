<?php
/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse;

use Closure;
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Exception\InvalidArgumentException;
use Doctrine\DBAL\ParameterType;
use Exception;

/**
 * ClickHouse Connection
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
class Connection extends \Doctrine\DBAL\Connection
{
    /**
     * {@inheritDoc}
     * @throws ClickHouseException
     */
    public function executeUpdate($query, array $params = array(), array $types = array())
    {
        // ClickHouse has no UPDATE statements
        $command = strtoupper(substr(trim($query), 0, 6));
        if ('UPDATE' == $command) {
            throw new ClickHouseException('UPDATE пока не реализовал');
        } elseif ('DELETE' == $command) {
            $query = substr(trim($query), 12);
            $tableNameEndPosition = stripos($query,' ');
            if ($tableNameEndPosition === false){
                $query = $query.' DELETE';
            }else {
                $query =
                    substr($query, 0, $tableNameEndPosition)
                    . ' DELETE '
                    . substr($query, $tableNameEndPosition + 1);
            }
            $query = 'ALTER TABLE ' . $query;
        }

        return parent::executeUpdate($query, $params, $types);
    }

    /**
     * @param $tableExpression
     * @param array $identifier
     * @param array $types
     * @return int
     * @throws ClickHouseException
     * @throws DBALException
     * @throws InvalidArgumentException
     */
    public function delete(
        /** @noinspection PhpUnusedParameterInspection */
        $tableExpression,
        array $identifier,
        array $types = array()
    ) {
        if (empty($identifier)) {
            throw InvalidArgumentException::fromEmptyCriteria();
        }

        [$columns, $values, $conditions] = $this->gatherConditions($identifier);

        return $this->executeUpdate(
            'ALTER TABLE ' . $tableExpression . ' DELETE WHERE ' . implode(' AND ', $conditions),
            $values,
            is_string(key($types)) ? $this->extractTypeValues($columns, $types) : $types
        );
    }

    /**
     * Gathers conditions for an update or delete call.
     *
     * @param mixed[] $identifiers Input array of columns to values
     *
     * @return string[][] a triplet with:
     *                    - the first key being the columns
     *                    - the second key being the values
     *                    - the third key being the conditions
     * @throws DBALException
     */
    private function gatherConditions(array $identifiers)
    {
        $columns    = [];
        $values     = [];
        $conditions = [];

        foreach ($identifiers as $columnName => $value) {
            if ($value === null) {
                $conditions[] = $this->getDatabasePlatform()->getIsNullExpression($columnName);
                continue;
            }

            $columns[]    = $columnName;
            $values[]     = $value;
            $conditions[] = $columnName . ' = ?';
        }

        return [$columns, $values, $conditions];
    }

    /**
     * Extract ordered type list from an ordered column list and type map.
     *
     * @param string[]       $columnList
     * @param int[]|string[] $types
     *
     * @return int[]|string[]
     */
    private function extractTypeValues(array $columnList, array $types)
    {
        $typeValues = [];

        foreach ($columnList as $columnIndex => $columnName) {
            $typeValues[] = $types[$columnName] ?? ParameterType::STRING;
        }

        return $typeValues;
    }

    /**
     * @param $tableExpression
     * @param array $data
     * @param array $identifier
     * @param array $types
     * @throws DBALException
     */
    public function update(
        /** @noinspection PhpUnusedParameterInspection */
        $tableExpression,
        array $data,
        array $identifier,
        array $types = array()
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * all methods below throw exceptions, because ClickHouse has not transactions
     */

    /**
     * @param $level
     * @throws DBALException
     */
    public function setTransactionIsolation(
        /** @noinspection PhpUnusedParameterInspection */
        $level
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function getTransactionIsolation()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function getTransactionNestingLevel()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @param Closure $func
     * @throws DBALException
     */
    public function transactional(
        /** @noinspection PhpUnusedParameterInspection */
        Closure $func
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @param $nestTransactionsWithSavepoints
     * @throws DBALException
     */
    public function setNestTransactionsWithSavepoints(
        /** @noinspection PhpUnusedParameterInspection */
        $nestTransactionsWithSavepoints
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function getNestTransactionsWithSavepoints()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function beginTransaction()
    {
        //throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function commit()
    {
        //throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function rollBack()
    {
        //throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @param $savepoint
     * @throws DBALException
     */
    public function createSavepoint(
        /** @noinspection PhpUnusedParameterInspection */
        $savepoint
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @param $savepoint
     * @throws DBALException
     */
    public function releaseSavepoint(
        /** @noinspection PhpUnusedParameterInspection */
        $savepoint
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @param $savepoint
     * @throws DBALException
     */
    public function rollbackSavepoint(
        /** @noinspection PhpUnusedParameterInspection */
        $savepoint
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function setRollbackOnly()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function isRollbackOnly()
    {
        throw DBALException::notSupported(__METHOD__);
    }
}
