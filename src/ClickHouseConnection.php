<?php
/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license inflormation, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse;

use ClickHouseDB\Client as Smi2CHClient;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Exception;
use LogicException;
use PDO;

/**
 * ClickHouse implementation for the Connection interface.
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
class ClickHouseConnection implements \Doctrine\DBAL\Driver\Connection
{
    /**
     * @var Smi2CHClient
     */
    protected $smi2CHClient;

    /**
     * @var AbstractPlatform
     */
    protected $platform;

    /**
     * Connection constructor
     *
     * @param string $username The username to use when connecting.
     * @param string $password The password to use when connecting.
     * @param string $host
     * @param int $port
     * @param string $database
     * @param array $driverOptions
     * @param AbstractPlatform|null $platform
     */
    public function __construct(
        $username = 'default',
        $password = '',
        $host = 'localhost',
        $port = 8123,
        $database = 'default',
        $driverOptions = [],
        AbstractPlatform $platform = null
    ) {
        $this->smi2CHClient = new Smi2CHClient([
            'host' => $host,
            'port' => $port,
            'username' => $username,
            'password' => $password,
        ], array_merge(['database' => $database,],$driverOptions));

        $this->platform = $platform;
    }

    /**
     * {@inheritDoc}
     * @throws Exception
     */
    public function prepare($prepareString)
    {
        if (!$this->smi2CHClient) {
            throw new Exception('ClickHouse\Client was not initialized');
        }

        return new ClickHouseStatement($this->smi2CHClient, $prepareString, $this->platform);
    }

    /**
     * {@inheritDoc}
     * @throws Exception
     */
    public function query()
    {
        $args = func_get_args();
        $stmt = $this->prepare($args[0]);
        $stmt->execute();

        return $stmt;
    }

    /**
     * {@inheritDoc}
     */
    public function quote($input, $type = PDO::PARAM_STR)
    {
        if (PDO::PARAM_INT == $type) {
            return $input;
        }

        return $this->platform->quoteStringLiteral($input);
    }

    /**
     * {@inheritDoc}
     * @throws Exception
     */
    public function exec($statement)
    {
        $stmt = $this->prepare($statement);
        $stmt->execute();

        return $stmt->rowCount();
    }

    /**
     * {@inheritDoc}
     */
    public function lastInsertId(
        /** @noinspection PhpUnusedParameterInspection */
        $name = null
    ) {
        throw new LogicException('Unable to get last insert id in ClickHouse');
    }

    /**
     * {@inheritDoc}
     */
    public function beginTransaction()
    {
        throw new LogicException('Transactions are not allowed in ClickHouse');
    }

    /**
     * {@inheritDoc}
     */
    public function commit()
    {
        throw new LogicException('Transactions are not allowed in ClickHouse');
    }

    /**
     * {@inheritDoc}
     */
    public function rollBack()
    {
        throw new LogicException('Transactions are not allowed in ClickHouse');
    }

    /**
     * {@inheritDoc}
     */
    public function errorCode()
    {
        throw new LogicException('You need to implement ClickHouseConnection::errorCode()');
    }

    /**
     * {@inheritDoc}
     */
    public function errorInfo()
    {
        throw new LogicException('You need to implement ClickHouseConnection::errorInfo()');
    }
}