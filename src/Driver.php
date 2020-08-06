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

use Doctrine\DBAL\DBALException;

/**
 * ClickHouse Driver
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
class Driver implements \Doctrine\DBAL\Driver
{
    /**
     * {@inheritDoc}
     * @throws ClickHouseException
     */
    public function connect(array $params, $user = null, $password = null, array $driverOptions = [])
    {
        if (is_null($user)) {
            if (!isset($params['user'])) {
                throw new ClickHouseException('Connection parameter `user` is required');
            }

            $user = $params['user'];
        }

        if (is_null($password)) {
            if (!isset($params['password'])) {
                throw new ClickHouseException('Connection parameter `password` is required');
            }

            $password = $params['password'];
        }

        if (!isset($params['host'])) {
            throw new ClickHouseException('Connection parameter `host` is required');
        }

        if (!isset($params['port'])) {
            throw new ClickHouseException('Connection parameter `port` is required');
        }

        if (!isset($params['dbname'])) {
            throw new ClickHouseException('Connection parameter `dbname` is required');
        }

        return new ClickHouseConnection(
            $user,
            $password,
            $params['host'],
            $params['port'],
            $params['dbname'],
            $driverOptions,
            $this->getDatabasePlatform());
    }

    /**
     * {@inheritDoc}
     */
    public function getDatabasePlatform()
    {
        return new ClickHousePlatform;
    }

    /**
     * {@inheritDoc}
     */
    public function getSchemaManager(\Doctrine\DBAL\Connection $conn)
    {
        return new ClickHouseSchemaManager($conn);
    }

    /**
     * {@inheritDoc}
     */
    public function getName()
    {
        return 'clickhouse';
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getDatabase(\Doctrine\DBAL\Connection $conn)
    {
        $params = $conn->getParams();
        if (isset($params['dbname'])) {
            return $params['dbname'];
        }

        return $conn->fetchColumn('SELECT currentDatabase() as dbname');
    }
}
