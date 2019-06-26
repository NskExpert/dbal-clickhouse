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

namespace FOD\DBALClickHouse\Tests;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\DriverManager;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Tests of create connection with ClickHouse
 *
 * @author Nikolay Mitrofanov <mitrofanovnk@gmail.com>
 */
class CreateConnectionTest extends TestCase
{
    /**
     * @throws DBALException
     */
    public function testCreateConnectionWithRightParams()
    {
        $this->assertInstanceOf(Connection::class, self::createConnection());
    }

    /**
     * @throws DBALException
     */
    public function testCreateConnectionWithBadParams()
    {
        $this->expectException(DBALException::class);
        $this->assertInstanceOf(Connection::class, self::createConnection([]));
    }

    /**
     * @param null|array $params
     * @return Connection
     * @throws DBALException
     */
    public static function createConnection($params = null)
    {
        if (null === $params) {
            /** @noinspection PhpUndefinedConstantInspection */
            $params = [
                'host' => phpunit_ch_host,
                'port' => phpunit_ch_port,
                'user' => phpunit_ch_user,
                'password' => phpunit_ch_password,
                'dbname' => phpunit_ch_dbname,
                'driverClass' => phpunit_ch_driver_class,
                'wrapperClass' => phpunit_ch_wrapper_class,
            ];
        }
        return DriverManager::getConnection($params, new Configuration());
    }
}
