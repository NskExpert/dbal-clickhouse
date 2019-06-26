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

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Exception\InvalidArgumentException;
use Exception;
use FOD\DBALClickHouse\ClickHouseException;
use FOD\DBALClickHouse\Connection;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Testing work with public methods of FOD\DBALClickHouse\Connection class
 *
 * @author Nikolay Mitrofanov <mitrofanovnk@gmail.com>
 */
class ConnectionTest extends TestCase
{
    /** @var  Connection */
    protected $connection;

    /**
     * @throws DBALException
     */
    public function setUp()
    {
        $this->connection = CreateConnectionTest::createConnection();
    }

    /**
     * @throws ClickHouseException
     * @throws DBALException
     */
    public function testExecuteUpdateDelete()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->executeUpdate('DELETE from test WHERE 1');
    }

    /**
     * @throws ClickHouseException
     * @throws DBALException
     */
    public function testExecuteUpdateUpdate()
    {
        $this->expectException(ClickHouseException::class);
        $this->connection->executeUpdate('UPDATE test SET name = :name WHERE id = :id', [':name' => 'test', ':id' => 1]);
    }

    /**
     * @throws ClickHouseException
     * @throws DBALException
     * @throws InvalidArgumentException
     */
    public function testDelete()
    {
        $this->expectException(DBALException::class);
        $this->connection->delete('test', ['id' => 1]);
    }

    /**
     * @throws DBALException
     */
    public function testUpdate()
    {
        $this->expectException(DBALException::class);
        $this->connection->update('test', ['name' => 'test'], ['id' => 1]);
    }

    /**
     * @throws DBALException
     */
    public function testSetTransactionIsolation()
    {
        $this->expectException(DBALException::class);
        $this->connection->setTransactionIsolation(1);
    }

    /**
     * @throws Exception
     */
    public function testGetTransactionIsolation()
    {
        $this->expectException(DBALException::class);
        $this->connection->getTransactionIsolation();
    }

    /**
     * @throws Exception
     */
    public function testGetTransactionNestingLevel()
    {
        $this->expectException(DBALException::class);
        $this->connection->getTransactionNestingLevel();
    }

    /**
     * @throws DBALException
     */
    public function testTransactional()
    {
        $this->expectException(DBALException::class);
        $this->connection->transactional(function () {
        });
    }

    /**
     * @throws DBALException
     */
    public function testSetNestTransactionsWithSavepoints()
    {
        $this->expectException(DBALException::class);
        $this->connection->setNestTransactionsWithSavepoints(true);
    }

    /**
     * @throws Exception
     */
    public function testGetNestTransactionsWithSavepoints()
    {
        $this->expectException(DBALException::class);
        $this->connection->getNestTransactionsWithSavepoints();
    }

    /**
     * @throws Exception
     */
    public function testBeginTransaction()
    {
        $this->expectException(DBALException::class);
        $this->connection->beginTransaction();
    }

    /**
     * @throws Exception
     */
    public function testCommit()
    {
        $this->expectException(DBALException::class);
        $this->connection->commit();
    }

    /**
     * @throws Exception
     */
    public function testRollBack()
    {
        $this->expectException(DBALException::class);
        $this->connection->rollBack();
    }

    /**
     * @throws DBALException
     */
    public function testCreateSavepoint()
    {
        $this->expectException(DBALException::class);
        $this->connection->createSavepoint('1');
    }

    /**
     * @throws DBALException
     */
    public function testReleaseSavepoint()
    {
        $this->expectException(DBALException::class);
        $this->connection->releaseSavepoint('1');
    }

    /**
     * @throws DBALException
     */
    public function testRollbackSavepoint()
    {
        $this->expectException(DBALException::class);
        $this->connection->rollbackSavepoint('1');
    }

    /**
     * @throws Exception
     */
    public function testSetRollbackOnly()
    {
        $this->expectException(DBALException::class);
        $this->connection->setRollbackOnly();
    }

    /**
     * @throws Exception
     */
    public function testIsRollbackOnly()
    {
        $this->expectException(DBALException::class);
        $this->connection->isRollbackOnly();
    }
}
