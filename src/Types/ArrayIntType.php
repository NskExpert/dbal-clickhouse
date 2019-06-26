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

namespace FOD\DBALClickHouse\Types;

use Doctrine\DBAL\Platforms\AbstractPlatform;

/**
 * Array(Int*) Types basic class
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
abstract class ArrayIntType extends ArrayNumType
{
    const UNSIGNED = false;

    /**
     * {@inheritDoc}
     */
    public function getSQLDeclaration(array $fieldDeclaration, AbstractPlatform $platform)
    {
        return 'Array(' . (static::UNSIGNED ? 'U' : '') . 'Int' . $this->getBitness() . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getName()
    {
        return 'array(' . (static::UNSIGNED ? 'u' : '') . 'int' . $this->getBitness() . ')';
    }
}
