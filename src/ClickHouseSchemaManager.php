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

use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\View;

/**
 * Schema manager for the ClickHouse DBMS.
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
class ClickHouseSchemaManager extends AbstractSchemaManager
{
    /**
     * {@inheritdoc}
     */
    protected function _getPortableTableDefinition($table)
    {
        if ($this->_conn->getDatabase() !== $table['database']) {
            return false;
        }

        return $table['name'];
    }

    /**
     * {@inheritdoc}
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function _getPortableViewDefinition($view)
    {
        $statement = $this->_conn->fetchColumn('SHOW CREATE TABLE ' . $view['name'] . ' FORMAT JSON');
        return new View($view['name'], $statement);
    }

    /**
     * {@inheritdoc}
     */
    public function listTableIndexes($table)
    {
        return [];
    }

    /**
     * {@inheritdoc}
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function _getPortableTableColumnDefinition($tableColumn)
    {
        $tableColumn = array_change_key_case($tableColumn, CASE_LOWER);

        $dbType = $tableColumn['type'];
        $length = null;
        $fixed = false;
        if (substr(strtolower($tableColumn['type']), 0, 11) == 'fixedstring') {
            // get length from FixedString definition
            $length = preg_replace('~.*\(([0-9]*)\).*~', '$1', $tableColumn['type']);
            $dbType = 'fixedstring';
            $fixed = true;
        }

        $unsigned = false;
        if (substr(strtolower($tableColumn['type']), 0, 4) === 'uint') {
            $unsigned = true;
        }

        if (!isset($tableColumn['name'])) {
            $tableColumn['name'] = '';
        }

        $default = null;
        //TODO process not only DEFAULT type, but ALIAS and MATERIALIZED too
        if ($tableColumn['default_expression'] && 'default' === strtolower($tableColumn['default_type'])) {
            $default = $tableColumn['default_expression'];
        }

        $options = array(
            'length' => $length,
            'notnull' => true,
            'default' => $default,
            'primary' => false,
            'fixed' => $fixed,
            'unsigned' => $unsigned,
            'autoincrement' => false,
            'comment' => null,
        );

        return new Column(
            $tableColumn['name'],
            Type::getType($this->_platform->getDoctrineTypeMapping($dbType)),
            $options
        );
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableDatabaseDefinition($database)
    {
        return $database['name'];
    }

}
