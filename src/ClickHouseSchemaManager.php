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
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Index;
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
     * @throws DBALException
     */
    protected function _getPortableViewDefinition($view)
    {
        $statement = $this->_conn->fetchColumn('SHOW CREATE TABLE ' . $view['name'] . ' FORMAT JSON');
        return new View($view['name'], $statement);
    }

    /**
     * {@inheritdoc}
     */
    public function listTableIndexes($table): array
    {
        return [new Index('primary', ['version'], false, true, [], [])];
    }

    /**
     * Lists the columns for a given table.
     *
     * In contrast to other libraries and to the old version of Doctrine,
     * this column definition does try to contain the 'primary' field for
     * the reason that it is not portable across different RDBMS. Use
     * {@see listTableIndexes($tableName)} to retrieve the primary key
     * of a table. We're a RDBMS specifies more details these are held
     * in the platformDetails array.
     *
     * @param string $table The name of the table.
     * @param string|null $database
     *
     * @return Column[]
     * @throws DBALException
     * @noinspection PhpMissingParamTypeInspection
     */
    public function listTableColumns($table, $database = null): array
    {
        if (!$database) {
            $database = $this->_conn->getDatabase();
        }

        $sql = $this->_platform->getListTableColumnsSQL($table, $database);

        $tableColumns = $this->_conn->fetchAll($sql);

        return array_filter(
            $this->_getPortableTableColumnList($table, $database, $tableColumns),
            function (Column $column) {
                return $column->getName() !== 'EventDate';
            }
        );
    }

    /**
     * {@inheritdoc}
     * @throws DBALException
     */
    protected function _getPortableTableColumnDefinition($tableColumn): Column
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
            'notnull' => $tableColumn['name'] === 'version',
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
